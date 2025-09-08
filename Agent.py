# core/metadata_manager.py
import boto3
import duckdb
import pandas as pd
import networkx as nx
from typing import Dict, List, Set, Tuple
import json
from dataclasses import dataclass
from collections import defaultdict
import numpy as np

@dataclass
class TableMetadata:
    database_name: str
    table_name: str
    columns: List[Dict]
    row_count: int
    size_bytes: int
    partitions: List[str]
    last_updated: str
    
@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    is_nullable: bool
    distinct_count: int
    null_percentage: float
    sample_values: List
    is_key_candidate: bool

class MetadataManager:
    def __init__(self, athena_config: Dict):
        self.athena_client = boto3.client('athena', 
                                        region_name=athena_config['region'])
        self.s3_client = boto3.client('s3')
        self.metadata_db = duckdb.connect('metadata.db')
        self.relationship_graph = nx.Graph()
        self.athena_config = athena_config
        self._setup_metadata_tables()
        
    def _setup_metadata_tables(self):
        """Initialize metadata storage tables"""
        self.metadata_db.execute("""
            CREATE TABLE IF NOT EXISTS table_metadata (
                database_name VARCHAR,
                table_name VARCHAR,
                column_name VARCHAR,
                data_type VARCHAR,
                is_nullable BOOLEAN,
                distinct_count BIGINT,
                null_percentage FLOAT,
                sample_values JSON,
                is_key_candidate BOOLEAN,
                last_profiled TIMESTAMP,
                PRIMARY KEY (database_name, table_name, column_name)
            )
        """)
        
        self.metadata_db.execute("""
            CREATE TABLE IF NOT EXISTS table_relationships (
                source_table VARCHAR,
                target_table VARCHAR,
                relationship_type VARCHAR,
                confidence_score FLOAT,
                join_columns JSON,
                PRIMARY KEY (source_table, target_table)
            )
        """)
    
    def discover_all_schemas(self) -> Dict[str, List[TableMetadata]]:
        """Main entry point for schema discovery"""
        print("Starting schema discovery...")
        
        # Step 1: Get all databases and tables
        databases = self._get_all_databases()
        all_metadata = {}
        
        for db_name in databases:
            print(f"Processing database: {db_name}")
            tables = self._get_tables_in_database(db_name)
            table_metadata_list = []
            
            for table_name in tables:
                print(f"  Processing table: {table_name}")
                metadata = self._profile_table(db_name, table_name)
                table_metadata_list.append(metadata)
                
            all_metadata[db_name] = table_metadata_list
        
        # Step 2: Detect relationships between tables
        self._detect_relationships(all_metadata)
        
        # Step 3: Save to persistent storage
        self._save_metadata(all_metadata)
        
        return all_metadata
    
    def _get_all_databases(self) -> List[str]:
        """Get all database names from Athena"""
        query = "SHOW DATABASES"
        result = self._execute_athena_query(query)
        return [row[0] for row in result]
    
    def _get_tables_in_database(self, db_name: str) -> List[str]:
        """Get all tables in a specific database"""
        query = f"SHOW TABLES IN {db_name}"
        result = self._execute_athena_query(query)
        return [row[0] for row in result]
    
    def _profile_table(self, db_name: str, table_name: str) -> TableMetadata:
        """Profile a single table to extract metadata"""
        # Get basic table info
        table_info_query = f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable
        FROM information_schema.columns 
        WHERE table_schema = '{db_name}' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        column_info = self._execute_athena_query(table_info_query)
        
        # Get table statistics
        stats_query = f"SELECT COUNT(*) as row_count FROM {db_name}.{table_name}"
        stats_result = self._execute_athena_query(stats_query)
        row_count = stats_result[0][0] if stats_result else 0
        
        # Profile each column
        columns = []
        for col_name, data_type, is_nullable in column_info:
            col_metadata = self._profile_column(db_name, table_name, 
                                              col_name, data_type, row_count)
            columns.append(col_metadata.__dict__)
        
        return TableMetadata(
            database_name=db_name,
            table_name=table_name,
            columns=columns,
            row_count=row_count,
            size_bytes=0,  # Calculate if needed
            partitions=[],  # Extract if using partitioned tables
            last_updated=""  # Get from table properties
        )
    
    def _profile_column(self, db_name: str, table_name: str, 
                       col_name: str, data_type: str, total_rows: int) -> ColumnMetadata:
        """Profile individual column statistics"""
        
        # Get distinct count and null percentage
        profile_query = f"""
        SELECT 
            COUNT(DISTINCT {col_name}) as distinct_count,
            COUNT(*) as non_null_count,
            COUNT(CASE WHEN {col_name} IS NULL THEN 1 END) as null_count
        FROM {db_name}.{table_name}
        """
        
        try:
            result = self._execute_athena_query(profile_query)
            distinct_count, non_null_count, null_count = result[0]
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        except:
            distinct_count, null_percentage = 0, 0
        
        # Get sample values
        sample_query = f"""
        SELECT DISTINCT {col_name} 
        FROM {db_name}.{table_name} 
        WHERE {col_name} IS NOT NULL 
        LIMIT 10
        """
        
        try:
            sample_result = self._execute_athena_query(sample_query)
            sample_values = [row[0] for row in sample_result]
        except:
            sample_values = []
        
        # Determine if column is a key candidate
        is_key_candidate = (distinct_count / total_rows) > 0.95 if total_rows > 0 else False
        
        return ColumnMetadata(
            name=col_name,
            data_type=data_type,
            is_nullable=is_nullable.upper() == 'YES',
            distinct_count=distinct_count,
            null_percentage=null_percentage,
            sample_values=sample_values,
            is_key_candidate=is_key_candidate
        )
    
    def _detect_relationships(self, all_metadata: Dict[str, List[TableMetadata]]):
        """Detect relationships between tables using various heuristics"""
        all_tables = []
        for db_tables in all_metadata.values():
            all_tables.extend(db_tables)
        
        relationships = []
        
        # Method 1: Column name matching (foreign key inference)
        for i, table1 in enumerate(all_tables):
            for j, table2 in enumerate(all_tables):
                if i >= j:  # Avoid duplicate pairs
                    continue
                
                # Check for potential foreign key relationships
                fk_relationships = self._find_foreign_key_candidates(table1, table2)
                relationships.extend(fk_relationships)
        
        # Method 2: Value overlap analysis (for tables with similar data)
        # This is computationally expensive, so we'll do it selectively
        
        # Store relationships
        for rel in relationships:
            self.relationship_graph.add_edge(
                f"{rel['source_db']}.{rel['source_table']}", 
                f"{rel['target_db']}.{rel['target_table']}",
                relationship_type=rel['type'],
                confidence=rel['confidence'],
                join_columns=rel['join_columns']
            )
    
    def _find_foreign_key_candidates(self, table1: TableMetadata, 
                                   table2: TableMetadata) -> List[Dict]:
        """Find potential foreign key relationships between two tables"""
        relationships = []
        
        # Common patterns for FK relationships
        fk_patterns = [
            # table2 has column that matches table1's primary key pattern
            (r'(.+)_id$', r'\1_id$'),  # user_id matches user_id
            (r'id$', r'(.+)_id$'),     # id matches something_id  
            (r'(.+)_key$', r'\1_key$'), # user_key matches user_key
        ]
        
        table1_cols = {col['name']: col for col in table1.columns}
        table2_cols = {col['name']: col for col in table2.columns}
        
        for col1_name, col1_info in table1_cols.items():
            for col2_name, col2_info in table2_cols.items():
                
                # Check if column names suggest a relationship
                confidence = 0.0
                
                # Exact name match
                if col1_name == col2_name:
                    confidence = 0.9
                
                # Pattern matching
                elif col1_name.endswith('_id') and col2_name.endswith('_id'):
                    if col1_name.replace('_id', '') in table2.table_name.lower():
                        confidence = 0.8
                
                # Data type compatibility
                if confidence > 0 and col1_info['data_type'] == col2_info['data_type']:
                    confidence += 0.1
                
                if confidence > 0.7:  # Threshold for FK candidate
                    relationships.append({
                        'source_db': table1.database_name,
                        'source_table': table1.table_name,
                        'target_db': table2.database_name,
                        'target_table': table2.table_name,
                        'type': 'foreign_key',
                        'confidence': confidence,
                        'join_columns': [(col1_name, col2_name)]
                    })
        
        return relationships
    
    def _execute_athena_query(self, query: str) -> List[Tuple]:
        """Execute query in Athena and return results"""
        # **[INPUT NEEDED: Your Athena execution configuration]**
        # Please provide:
        # - S3 bucket for query results
        # - Database name for queries
        # - Any specific Athena settings you use
        
        query_execution = self.athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': f"s3://{self.athena_config['results_bucket']}/query-results/"
            },
            WorkGroup=self.athena_config.get('workgroup', 'primary')
        )
        
        execution_id = query_execution['QueryExecutionId']
        
        # Wait for completion
        while True:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=execution_id
            )
            
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED']:
                break
            elif status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Query failed: {response['QueryExecution']['Status']}")
            
            time.sleep(1)
        
        # Get results
        results = self.athena_client.get_query_results(
            QueryExecutionId=execution_id
        )
        
        # Parse results
        rows = []
        for row in results['ResultSet']['Rows'][1:]:  # Skip header
            rows.append(tuple(col.get('VarCharValue', '') for col in row['Data']))
        
        return rows
    
    def get_table_recommendations(self, entities: List[str], 
                                intent_type: str) -> List[Dict]:
        """Recommend tables based on entities and intent"""
        
        # Method 1: Direct keyword matching
        direct_matches = []
        for entity in entities:
            query = f"""
            SELECT database_name, table_name, column_name, 
                   CASE 
                       WHEN LOWER(table_name) LIKE '%{entity.lower()}%' THEN 0.9
                       WHEN LOWER(column_name) LIKE '%{entity.lower()}%' THEN 0.7
                       ELSE 0.0 
                   END as score
            FROM table_metadata 
            WHERE LOWER(table_name) LIKE '%{entity.lower()}%' 
               OR LOWER(column_name) LIKE '%{entity.lower()}%'
            ORDER BY score DESC
            """
            
            results = self.metadata_db.execute(query).fetchall()
            direct_matches.extend(results)
        
        # Method 2: Use relationship graph for connected tables
        related_tables = set()
        for match in direct_matches:
            table_key = f"{match[0]}.{match[1]}"
            if table_key in self.relationship_graph:
                neighbors = list(self.relationship_graph.neighbors(table_key))
                related_tables.update(neighbors)
        
        # Combine and rank recommendations
        recommendations = self._rank_table_recommendations(
            direct_matches, related_tables, intent_type
        )
        
        return recommendations[:10]  # Top 10 recommendations
