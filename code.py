import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
from io import StringIO

class AthenaQueryProcessor:
    def __init__(self, database_name, s3_output_location, region_name='us-east-1'):
        """
        Initialize Athena client
        
        Args:
            database_name (str): Athena database name
            s3_output_location (str): S3 bucket location for query results
            region_name (str): AWS region
        """
        self.athena_client = boto3.client('athena', region_name=region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.database = database_name
        self.s3_output = s3_output_location
        
    def execute_query(self, query, max_wait_time=300):
        """
        Execute Athena query and return results as DataFrame
        
        Args:
            query (str): SQL query to execute
            max_wait_time (int): Maximum time to wait for query completion
            
        Returns:
            pandas.DataFrame: Query results
        """
        print(f"Executing query...")
        
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': self.s3_output}
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Query execution ID: {query_execution_id}")
        
        # Wait for query to complete
        wait_time = 0
        while wait_time < max_wait_time:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
            time.sleep(5)
            wait_time += 5
            print(f"Query status: {status}, waiting...")
        
        if status != 'SUCCEEDED':
            raise Exception(f"Query failed with status: {status}")
        
        print("Query completed successfully!")
        
        # Get results
        results = self.athena_client.get_query_results(
            QueryExecutionId=query_execution_id
        )
        
        # Convert to DataFrame
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        
        for row in results['ResultSet']['Rows'][1:]:  # Skip header row
            rows.append([field.get('VarCharValue', '') for field in row['Data']])
        
        df = pd.DataFrame(rows, columns=columns)
        return df
    
    def save_to_csv(self, df, filename, local_path='./output/'):
        """
        Save DataFrame to CSV file
        
        Args:
            df (pandas.DataFrame): DataFrame to save
            filename (str): Output filename
            local_path (str): Local directory path
        """
        os.makedirs(local_path, exist_ok=True)
        filepath = os.path.join(local_path, filename)
        df.to_csv(filepath, index=False)
        print(f"Data saved to: {filepath}")
        return filepath

def main():
    # Configuration - UPDATE THESE VALUES
    DATABASE_NAME = 'your_athena_database'  # Replace with your Athena database name
    S3_OUTPUT_LOCATION = 's3://your-bucket/athena-results/'  # Replace with your S3 bucket
    TABLE_NAME = 'your_table_name'  # Replace with your actual table name
    
    # Initialize processor
    processor = AthenaQueryProcessor(DATABASE_NAME, S3_OUTPUT_LOCATION)
    
    # Step 1: Create initial dataset (equivalent to temp.purch_202507)
    print("=== STEP 1: Creating initial dataset ===")
    
    initial_query = f"""
    SELECT 
        eff_dt,
        extrnl_acct_id,
        acct_open_dt,
        chrgoff_dt,
        tran_amt_net,
        tran_post_dt,
        posting_dt,
        posting_mo,
        sfx_nbr
    FROM {TABLE_NAME}
    WHERE 
        DATE(eff_dt) >= DATE '2024-01-01'
        AND DATE(eff_dt) <= DATE '2025-07-31'
        AND dbt_cr_flag = 'D'
        AND tran_amt_net <> 0
        AND tran_post_dt IS NOT NULL
        AND posting_dt IS NOT NULL
        AND acct_open_dt IS NOT NULL
        AND chrgoff_dt IS NOT NULL
        AND clmt_arrdcr_cd = 'CDC'
        AND sfx_nbr = 'suffix_value'  -- Replace with actual suffix value
    """
    
    df_initial = processor.execute_query(initial_query)
    initial_file = processor.save_to_csv(df_initial, 'purch_2024_2025_initial.csv')
    print(f"Initial dataset created with {len(df_initial)} records")
    
    # Step 2: Create aggregated dataset (equivalent to PROC SUMMARY)
    print("\n=== STEP 2: Creating aggregated dataset ===")
    
    aggregated_query = f"""
    WITH base_data AS (
        SELECT 
            extrnl_acct_id,
            acct_open_dt,
            chrgoff_dt,
            CASE 
                WHEN sfx_nbr = '0' THEN 'a2'
                ELSE 'other'
            END as account_type,
            CASE 
                WHEN extrnl_acct_id = acct_open_dt THEN 1
                ELSE 0
            END as matching_account_flag,
            CASE 
                WHEN chrgoff_dt IS NOT NULL AND tran_post_dt = chrgoff_dt THEN 1
                ELSE 0
            END as chrgoff_match_flag,
            CASE 
                WHEN tran_post_dt NOT IN ('375', '376') THEN 'DAR'
                WHEN tran_sfc_cd <> 'SG' THEN 'other'
                WHEN tran_cd NOT IN ('375', '376') THEN 'excluded'
                ELSE 'included'
            END as transaction_category,
            tran_amt_net
        FROM {TABLE_NAME}
        WHERE 
            DATE(eff_dt) >= DATE '2024-01-01'
            AND DATE(eff_dt) <= DATE '2025-07-31'
            AND dbt_cr_flag = 'D'
            AND tran_amt_net <> 0
            AND tran_post_dt IS NOT NULL
            AND posting_dt IS NOT NULL
            AND acct_open_dt IS NOT NULL
            AND chrgoff_dt IS NOT NULL
            AND clmt_arrdcr_cd = 'CDC'
    )
    SELECT 
        account_type,
        transaction_category,
        COUNT(*) as record_count,
        SUM(tran_amt_net) as total_amount,
        AVG(tran_amt_net) as avg_amount,
        MIN(tran_amt_net) as min_amount,
        MAX(tran_amt_net) as max_amount,
        COUNT(DISTINCT extrnl_acct_id) as unique_accounts,
        SUM(matching_account_flag) as matching_accounts,
        SUM(chrgoff_match_flag) as chrgoff_matches
    FROM base_data
    GROUP BY account_type, transaction_category
    ORDER BY account_type, transaction_category
    """
    
    df_aggregated = processor.execute_query(aggregated_query)
    aggregated_file = processor.save_to_csv(df_aggregated, 'purch_2024_2025_summary.csv')
    print(f"Aggregated dataset created with {len(df_aggregated)} summary records")
    
    # Step 3: Create detailed analysis dataset
    print("\n=== STEP 3: Creating detailed analysis dataset ===")
    
    detailed_query = f"""
    SELECT 
        DATE_FORMAT(DATE(eff_dt), '%Y-%m') as year_month,
        extrnl_acct_id,
        acct_open_dt,
        chrgoff_dt,
        tran_amt_net,
        tran_post_dt,
        posting_dt,
        posting_mo,
        sfx_nbr,
        CASE 
            WHEN sfx_nbr = '0' THEN 'a2'
            ELSE 'other'
        END as account_type,
        CASE 
            WHEN DATE(tran_post_dt) >= DATE '2024-01-01' 
            AND DATE(tran_post_dt) <= DATE '2025-07-31' THEN 1
            ELSE 0
        END as in_date_range,
        DATEDIFF('day', DATE(acct_open_dt), DATE(tran_post_dt)) as days_since_open,
        CASE 
            WHEN chrgoff_dt IS NOT NULL THEN 
                DATEDIFF('day', DATE(tran_post_dt), DATE(chrgoff_dt))
            ELSE NULL
        END as days_to_chrgoff
    FROM {TABLE_NAME}
    WHERE 
        DATE(eff_dt) >= DATE '2024-01-01'
        AND DATE(eff_dt) <= DATE '2025-07-31'
        AND dbt_cr_flag = 'D'
        AND tran_amt_net <> 0
        AND tran_post_dt IS NOT NULL
        AND posting_dt IS NOT NULL
        AND acct_open_dt IS NOT NULL
        AND chrgoff_dt IS NOT NULL
        AND clmt_arrdcr_cd = 'CDC'
    ORDER BY year_month, extrnl_acct_id, tran_post_dt
    """
    
    df_detailed = processor.execute_query(detailed_query)
    detailed_file = processor.save_to_csv(df_detailed, 'purch_2024_2025_detailed.csv')
    print(f"Detailed analysis dataset created with {len(df_detailed)} records")
    
    # Step 4: Create Excel output with multiple sheets
    print("\n=== STEP 4: Creating Excel output ===")
    
    excel_filename = './output/purchase_analysis_2024_2025.xlsx'
    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        # Summary sheet
        df_aggregated.to_excel(writer, sheet_name='Summary', index=False)
        
        # Monthly breakdown
        monthly_summary = df_detailed.groupby(['year_month', 'account_type']).agg({
            'tran_amt_net': ['count', 'sum', 'mean'],
            'extrnl_acct_id': 'nunique'
        }).round(2)
        monthly_summary.columns = ['Transaction_Count', 'Total_Amount', 'Avg_Amount', 'Unique_Accounts']
        monthly_summary.reset_index().to_excel(writer, sheet_name='Monthly_Summary', index=False)
        
        # Account type breakdown
        account_summary = df_detailed.groupby('account_type').agg({
            'tran_amt_net': ['count', 'sum', 'mean', 'std'],
            'days_since_open': 'mean',
            'days_to_chrgoff': 'mean'
        }).round(2)
        account_summary.columns = ['Txn_Count', 'Total_Amount', 'Avg_Amount', 'Std_Amount', 'Avg_Days_Open', 'Avg_Days_Chrgoff']
        account_summary.reset_index().to_excel(writer, sheet_name='Account_Type_Summary', index=False)
        
        # Raw data (first 10,000 rows for Excel compatibility)
        df_detailed.head(10000).to_excel(writer, sheet_name='Raw_Data', index=False)
    
    print(f"Excel file created: {excel_filename}")
    
    # Step 5: Create summary report
    print("\n=== STEP 5: Summary Report ===")
    print(f"Date range: January 2024 - July 2025")
    print(f"Total records processed: {len(df_initial):,}")
    print(f"Unique accounts: {df_detailed['extrnl_acct_id'].nunique():,}")
    print(f"Total transaction amount: ${df_detailed['tran_amt_net'].astype(float).sum():,.2f}")
    print(f"Average transaction amount: ${df_detailed['tran_amt_net'].astype(float).mean():.2f}")
    print(f"\nFiles created:")
    print(f"- {initial_file}")
    print(f"- {aggregated_file}")
    print(f"- {detailed_file}")
    print(f"- {excel_filename}")
    
    return {
        'initial_df': df_initial,
        'aggregated_df': df_aggregated,
        'detailed_df': df_detailed,
        'files_created': [initial_file, aggregated_file, detailed_file, excel_filename]
    }

if __name__ == "__main__":
    try:
        results = main()
        print("\n✅ Process completed successfully!")
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        print("Please check your AWS credentials, database name, and table name configuration.")
