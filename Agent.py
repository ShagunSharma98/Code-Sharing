# core/intent_classifier.py
import re
import spacy
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import joblib
from typing import List, Dict, Tuple
from dataclasses import dataclass

@dataclass
class IntentResult:
    intent_type: str
    confidence: float
    entities: List[str]
    filters: List[Dict]
    aggregations: List[str]
    dimensions: List[str]

class IntentClassifier:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        self.intent_model = None
        self.vectorizer = None
        self.entity_patterns = self._build_entity_patterns()
        self.intent_patterns = self._build_intent_patterns()
        self.trained = False
        
    def _build_intent_patterns(self) -> Dict[str, Dict]:
        """Define patterns for different analysis intents"""
        return {
            'aggregation': {
                'keywords': [
                    'total', 'sum', 'count', 'average', 'mean', 'max', 'maximum',
                    'min', 'minimum', 'aggregate', 'group by', 'breakdown'
                ],
                'patterns': [
                    r'total \w+ by \w+',
                    r'sum of \w+',
                    r'count of \w+',
                    r'average \w+ per \w+',
                    r'breakdown of \w+',
                    r'\w+ by \w+ and \w+'
                ]
            },
            'comparison': {
                'keywords': [
                    'compare', 'vs', 'versus', 'difference', 'between',
                    'against', 'relative to', 'compared to'
                ],
                'patterns': [
                    r'compare \w+ and \w+',
                    r'\w+ vs \w+',
                    r'difference between \w+ and \w+',
                    r'\w+ versus \w+'
                ]
            },
            'filtering': {
                'keywords': [
                    'where', 'filter', 'only', 'exclude', 'remove',
                    'specific', 'particular', 'certain'
                ],
                'patterns': [
                    r'where \w+ (equals|is|=) \w+',
                    r'only \w+ that \w+',
                    r'filter by \w+',
                    r'exclude \w+ where \w+'
                ]
            },
            'time_series': {
                'keywords': [
                    'trend', 'over time', 'time series', 'monthly', 'daily',
                    'yearly', 'quarterly', 'weekly', 'growth', 'change',
                    'evolution', 'historical'
                ],
                'patterns': [
                    r'\w+ over time',
                    r'monthly \w+',
                    r'trend in \w+',
                    r'\w+ growth',
                    r'historical \w+'
                ]
            },
            'ranking': {
                'keywords': [
                    'top', 'bottom', 'best', 'worst', 'highest', 'lowest',
                    'rank', 'order', 'sort', 'most', 'least'
                ],
                'patterns': [
                    r'top \d+ \w+',
                    r'best \w+',
                    r'highest \w+',
                    r'rank \w+ by \w+'
                ]
            },
            'correlation': {
                'keywords': [
                    'correlation', 'relationship', 'related', 'associated',
                    'connection', 'impact', 'influence', 'affect'
                ],
                'patterns': [
                    r'correlation between \w+ and \w+',
                    r'relationship between \w+ and \w+',
                    r'how \w+ affects \w+'
                ]
            }
        }
    
    def _build_entity_patterns(self) -> Dict[str, List[str]]:
        """Build patterns for entity extraction"""
        # **[INPUT NEEDED: Your domain-specific entities]**
        # Please provide lists of common entities in your domain
        return {
            'metrics': [
                # Examples - replace with your actual metrics
                'revenue', 'sales', 'profit', 'cost', 'price', 'quantity',
                'users', 'customers', 'orders', 'transactions', 'clicks'
            ],
            'dimensions': [
                # Examples - replace with your actual dimensions  
                'region', 'country', 'state', 'city', 'product', 'category',
                'channel', 'segment', 'department', 'brand', 'campaign'
            ],
            'time_periods': [
                'day', 'week', 'month', 'quarter', 'year', 'daily', 'weekly',
                'monthly', 'quarterly', 'yearly', 'today', 'yesterday'
            ],
            'operators': [
                'greater than', 'less than', 'equals', 'not equals', 'contains',
                'starts with', 'ends with', 'between', 'in', 'not in'
            ]
        }
    
    def classify_intent(self, query: str) -> IntentResult:
        """Main method to classify user intent"""
        query_lower = query.lower()
        
        # Method 1: Pattern-based classification (fast, deterministic)
        pattern_result = self._classify_by_patterns(query_lower)
        
        # Method 2: ML-based classification (if trained model available)
        ml_result = self._classify_by_ml(query) if self.trained else None
        
        # Combine results
        if pattern_result.confidence > 0.7:
            final_result = pattern_result
        elif ml_result and ml_result.confidence > pattern_result.confidence:
            final_result = ml_result  
        else:
            final_result = pattern_result
        
        # Extract entities
        entities = self._extract_entities(query)
        final_result.entities = entities
        
        # Extract filters, aggregations, dimensions
        filters = self._extract_filters(query)
        aggregations = self._extract_aggregations(query, final_result.intent_type)
        dimensions = self._extract_dimensions(query, entities)
        
        final_result.filters = filters
        final_result.aggregations = aggregations
        final_result.dimensions = dimensions
        
        return final_result
    
    def _classify_by_patterns(self, query: str) -> IntentResult:
        """Pattern-based intent classification"""
        intent_scores = {}
        
        for intent, config in self.intent_patterns.items():
            score = 0.0
            
            # Keyword matching
            keyword_matches = sum(1 for kw in config['keywords'] 
                                if kw in query)
            score += (keyword_matches / len(config['keywords'])) * 0.6
            
            # Pattern matching
            pattern_matches = sum(1 for pattern in config['patterns']
                                if re.search(pattern, query, re.IGNORECASE))
            if pattern_matches > 0:
                score += 0.4
            
            intent_scores[intent] = score
        
        # Get best match
        best_intent = max(intent_scores, key=intent_scores.get)
        confidence = intent_scores[best_intent]
        
        return IntentResult(
            intent_type=best_intent,
            confidence=confidence,
            entities=[],
            filters=[],
            aggregations=[],
            dimensions=[]
        )
    
    def _classify_by_ml(self, query: str) -> IntentResult:
        """ML-based intent classification"""
        if not self.intent_model:
            return None
            
        features = self.vectorizer.transform([query])
        prediction = self.intent_model.predict(features)[0]
        probabilities = self.intent_model.predict_proba(features)[0]
        confidence = max(probabilities)
        
        return IntentResult(
            intent_type=prediction,
            confidence=confidence,
            entities=[],
            filters=[],
            aggregations=[],
            dimensions=[]
        )
    
    def _extract_entities(self, query: str) -> List[str]:
        """Extract business entities from query"""
        doc = self.nlp(query)
        entities = []
        
        # Named entity recognition
        for ent in doc.ents:
            entities.append(ent.text)
        
        # Domain-specific entity matching
        query_lower = query.lower()
        for entity_type, entity_list in self.entity_patterns.items():
            for entity in entity_list:
                if entity.lower() in query_lower:
                    entities.append(entity)
        
        return list(set(entities))  # Remove duplicates
    
    def _extract_filters(self, query: str) -> List[Dict]:
        """Extract filter conditions from query"""
        filters = []
        
        # Common filter patterns
        filter_patterns = [
            (r'where (\w+) equals (\w+)', 'equals'),
            (r'where (\w+) is (\w+)', 'equals'),
            (r'where (\w+) = (\w+)', 'equals'),
            (r'(\w+) greater than (\w+)', 'greater_than'),
            (r'(\w+) > (\w+)', 'greater_than'),
            (r'(\w+) less than (\w+)', 'less_than'),
            (r'(\w+) < (\w+)', 'less_than'),
            (r'only (\w+) where (\w+)', 'filter'),
        ]
        
        for pattern, operator in filter_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            for match in matches:
                filters.append({
                    'column': match[0],
                    'operator': operator,
                    'value': match[1] if len(match) > 1 else None
                })
        
        return filters
    
    def _extract_aggregations(self, query: str, intent_type: str) -> List[str]:
        """Extract aggregation functions needed"""
        aggregations = []
        query_lower = query.lower()
        
        agg_keywords = {
            'sum': ['total', 'sum'],
            'count': ['count', 'number of'],
            'avg': ['average', 'mean'],
            'max': ['maximum', 'max', 'highest'],
            'min': ['minimum', 'min', 'lowest']
        }
        
        for agg_func, keywords in agg_keywords.items():
            if any(kw in query_lower for kw in keywords):
                aggregations.append(agg_func)
        
        # Default aggregations based on intent
        if intent_type == 'aggregation' and not aggregations:
            aggregations = ['sum']  # Default
        elif intent_type == 'comparison':
            aggregations = ['sum', 'count']
        
        return aggregations
    
    def _extract_dimensions(self, query: str, entities: List[str]) -> List[str]:
        """Extract grouping dimensions"""
        dimensions = []
        query_lower = query.lower()
        
        # Look for "by" keywords
        by_patterns = [
            r'by (\w+)',
            r'per (\w+)',  
            r'for each (\w+)',
            r'group by (\w+)'
        ]
        
        for pattern in by_patterns:
            matches = re.findall(pattern, query_lower)
            dimensions.extend(matches)
        
        # Use dimension entities
        for entity in entities:
            if entity.lower() in self.entity_patterns['dimensions']:
                dimensions.append(entity)
        
        return list(set(dimensions))
    
    def train_model(self, training_data: List[Tuple[str, str]]):
        """Train the ML model on your specific queries"""
        # **[INPUT NEEDED: Training data]**
        # Please provide examples of your typical queries with labels
        
        if not training_data:
            print("No training data provided, using pattern-based classification only")
            return
        
        queries, labels = zip(*training_data)
        
        # Create pipeline
        self.vectorizer = TfidfVectorizer(
            ngram_range=(1, 3),
            max_features=5000,
            stop_words='english'
        )
        
        self.intent_model = Pipeline([
            ('tfidf', self.vectorizer),
            ('classifier', RandomForestClassifier(n_estimators=100, random_state=42))
        ])
        
        # Train
        X_train, X_test, y_train, y_test = train_test_split(
            queries, labels, test_size=0.2, random_state=42
        )
        
        self.intent_model.fit(X_train, y_train)
        
        # Evaluate
        accuracy = self.intent_model.score(X_test, y_test)
        print(f"Model trained with accuracy: {accuracy:.3f}")
        
        # Save model
        joblib.dump(self.intent_model, 'models/intent_model/intent_classifier.pkl')
        self.trained = True
    
    def load_model(self):
        """Load pre-trained model"""
        try:
            self.intent_model = joblib.load('models/intent_model/intent_classifier.pkl')
            self.trained = True
            print("Intent model loaded successfully")
        except:
            print("No pre-trained model found, using pattern-based classification")
