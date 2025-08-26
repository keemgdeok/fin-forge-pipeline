"""Data transformation Lambda function handler for lightweight data processing."""
import json
import logging
import os
from typing import Dict, Any, Optional, List, Union
import boto3
from datetime import datetime, timezone
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DataTransformer:
    """Helper class for lightweight data transformations."""
    
    def __init__(self, region_name: Optional[str] = None):
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.s3_resource = boto3.resource('s3', region_name=region_name)
    
    def read_csv_from_s3(self, bucket: str, key: str) -> Optional[pd.DataFrame]:
        """Read CSV file from S3 into pandas DataFrame."""
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(obj['Body'])
            logger.info(f"Successfully read {len(df)} rows from s3://{bucket}/{key}")
            return df
        except ClientError as e:
            logger.error(f"Failed to read CSV from S3: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Failed to parse CSV data: {str(e)}")
            return None
    
    def read_json_from_s3(self, bucket: str, key: str) -> Optional[Union[Dict, List]]:
        """Read JSON file from S3."""
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj['Body'].read().decode('utf-8'))
            logger.info(f"Successfully read JSON from s3://{bucket}/{key}")
            return data
        except ClientError as e:
            logger.error(f"Failed to read JSON from S3: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON data: {str(e)}")
            return None
    
    def write_csv_to_s3(self, df: pd.DataFrame, bucket: str, key: str) -> bool:
        """Write pandas DataFrame to S3 as CSV."""
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            logger.info(f"Successfully wrote {len(df)} rows to s3://{bucket}/{key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to write CSV to S3: {str(e)}")
            return False
    
    def write_json_to_s3(self, data: Union[Dict, List], bucket: str, key: str) -> bool:
        """Write JSON data to S3."""
        try:
            json_str = json.dumps(data, indent=2, default=str)
            
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_str,
                ContentType='application/json'
            )
            
            logger.info(f"Successfully wrote JSON to s3://{bucket}/{key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to write JSON to S3: {str(e)}")
            return False
    
    def apply_data_quality_checks(self, df: pd.DataFrame, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data quality checks to DataFrame."""
        results = {
            'total_rows': len(df),
            'passed_checks': 0,
            'failed_checks': 0,
            'check_results': []
        }
        
        for rule in rules:
            rule_type = rule.get('type')
            column = rule.get('column')
            check_result = {
                'rule': rule,
                'passed': False,
                'details': ''
            }
            
            try:
                if rule_type == 'not_null':
                    null_count = df[column].isnull().sum()
                    check_result['passed'] = null_count == 0
                    check_result['details'] = f"Null values found: {null_count}"
                
                elif rule_type == 'unique':
                    duplicate_count = df[column].duplicated().sum()
                    check_result['passed'] = duplicate_count == 0
                    check_result['details'] = f"Duplicate values found: {duplicate_count}"
                
                elif rule_type == 'range':
                    min_val, max_val = rule.get('min'), rule.get('max')
                    out_of_range = 0
                    if min_val is not None:
                        out_of_range += (df[column] < min_val).sum()
                    if max_val is not None:
                        out_of_range += (df[column] > max_val).sum()
                    check_result['passed'] = out_of_range == 0
                    check_result['details'] = f"Values out of range: {out_of_range}"
                
                elif rule_type == 'format':
                    pattern = rule.get('pattern')
                    if pattern:
                        invalid_format = ~df[column].astype(str).str.match(pattern)
                        invalid_count = invalid_format.sum()
                        check_result['passed'] = invalid_count == 0
                        check_result['details'] = f"Invalid format values: {invalid_count}"
                
                if check_result['passed']:
                    results['passed_checks'] += 1
                else:
                    results['failed_checks'] += 1
                    
            except Exception as e:
                check_result['details'] = f"Check failed with error: {str(e)}"
                results['failed_checks'] += 1
            
            results['check_results'].append(check_result)
        
        return results
    
    def add_audit_columns(self, df: pd.DataFrame, source_system: str = 'unknown') -> pd.DataFrame:
        """Add standard audit columns to DataFrame."""
        df = df.copy()
        current_time = datetime.now(timezone.utc).isoformat()
        
        df['_source_system'] = source_system
        df['_ingestion_timestamp'] = current_time
        df['_processed_timestamp'] = current_time
        df['_record_id'] = range(1, len(df) + 1)
        
        return df


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main handler for data transformation Lambda function.
    
    Expected event structure:
    {
        "transformation_type": "string",         # csv_processing, json_processing, data_quality
        "source_bucket": "string",
        "source_key": "string", 
        "target_bucket": "string",
        "target_key": "string",
        "table_name": "string",
        "domain": "string",
        "transformations": [                     # List of transformations to apply
            {
                "type": "string",                # filter, rename, add_column, data_quality
                "config": {}                     # Transformation-specific config
            }
        ],
        "data_quality_rules": [                  # Optional data quality rules
            {
                "type": "not_null",
                "column": "string"
            }
        ]
    }
    
    Args:
        event: Lambda event data containing transformation configuration
        context: Lambda context
        
    Returns:
        Response dictionary with transformation results
    """
    try:
        logger.info(f"Received event: {json.dumps(event, default=str)}")
        
        # Extract event parameters
        transformation_type = event.get('transformation_type', 'csv_processing')
        source_bucket = event.get('source_bucket')
        source_key = event.get('source_key')
        target_bucket = event.get('target_bucket')
        target_key = event.get('target_key')
        table_name = event.get('table_name')
        domain = event.get('domain')
        transformations = event.get('transformations', [])
        data_quality_rules = event.get('data_quality_rules', [])
        
        # Environment variables
        environment = os.environ.get('ENVIRONMENT')
        default_source_bucket = os.environ.get('RAW_BUCKET')
        default_target_bucket = os.environ.get('CURATED_BUCKET')
        
        # Use defaults if not provided
        source_bucket = source_bucket or default_source_bucket
        target_bucket = target_bucket or default_target_bucket
        
        # Validate required parameters
        if not source_bucket or not source_key:
            raise ValueError("source_bucket and source_key are required")
        if not target_bucket or not target_key:
            raise ValueError("target_bucket and target_key are required")
        
        logger.info(f"Processing {transformation_type} from s3://{source_bucket}/{source_key}")
        logger.info(f"Output to s3://{target_bucket}/{target_key}")
        
        # Initialize data transformer
        transformer = DataTransformer()
        
        # Process based on transformation type
        if transformation_type in ['csv_processing', 'data_quality']:
            # Read CSV data
            df = transformer.read_csv_from_s3(source_bucket, source_key)
            if df is None:
                raise Exception("Failed to read source CSV data")
            
            original_row_count = len(df)
            logger.info(f"Processing {original_row_count} rows")
            
            # Apply transformations
            for transformation in transformations:
                transform_type = transformation.get('type')
                config = transformation.get('config', {})
                
                if transform_type == 'filter':
                    # Apply filter condition
                    condition = config.get('condition')
                    if condition:
                        df = df.query(condition)
                        logger.info(f"Applied filter '{condition}': {len(df)} rows remaining")
                
                elif transform_type == 'rename':
                    # Rename columns
                    column_mapping = config.get('columns', {})
                    df = df.rename(columns=column_mapping)
                    logger.info(f"Renamed columns: {list(column_mapping.keys())}")
                
                elif transform_type == 'add_column':
                    # Add new column with value
                    column_name = config.get('name')
                    column_value = config.get('value')
                    if column_name:
                        df[column_name] = column_value
                        logger.info(f"Added column '{column_name}' with value '{column_value}'")
                
                elif transform_type == 'drop_columns':
                    # Drop specified columns
                    columns_to_drop = config.get('columns', [])
                    existing_columns = [col for col in columns_to_drop if col in df.columns]
                    if existing_columns:
                        df = df.drop(columns=existing_columns)
                        logger.info(f"Dropped columns: {existing_columns}")
            
            # Add audit columns
            source_system = f"{domain}/{table_name}" if domain and table_name else 'unknown'
            df = transformer.add_audit_columns(df, source_system)
            
            # Apply data quality checks if specified
            data_quality_results = None
            if data_quality_rules:
                logger.info("Applying data quality checks")
                data_quality_results = transformer.apply_data_quality_checks(df, data_quality_rules)
                logger.info(f"Data quality: {data_quality_results['passed_checks']} passed, {data_quality_results['failed_checks']} failed")
            
            # Write transformed data
            success = transformer.write_csv_to_s3(df, target_bucket, target_key)
            if not success:
                raise Exception("Failed to write transformed data")
            
            # Prepare result
            result = {
                "statusCode": 200,
                "body": {
                    "message": "Data transformation completed successfully",
                    "transformation_type": transformation_type,
                    "source_location": f"s3://{source_bucket}/{source_key}",
                    "target_location": f"s3://{target_bucket}/{target_key}",
                    "original_row_count": original_row_count,
                    "final_row_count": len(df),
                    "transformations_applied": len(transformations),
                    "table_name": table_name,
                    "domain": domain,
                    "environment": environment
                }
            }
            
            if data_quality_results:
                result["body"]["data_quality_results"] = data_quality_results
        
        elif transformation_type == 'json_processing':
            # Read JSON data
            data = transformer.read_json_from_s3(source_bucket, source_key)
            if data is None:
                raise Exception("Failed to read source JSON data")
            
            # Basic JSON processing (can be extended)
            if isinstance(data, list):
                original_count = len(data)
            else:
                original_count = 1
            
            # Write processed data
            success = transformer.write_json_to_s3(data, target_bucket, target_key)
            if not success:
                raise Exception("Failed to write transformed JSON data")
            
            result = {
                "statusCode": 200,
                "body": {
                    "message": "JSON transformation completed successfully",
                    "transformation_type": transformation_type,
                    "source_location": f"s3://{source_bucket}/{source_key}",
                    "target_location": f"s3://{target_bucket}/{target_key}",
                    "record_count": original_count,
                    "table_name": table_name,
                    "domain": domain,
                    "environment": environment
                }
            }
        
        else:
            raise ValueError(f"Unsupported transformation type: {transformation_type}")
        
        logger.info(f"Successfully completed {transformation_type} transformation")
        return result
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            "statusCode": 400,
            "body": {
                "error": str(e),
                "message": "Invalid transformation parameters"
            }
        }
    
    except Exception as e:
        logger.error(f"Error in data transformation: {str(e)}")
        return {
            "statusCode": 500,
            "body": {
                "error": str(e),
                "message": "Data transformation failed"
            }
        }