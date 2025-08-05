import csv
import io
import logging
from typing import Dict, List, Optional, Any

import boto3
import polars as pl
import pandera.pandas as pa
from pandera import Column, DataFrameSchema

from config import config

logger = logging.getLogger(__name__)

class CSVProcessor:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=config.AWS_URL,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            region_name=config.AWS_REGION
        )
    
    async def detect_delimiter(self, sample_data: str) -> str:
        """Detect CSV delimiter using csv.Sniffer"""
        try:
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample_data, delimiters=',;\t|').delimiter
            logger.info(f"Detected delimiter: '{delimiter}'")
            return delimiter
        except Exception as e:
            logger.warning(f"Could not detect delimiter, using comma: {e}")
            return ','
    
    async def load_csv_from_s3(self, s3_key: str, sample_only: bool = False) -> pl.DataFrame:
        """Load CSV from S3 and return Polars DataFrame. 
        If the file is in the root, move it to s3_key/s3_key and move s3_key.info to s3_key/s3_key.info.
        """
        try:
            logger.info(f"Loading CSV from S3: {s3_key}")

            # Check if the file exists at the root (i.e., s3_key) and move it to s3_key/s3_key if needed
            def object_exists(key):
                try:
                    self.s3_client.head_object(Bucket=config.S3_BUCKET, Key=key)
                    return True
                except Exception:
                    return False

            # If the file is at the root (e.g., "abc123.csv"), move it to "abc123.csv/abc123.csv"
            root_file_exists = object_exists(s3_key)
            nested_file_key = f"{s3_key}/{s3_key}"
            nested_info_key = f"{s3_key}/{s3_key}.info"
            info_key = f"{s3_key}.info"

            # Move the main file if needed
            if root_file_exists and not object_exists(nested_file_key):
                logger.info(f"Moving root file {s3_key} to {nested_file_key}")
                copy_source = {'Bucket': config.S3_BUCKET, 'Key': s3_key}
                self.s3_client.copy_object(Bucket=config.S3_BUCKET, CopySource=copy_source, Key=nested_file_key)
                self.s3_client.delete_object(Bucket=config.S3_BUCKET, Key=s3_key)
                logger.info(f"Moved {s3_key} to {nested_file_key}")

            # Move the .info file if needed
            if object_exists(info_key) and not object_exists(nested_info_key):
                logger.info(f"Moving info file {info_key} to {nested_info_key}")
                copy_source = {'Bucket': config.S3_BUCKET, 'Key': info_key}
                self.s3_client.copy_object(Bucket=config.S3_BUCKET, CopySource=copy_source, Key=nested_info_key)
                self.s3_client.delete_object(Bucket=config.S3_BUCKET, Key=info_key)
                logger.info(f"Moved {info_key} to {nested_info_key}")

            # Now, always try to load from nested_file_key first, fallback to s3_key
            try:
                logger.info(f"Attempting to load file with key: {nested_file_key}")
                response = self.s3_client.get_object(Bucket=config.S3_BUCKET, Key=nested_file_key)
                csv_content = response['Body'].read().decode('utf-8')
                logger.info(f"Successfully loaded file with key: {nested_file_key}")
            except Exception as e_nested:
                logger.warning(f"Failed to load file with key: {nested_file_key}, trying {s3_key}")
                try:
                    response = self.s3_client.get_object(Bucket=config.S3_BUCKET, Key=s3_key)
                    csv_content = response['Body'].read().decode('utf-8')
                    logger.info(f"Successfully loaded file with key: {s3_key}")
                except Exception as e_root:
                    logger.error(f"Failed to load file with both keys: {nested_file_key} and {s3_key}")
                    raise e_root

            # Detect delimiter using first 1024 characters
            sample = csv_content[:1024]
            delimiter = await self.detect_delimiter(sample)

            # For sample mode, limit rows
            n_rows = config.MAX_SAMPLE_SIZE if sample_only else None

            # Load with Polars for fast processing
            df = pl.read_csv(
                io.StringIO(csv_content),
                separator=delimiter,
                try_parse_dates=True,
                infer_schema_length=1000,
                n_rows=n_rows
            )

            logger.info(f"Loaded DataFrame with shape: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Error loading CSV from S3: {e}")
            raise
    
    def infer_schema_with_pandera(self, df: pl.DataFrame) -> DataFrameSchema:
        """Infer schema using Pandera"""
        try:
            # Convert to pandas for pandera (pandera doesn't support polars directly yet)
            pandas_df = df.to_pandas()
            
            # Infer schema
            schema_dict = {}
            for column, dtype in pandas_df.dtypes.items():
                if dtype == 'object':
                    schema_dict[column] = Column(str, nullable=True)
                elif dtype == 'int64':
                    schema_dict[column] = Column(int, nullable=True)
                elif dtype == 'float64':
                    schema_dict[column] = Column(float, nullable=True)
                elif dtype == 'bool':
                    schema_dict[column] = Column(bool, nullable=True)
                elif 'datetime' in str(dtype):
                    schema_dict[column] = Column('datetime64[ns]', nullable=True)
                else:
                    schema_dict[column] = Column(str, nullable=True)
            
            schema = DataFrameSchema(schema_dict)
            logger.info(f"Inferred schema for {len(schema_dict)} columns")
            
            # Validate the dataframe against the schema
            validated_df = schema.validate(pandas_df)
            logger.info("Schema validation successful")
            
            return schema
            
        except Exception as e:
            logger.error(f"Error inferring schema: {e}")
            raise
    
    def get_column_stats(self, df: pl.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Get statistics for each column"""
        stats = {}
        
        for col_name in df.columns:
            col_data = df.select(pl.col(col_name))
            
            stats[col_name] = {
                "dtype": str(df[col_name].dtype),
                "null_count": col_data.null_count().item(),
                "non_null_count": len(df) - col_data.null_count().item(),
                "unique_count": col_data.n_unique(),
            }
            
            # Add type-specific stats
            if df[col_name].dtype.is_numeric():
                col_stats = df.select([
                    pl.col(col_name).min().alias("min"),
                    pl.col(col_name).max().alias("max"),
                    pl.col(col_name).mean().alias("mean"),
                    pl.col(col_name).std().alias("std")
                ]).to_dicts()[0]
                stats[col_name].update(col_stats)
        
        return stats