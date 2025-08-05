import logging
import re
import time
from contextlib import asynccontextmanager
from typing import List, Tuple, Any
import os

import polars as pl
from pyhive import hive

from config import config

logger = logging.getLogger(__name__)

class HiveManager:
    def __init__(self):
        self.connection = None
    
    @asynccontextmanager
    async def get_connection(self):
        """Async context manager for Hive connection"""
        try:
            logger.info(f"Connecting to Hive at {config.HIVE_HOST}:{config.HIVE_PORT}")
            self.connection = hive.Connection(
                host=config.HIVE_HOST,
                port=config.HIVE_PORT,
                username=config.HIVE_USERNAME,
                database=config.HIVE_DATABASE
            )
            yield self.connection
        except Exception as e:
            logger.error(f"Failed to connect to Hive: {e}")
            raise
        finally:
            if self.connection:
                try:
                    self.connection.close()
                    logger.info("Hive connection closed")
                except Exception as e:
                    logger.warning(f"Error closing Hive connection: {e}")
    
    def generate_hive_column_type(self, polars_dtype) -> str:
        """Convert Polars dtype to Hive column type"""
        dtype_str = str(polars_dtype)
        
        if 'Int8' in dtype_str:
            return 'TINYINT'
        elif 'Int16' in dtype_str:
            return 'SMALLINT'
        elif 'Int32' in dtype_str:
            return 'INT'
        elif 'Int64' in dtype_str or 'Int' in dtype_str:
            return 'BIGINT'
        elif 'Float32' in dtype_str:
            return 'FLOAT'
        elif 'Float64' in dtype_str or 'Float' in dtype_str:
            return 'DOUBLE'
        elif 'Bool' in dtype_str:
            return 'BOOLEAN'
        elif 'Date' in dtype_str:
            return 'DATE'
        elif 'Datetime' in dtype_str:
            return 'TIMESTAMP'
        elif 'Utf8' in dtype_str or 'String' in dtype_str:
            return 'STRING'
        else:
            return 'STRING'
    
    def sanitize_table_name(self, table_name: str) -> str:
        """Sanitize table name for Hive compatibility"""
        # Remove or replace invalid characters
        # Hive table names can contain: letters, digits, underscores
        # Replace dots, hyphens, and other special characters with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = 'table_' + sanitized
            
        # Ensure it's not empty
        if not sanitized:
            sanitized = 'table_unknown'
            
        # Limit length to avoid issues - make it shorter for better compatibility
        if len(sanitized) > 32:
            sanitized = sanitized[:32]
            
        # Add a timestamp suffix to make it unique and avoid conflicts
        timestamp = str(int(time.time()))[-6:]  # Last 6 digits of timestamp
        sanitized = f"{sanitized}_{timestamp}"
            
        return sanitized
    
    def construct_s3_location(self, file_path: str) -> str:
        """Construct S3 location from file path or upload ID"""
        # Remove file extension to get directory path
        base_path = os.path.splitext(file_path)[0]
        
        # Construct S3 location - assumes your tus.io uploads go to 'uploads/' prefix
        s3_location = f"s3a://{config.S3_BUCKET}/{base_path}"
        
        logger.info(f"Constructed S3 location: {s3_location}")
        return s3_location
    
    async def table_exists(self, table_name: str) -> bool:
        """Check if table exists in Hive"""
        try:
            # Sanitize table name for Hive compatibility
            sanitized_table_name = self.sanitize_table_name(table_name)
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{sanitized_table_name}'")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            return False
    
    async def drop_table(self, table_name: str) -> bool:
        """Drop external table if exists (only removes metadata, keeps S3 data)"""
        try:
            # Sanitize table name for Hive compatibility
            sanitized_table_name = self.sanitize_table_name(table_name)
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {sanitized_table_name}")
                logger.info(f"Dropped external table: {sanitized_table_name} (S3 data preserved)")
                return True
        except Exception as e:
            logger.error(f"Error dropping table: {e}")
            raise
    
    async def create_hive_external_table(self, table_name: str, df: pl.DataFrame, file_path: str, 
                                       drop_if_exists: bool = False, has_header: bool = True) -> str:
        """Create Hive external table pointing to S3 data"""
        try:
            # Sanitize table name for Hive compatibility
            sanitized_table_name = self.sanitize_table_name(table_name)
            logger.info(f"Original table name: {table_name}, sanitized: {sanitized_table_name}")
            
            # Drop table if requested
            if drop_if_exists:
                await self.drop_table(sanitized_table_name)
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Test connection first
                logger.info("Testing Hive connection...")
                cursor.execute("SELECT 1")
                test_result = cursor.fetchone()
                logger.info(f"Hive connection test result: {test_result}")
                
                # Generate column definitions
                columns = []
                for col_name, dtype in zip(df.columns, df.dtypes):
                    hive_type = self.generate_hive_column_type(dtype)
                    # Escape column names with backticks to handle special characters
                    columns.append(f"`{col_name}` {hive_type}")
                
                logger.info(f"Generated columns: {columns}")
                
                # Construct S3 location
                s3_location = self.construct_s3_location(file_path)
                
                # Create external table DDL
                create_table_sql = f"""
                CREATE EXTERNAL TABLE {sanitized_table_name} (
                    {', '.join(columns)}
                )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
                LOCATION '{s3_location}'
                """
                
                # Add table properties for CSV with header
                if has_header:
                    create_table_sql += "\nTBLPROPERTIES ('skip.header.line.count'='1')"
                
                logger.info(f"Creating external table with SQL: {create_table_sql}")
                
                try:
                    cursor.execute(create_table_sql)
                    logger.info(f"Successfully created external table: {sanitized_table_name}")
                    return sanitized_table_name
                except Exception as create_error:
                    logger.error(f"Failed to create external table with error: {create_error}")
                    logger.error(f"Error type: {type(create_error)}")
                    logger.error(f"Error args: {create_error.args}")
                    
                    # Try alternative approach with a simpler name
                    logger.info("Trying alternative approach with simpler table name...")
                    simple_table_name = f"csv_data_{int(time.time())}"
                    
                    simple_create_sql = f"""
                    CREATE EXTERNAL TABLE {simple_table_name} (
                        {', '.join(columns)}
                    )
                    ROW FORMAT DELIMITED
                    FIELDS TERMINATED BY ','
                    STORED AS TEXTFILE
                    LOCATION '{s3_location}'
                    """
                    
                    if has_header:
                        simple_create_sql += "\nTBLPROPERTIES ('skip.header.line.count'='1')"
                    
                    try:
                        cursor.execute(simple_create_sql)
                        logger.info(f"Successfully created external table with simple name: {simple_table_name}")
                        return simple_table_name
                    except Exception as simple_error:
                        logger.error(f"Alternative approach also failed: {simple_error}")
                        raise create_error
                
        except Exception as e:
            logger.error(f"Error creating Hive external table: {e}")
            logger.error(f"Full error details: {str(e)}")
            raise
    
    async def create_hive_table(self, table_name: str, df: pl.DataFrame, file_path: str = None, 
                              drop_if_exists: bool = False, has_header: bool = True) -> str:
        """Wrapper method - now defaults to creating external tables"""
        if file_path:
            return await self.create_hive_external_table(table_name, df, file_path, drop_if_exists, has_header)
        else:
            # Fallback to managed table if no file path provided
            logger.warning("No file_path provided, creating managed table instead")
            return await self.create_hive_managed_table(table_name, df, drop_if_exists)
    
    async def create_hive_managed_table(self, table_name: str, df: pl.DataFrame, drop_if_exists: bool = False) -> str:
        """Create managed Hive table (original implementation for fallback)"""
        try:
            # Sanitize table name for Hive compatibility
            sanitized_table_name = self.sanitize_table_name(table_name)
            logger.info(f"Creating managed table - Original table name: {table_name}, sanitized: {sanitized_table_name}")
            
            # Drop table if requested
            if drop_if_exists:
                await self.drop_table(sanitized_table_name)
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Generate column definitions
                columns = []
                for col_name, dtype in zip(df.columns, df.dtypes):
                    hive_type = self.generate_hive_column_type(dtype)
                    columns.append(f"`{col_name}` {hive_type}")
                
                # Create table DDL
                create_table_sql = f"""
                CREATE TABLE {sanitized_table_name} (
                    {', '.join(columns)}
                )
                """
                
                logger.info(f"Creating managed table with SQL: {create_table_sql}")
                cursor.execute(create_table_sql)
                logger.info(f"Successfully created managed table: {sanitized_table_name}")
                return sanitized_table_name
                
        except Exception as e:
            logger.error(f"Error creating managed Hive table: {e}")
            raise
    
    def prepare_row_for_hive(self, row: List[Any]) -> List[Any]:
        """Prepare a row for Hive insertion by handling None values and data types"""
        prepared_row = []
        for value in row:
            if value is None or (isinstance(value, float) and pl.Series([value]).is_nan().item()):
                prepared_row.append(None)
            elif isinstance(value, bool):
                prepared_row.append(str(value).lower())
            else:
                prepared_row.append(value)
        return prepared_row
    
    async def batch_insert_to_hive(self, table_name: str, df: pl.DataFrame) -> int:
        """Batch insert data to Hive table (only works with managed tables)"""
        logger.warning("Note: batch_insert_to_hive only works with managed tables, not external tables")
        try:
            # Sanitize table name for Hive compatibility
            sanitized_table_name = self.sanitize_table_name(table_name)
            
            total_rows = len(df)
            inserted_rows = 0
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Process in batches
                for i in range(0, total_rows, config.BATCH_SIZE):
                    batch_df = df.slice(i, config.BATCH_SIZE)
                    
                    # Convert to list of tuples for insertion, handling special values
                    batch_data = []
                    for row in batch_df.to_numpy():
                        prepared_row = self.prepare_row_for_hive(row.tolist())
                        batch_data.append(tuple(prepared_row))
                    
                    # Generate INSERT statement
                    placeholders = ', '.join(['%s'] * len(df.columns))
                    insert_sql = f"INSERT INTO {sanitized_table_name} VALUES ({placeholders})"
                    
                    # Execute batch insert
                    try:
                        cursor.executemany(insert_sql, batch_data)
                        batch_size = len(batch_data)
                        inserted_rows += batch_size
                        
                        logger.info(f"Inserted batch {i//config.BATCH_SIZE + 1}: {inserted_rows}/{total_rows} rows")
                        
                    except Exception as batch_error:
                        logger.error(f"Error in batch {i//config.BATCH_SIZE + 1}: {batch_error}")
                        # Try inserting rows one by one to identify problematic rows
                        for j, single_row in enumerate(batch_data):
                            try:
                                cursor.execute(insert_sql, single_row)
                                inserted_rows += 1
                            except Exception as row_error:
                                logger.warning(f"Failed to insert row {i + j}: {row_error}")
                
                logger.info(f"Successfully inserted {inserted_rows} rows into {sanitized_table_name}")
                return inserted_rows
                
        except Exception as e:
            logger.error(f"Error inserting data to Hive: {e}")
            raise
    
    async def refresh_table(self, table_name: str) -> bool:
        """Refresh external table metadata (useful after S3 data changes)"""
        try:
            sanitized_table_name = self.sanitize_table_name(table_name)
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Refresh table metadata
                cursor.execute(f"MSCK REPAIR TABLE {sanitized_table_name}")
                logger.info(f"Refreshed external table metadata: {sanitized_table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error refreshing table: {e}")
            return False
    
    async def test_table_creation(self) -> bool:
        """Test if we can create a simple external table in Hive"""
        try:
            test_table_name = "test_external_table_creation"
            logger.info(f"Testing external table creation with name: {test_table_name}")
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Try to create a simple test external table
                test_s3_location = f"s3a://{config.S3_BUCKET}/test/"
                create_test_sql = f"""
                CREATE EXTERNAL TABLE {test_table_name} (
                    id INT,
                    name STRING
                )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
                LOCATION '{test_s3_location}'
                """
                
                logger.info(f"Creating test external table with SQL: {create_test_sql}")
                cursor.execute(create_test_sql)
                
                # Try to drop it
                cursor.execute(f"DROP TABLE {test_table_name}")
                
                logger.info("Test external table creation successful")
                return True
                
        except Exception as e:
            logger.error(f"Test external table creation failed: {e}")
            return False

    async def get_table_info(self, table_name: str) -> dict:
        """Get table information"""
        try:
            # Sanitize table name for Hive compatibility
            sanitized_table_name = self.sanitize_table_name(table_name)
            
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get table schema
                cursor.execute(f"DESCRIBE {sanitized_table_name}")
                columns = cursor.fetchall()
                
                # Get row count (may be slow for large external tables)
                cursor.execute(f"SELECT COUNT(*) FROM {sanitized_table_name}")
                row_count = cursor.fetchone()[0]
                
                # Check if it's an external table
                cursor.execute(f"DESCRIBE FORMATTED {sanitized_table_name}")
                table_details = cursor.fetchall()
                
                is_external = any("EXTERNAL" in str(detail).upper() for detail in table_details)
                
                return {
                    "table_name": table_name,
                    "sanitized_name": sanitized_table_name,
                    "columns": [{"name": col[0], "type": col[1]} for col in columns],
                    "row_count": row_count,
                    "is_external": is_external
                }
                
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            raise