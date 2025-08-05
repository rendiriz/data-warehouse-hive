import asyncio
import logging
import uvloop
from sanic import Sanic, Request, response
from sanic.response import JSONResponse

from config import config
from csv_processor import CSVProcessor
from hive_manager import HiveManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Sanic app
app = Sanic("csv-hive-processor")

# Add CORS headers
@app.middleware("response")
async def add_cors_headers(request, response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"

# Initialize processors
csv_processor = CSVProcessor()
hive_manager = HiveManager()

@app.route("/process-csv", methods=["POST"])
async def process_csv_endpoint(request: Request) -> JSONResponse:
    """
    Main endpoint to create Hive external table from CSV in S3
    
    Expected JSON payload:
    {
        "s3_key": "uploads/6512ecd72353cc2096d991c883aca897/6512ecd72353cc2096d991c883aca897.csv",
        "table_name": "my_table",
        "drop_if_exists": false,  // optional
        "has_header": true        // optional, defaults to true
    }
    """
    try:
        # Parse request
        data = request.json
        s3_key = data.get("s3_key")
        table_name = data.get("table_name")
        drop_if_exists = data.get("drop_if_exists", False)
        has_header = data.get("has_header", True)
        
        if not s3_key or not table_name:
            return response.json(
                {"error": "Missing required fields: s3_key, table_name"}, 
                status=400
            )
        
        logger.info(f"Creating external table for CSV: {s3_key} -> {table_name}")
        
        # Step 1: Load CSV from S3 to infer schema (sample only for large files)
        df = await csv_processor.load_csv_from_s3(s3_key)
        
        # Step 2: Infer schema with Pandera
        schema = csv_processor.infer_schema_with_pandera(df)
        logger.info(f"Schema validation passed - inferred from {len(df)} sample rows")
        
        # Step 2.5: Test Hive external table creation capability
        # logger.info("Testing Hive external table creation capability...")
        # test_result = await hive_manager.test_table_creation()
        # if not test_result:
        #     logger.error("Hive external table creation test failed - there may be configuration issues")
        #     return response.json(
        #         {"error": "Hive configuration issue - external table creation test failed"}, 
        #         status=500
        #     )
        # logger.info("Hive external table creation test passed")
        
        # Step 3: Extract file path from s3_key for external table location
        # Remove file extension to get directory path (for external table LOCATION)
        file_path = s3_key.replace("uploads/", "")  # Remove uploads prefix if present
        if file_path.endswith('.csv'):
            file_path = file_path[:-4]  # Remove .csv extension
        
        # Step 4: Create Hive external table pointing to S3 data
        actual_table_name = await hive_manager.create_hive_external_table(
            table_name=table_name, 
            df=df, 
            file_path=file_path,
            drop_if_exists=drop_if_exists,
            has_header=has_header
        )
        
        # # Step 5: Get table info to verify creation and get row count
        # table_info = await hive_manager.get_table_info(actual_table_name)
        
        # Step 6: Optional - Refresh table metadata to ensure S3 data is recognized
        await hive_manager.refresh_table(actual_table_name)
        
        return response.json({
            "status": "success",
            "message": f"Successfully created external table for {s3_key}",
            "table_name": actual_table_name,
            "table_type": "external",
            "s3_location": f"s3a://{config.S3_BUCKET}/uploads/{file_path}/",
            "schema_columns": len(df.columns),
            "columns": [{"name": col, "type": str(dtype)} for col, dtype in zip(df.columns, df.dtypes)],
            # "estimated_rows": table_info.get("row_count", "unknown"),
            "sample_file_size_mb": round(df.estimated_size("mb"), 2),
            "has_header": has_header
        })
        
    except Exception as e:
        logger.error(f"Error creating external table: {e}")
        return response.json(
            {"error": f"External table creation failed: {str(e)}"}, 
            status=500
        )

@app.route("/health", methods=["GET"])
async def health_check(request: Request) -> JSONResponse:
    """Health check endpoint"""
    try:
        # Test S3 connection
        csv_processor.s3_client.list_buckets()
        s3_status = "connected"
    except Exception as e:
        s3_status = f"error: {str(e)}"
    
    try:
        # Test Hive connection
        async with hive_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        hive_status = "connected"
    except Exception as e:
        hive_status = f"error: {str(e)}"
    
    return response.json({
        "status": "healthy",
        "service": "csv-hive-processor",
        "connections": {
            "s3": s3_status,
            "hive": hive_status
        }
    })

@app.route("/schema/<s3_key:path>", methods=["GET"])
async def get_schema_preview(request: Request, s3_key: str) -> JSONResponse:
    """Preview schema without processing the full file"""
    try:
        # Load sample data for schema preview
        df_sample = await csv_processor.load_csv_from_s3(s3_key, sample_only=True)
        
        # Get column statistics
        column_stats = csv_processor.get_column_stats(df_sample)
        
        # Generate Hive schema
        hive_schema = {}
        for col_name, dtype in zip(df_sample.columns, df_sample.dtypes):
            hive_schema[col_name] = {
                "polars_type": str(dtype),
                "hive_type": hive_manager.generate_hive_column_type(dtype)
            }
        
        return response.json({
            "s3_key": s3_key,
            "sample_rows": len(df_sample),
            "columns": len(df_sample.columns),
            "schema": hive_schema,
            "statistics": column_stats
        })
        
    except Exception as e:
        logger.error(f"Error getting schema preview: {e}")
        return response.json(
            {"error": f"Schema preview failed: {str(e)}"}, 
            status=500
        )

@app.route("/tables", methods=["GET"])
async def list_tables(request: Request) -> JSONResponse:
    """List all tables in Hive database"""
    try:
        async with hive_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
        
        return response.json({
            "database": config.HIVE_DATABASE,
            "tables": tables,
            "count": len(tables)
        })
        
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return response.json(
            {"error": f"Failed to list tables: {str(e)}"}, 
            status=500
        )

@app.route("/table/<table_name>/info", methods=["GET"])
async def get_table_info(request: Request, table_name: str) -> JSONResponse:
    """Get detailed information about a specific table"""
    try:
        table_info = await hive_manager.get_table_info(table_name)
        return response.json(table_info)
        
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
        return response.json(
            {"error": f"Failed to get table info: {str(e)}"}, 
            status=500
        )

@app.route("/table/<table_name>", methods=["DELETE"])
async def drop_table(request: Request, table_name: str) -> JSONResponse:
    """Drop a table"""
    try:
        await hive_manager.drop_table(table_name)
        return response.json({
            "status": "success",
            "message": f"Table {table_name} dropped successfully"
        })
        
    except Exception as e:
        logger.error(f"Error dropping table: {e}")
        return response.json(
            {"error": f"Failed to drop table: {str(e)}"}, 
            status=500
        )

# Error handlers
@app.exception(Exception)
async def handle_exception(request: Request, exception: Exception):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exception}")
    return response.json({
        "error": "Internal server error",
        "message": str(exception)
    }, status=500)

@app.before_server_start
async def setup_app(app, loop):
    """Initialize app before starting"""
    logger.info("Starting CSV-Hive Processor")
    
    # Validate configuration
    try:
        config.validate()
        logger.info("Configuration validated successfully")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        raise

if __name__ == "__main__":
    # Use uvloop for better async performance
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    
    # Run the app
    app.run(
        host=config.HOST,
        port=config.PORT,
        debug=config.DEBUG,
        workers=config.WORKERS,
        access_log=True
    )