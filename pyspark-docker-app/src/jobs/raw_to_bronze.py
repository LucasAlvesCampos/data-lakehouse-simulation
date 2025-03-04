import logging
from datetime import datetime
from src.utils.spark_utils import create_spark_session
from src.config.config import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main execution function"""
    start_time = datetime.now()
    logger.info("Starting data ingestion process")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Read CSV file
        logger.info("Reading CSV file...")
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .csv("/app/src/data/info_transportes.csv")
        
        # Show sample and statistics
        logger.info("\nSample of loaded data:")
        df.show(5, truncate=False)
        logger.info(f"Total records: {df.count()}")
        
        # Create database and table
        table_name = f"nessie.{BRONZE_LAYER_DATABASE}.{BRONZE_TRANSPORT_TABLE}"
        logger.info(f"Creating Iceberg table: {table_name}")
        
        spark.sql(f"CREATE DATABASE IF NOT EXISTS nessie.{BRONZE_LAYER_DATABASE}")
        
        # Write to Iceberg table with optimizations
        df.writeTo(table_name) \
            .using("iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .tableProperty("write.metadata.compression-codec", "gzip") \
            .createOrReplace()
        
        logger.info("Data successfully saved as Iceberg table in Nessie catalog")
        
        # Log execution time
        execution_time = datetime.now() - start_time
        logger.info(f"Total execution time: {execution_time}")
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise e
    
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()