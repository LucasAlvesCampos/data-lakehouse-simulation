import logging
from pyspark.sql.functions import col, when, to_date
from pyspark.sql.types import StringType, IntegerType, DateType
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
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark Running")

        # Read Iceberg table from Bronze layer
        logger.info(f"Reading source table from Bronze layer: {BRONZE_LAYER_DATABASE}.{BRONZE_TRANSPORT_TABLE}...")
        df = spark.table(f"nessie.{BRONZE_LAYER_DATABASE}.{BRONZE_TRANSPORT_TABLE}")
        
        # Extract date and time from timestamp and separate in two columns
        df = df.withColumn("HORA_INICIO", col("DATA_INICIO").substr(12, 8))\
            .withColumn("HORA_FIM", col("DATA_FIM").substr(12, 8))\
            .withColumn("DATA_INICIO", to_date(col("DATA_INICIO").substr(1, 10), "MM-dd-yyyy"))\
            .withColumn("DATA_FIM", to_date(col("DATA_FIM").substr(1, 10), "MM-dd-yyyy"))

        # Clean and transform data
        logger.info("\nCleaning and transforming data...")
        cleaned_df = df.select(
            col("DATA_INICIO").cast(DateType()),
            col("HORA_INICIO"),  # Include the new time column
            col("DATA_FIM").cast(DateType()),
            col("HORA_FIM"),     # Include the new time column
            when(col("CATEGORIA").isNull(), "N/A").otherwise(col("CATEGORIA")).cast(StringType()).alias("CATEGORIA"),
            when(col("LOCAL_INICIO").isNull(), "N/A").otherwise(col("LOCAL_INICIO")).cast(StringType()).alias("LOCAL_INICIO"),
            when(col("LOCAL_FIM").isNull(), "N/A").otherwise(col("LOCAL_FIM")).cast(StringType()).alias("LOCAL_FIM"),        
            when(col("DISTANCIA").isNull(), 0).otherwise(col("DISTANCIA")).cast(IntegerType()).alias("DISTANCIA"),
            when(col("PROPOSITO").isNull(), "N/A").otherwise(col("PROPOSITO")).cast(StringType()).alias("PROPOSITO")
        )
        
        # Show sample of cleaned data
        logger.info("\nSample of cleaned data:")
        cleaned_df.show(5, truncate=False)
        
        # Create database and table
        table_name = f"nessie.{SILVER_LAYER_DATABASE}.{SILVER_TRANSPORT_TABLE}"
        logger.info(f"Creating Iceberg table: {table_name}")
            
        spark.sql(f"CREATE DATABASE IF NOT EXISTS nessie.{SILVER_LAYER_DATABASE}")

        # Save cleaned data using the cleaned dataframe
        logger.info(f"\nSaving cleaned data to Silver layer: {SILVER_LAYER_DATABASE}.{SILVER_TRANSPORT_TABLE}...")
        cleaned_df.writeTo(table_name) \
            .using("iceberg") \
            .createOrReplace()
        
        logger.info("Data cleaning completed successfully!")

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