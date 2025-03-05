import logging
import re
from pyspark.sql.functions import col, when, to_date, regexp_extract, length, lit
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
        df = spark.table(f"{LAKEHOUSE_CATALOG}.{BRONZE_LAYER_DATABASE}.{BRONZE_TRANSPORT_TABLE}")
        
        # Count rows before dropping duplicates
        row_count_before = df.count()
        logger.info(f"Number of rows before dropping duplicates: {row_count_before}")

        # Drop duplicates if there are any
        df = df.dropDuplicates()
        
        # Count rows after dropping duplicates
        row_count_after = df.count()
        logger.info(f"Number of rows after dropping duplicates: {row_count_after}")
        
        # Report if any duplicates were found and removed
        if row_count_before > row_count_after:
            logger.warning(f"Found and removed {row_count_before - row_count_after} duplicate rows")
        else:
            logger.info("No duplicate rows found")

        # Check for NULL dates and invalid formats
        date_pattern = "^\\d{2}-\\d{2}-\\d{4}\\s\\d{1,2}:\\d{2}$"
        
        # Add validation columns
        df_validated = df.withColumn(
            "DATA_INICIO_VALID", 
            when(col("DATA_INICIO").isNull(), lit(False))
            .when(col("DATA_INICIO").rlike(date_pattern), lit(True))
            .otherwise(lit(False))
        ).withColumn(
            "DATA_FIM_VALID", 
            when(col("DATA_FIM").isNull(), lit(False))
            .when(col("DATA_FIM").rlike(date_pattern), lit(True))
            .otherwise(lit(False))
        )
        
        # Count invalid records
        invalid_start_dates = df_validated.filter(~col("DATA_INICIO_VALID")).count()
        invalid_end_dates = df_validated.filter(~col("DATA_FIM_VALID")).count()
        
        logger.info(f"Found {invalid_start_dates} records with NULL or invalid DATA_INICIO format")
        logger.info(f"Found {invalid_end_dates} records with NULL or invalid DATA_FIM format")
        
        # Show sample of invalid records if any exist
        if invalid_start_dates > 0:
            logger.warning("Sample of records with invalid DATA_INICIO:")
            df_validated.filter(~col("DATA_INICIO_VALID")).select("DATA_INICIO", "DATA_FIM", "CATEGORIA").show(5, False)

        if invalid_end_dates > 0:
            logger.warning("Sample of records with invalid DATA_FIM:")
            df_validated.filter(~col("DATA_FIM_VALID")).select("DATA_INICIO", "DATA_FIM", "CATEGORIA").show(5, False)
        
        # Extract date and time from timestamp and separate in two columns, handling null and invalid formats
        df_transformed = df_validated.withColumn(
            "HORA_INICIO", 
            when(col("DATA_INICIO_VALID"), col("DATA_INICIO").substr(12, 5))
            .otherwise(lit("00:00"))
        ).withColumn(
            "HORA_FIM", 
            when(col("DATA_FIM_VALID"), col("DATA_FIM").substr(12, 5))
            .otherwise(lit("00:00"))
        ).withColumn(
            "DATA_INICIO", 
            when(col("DATA_INICIO_VALID"), to_date(col("DATA_INICIO").substr(1, 10), "MM-dd-yyyy"))
            .otherwise(lit("1900-01-01").cast(DateType()))
        ).withColumn(
            "DATA_FIM", 
            when(col("DATA_FIM_VALID"), to_date(col("DATA_FIM").substr(1, 10), "MM-dd-yyyy"))
            .otherwise(lit("1900-01-01").cast(DateType()))
        )
        
        # Drop the validation columns
        df = df_transformed.drop("DATA_INICIO_VALID", "DATA_FIM_VALID")
        
        # Clean and transform data and scheme enforcement
        logger.info("\nCleaning and transforming data...")
        cleaned_df = df.select(
            col("DATA_INICIO").cast(DateType()),
            col("HORA_INICIO").cast(StringType()),
            col("DATA_FIM").cast(DateType()),
            col("HORA_FIM").cast(StringType()),
            when(col("CATEGORIA").isNull(), "Categoria não reclarada").otherwise(col("CATEGORIA")).cast(StringType()).alias("CATEGORIA"),
            when(col("LOCAL_INICIO").isNull(), "Local de inicio não declarado").otherwise(col("LOCAL_INICIO")).cast(StringType()).alias("LOCAL_INICIO"),
            when(col("LOCAL_FIM").isNull(), "Local final não declarado").otherwise(col("LOCAL_FIM")).cast(StringType()).alias("LOCAL_FIM"),        
            when(col("DISTANCIA").isNull(), 0).otherwise(col("DISTANCIA")).cast(IntegerType()).alias("DISTANCIA"),
            when(col("PROPOSITO").isNull(), "Propósito não declarado").otherwise(col("PROPOSITO")).cast(StringType()).alias("PROPOSITO")
        )
        
        cleaned_df.printSchema()

        # Show sample of cleaned data
        logger.info("\nSample of cleaned data:")
        cleaned_df.show(5, truncate=False)
        
        # Create database and table
        table_name = f"{LAKEHOUSE_CATALOG}.{SILVER_LAYER_DATABASE}.{SILVER_TRANSPORT_TABLE}"
        logger.info(f"Creating Iceberg table: {table_name}")
            
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {LAKEHOUSE_CATALOG}.{SILVER_LAYER_DATABASE}")

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