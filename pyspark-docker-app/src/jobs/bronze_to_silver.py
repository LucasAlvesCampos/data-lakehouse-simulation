import logging
import re
from pyspark.sql.functions import col, when, to_date, regexp_extract, length, lit, regexp_replace, to_timestamp, concat, sum, count
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from src.utils.spark_utils import create_spark_session
from src.config.config import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

"""
Silver Layer Schema Documentation:

Date/Time Columns:
- DATA_INICIO (DateType): Start date, NULL if invalid
- HORA_INICIO (TimestampType): Start timestamp, NULL if invalid
- DATA_FIM (DateType): End date, NULL if invalid
- HORA_FIM (TimestampType): End timestamp, NULL if invalid

Categorical Columns:
- CATEGORIA (StringType): Category, "Unknown Category" if NULL
- LOCAL_INICIO (StringType): Start location, "Unknown Location" if NULL
- LOCAL_FIM (StringType): End location, "Unknown Location" if NULL
- PROPOSITO (StringType): Purpose, "Unknown Purpose" if NULL

Numeric Columns:
- DISTANCIA (FloatType): Distance, 0 if NULL

Data Quality Rules:
1. Invalid dates/times are set to NULL for data quality tracking
2. Categorical fields use meaningful default values
3. Numeric fields default to 0 when NULL
4. No duplicate records allowed
"""

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

        date_patterns = {
            'single_digit_hour': "^\\d{2}-\\d{2}-\\d{4}\\s\\d{1}:\\d{2}$",
            'double_digit_hour': "^\\d{2}-\\d{2}-\\d{4}\\s\\d{2}:\\d{2}$"
        }

        # Check for single-digit hours in timestamp and fix the format
        df = df.withColumn(
            "DATA_INICIO",
            when(
                col("DATA_INICIO").rlike(date_patterns['single_digit_hour']),
                regexp_replace(
                    col("DATA_INICIO"),
                    "(\\d{2}-\\d{2}-\\d{4}\\s)(\\d):(\\d{2})",
                    "$100:$3"
                )
            ).otherwise(col("DATA_INICIO"))
        )

        df = df.withColumn(
            "DATA_FIM",
            when(
                col("DATA_FIM").rlike(date_patterns['single_digit_hour']),
                regexp_replace(
                    col("DATA_FIM"),
                    "(\\d{2}-\\d{2}-\\d{4}\\s)(\\d):(\\d{2})",
                    "$100:$3"
                )
            ).otherwise(col("DATA_FIM"))
        )

        # Add validation columns
        df_validated = df.withColumn(
            "DATA_INICIO_VALID", 
            when(col("DATA_INICIO").isNull(), lit(False))
            .when(col("DATA_INICIO").rlike(date_patterns['double_digit_hour']), lit(True))
            .otherwise(lit(False))
        ).withColumn(
            "DATA_FIM_VALID", 
            when(col("DATA_FIM").isNull(), lit(False))
            .when(col("DATA_FIM").rlike(date_patterns["double_digit_hour"]), lit(True))
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
            when(col("DATA_INICIO_VALID"), 
                to_timestamp(concat(col("DATA_INICIO").substr(1, 10), lit(" "), 
                                col("DATA_INICIO").substr(12, 5)), "MM-dd-yyyy HH:mm"))
            .otherwise(lit(None).cast(TimestampType()))  
        ).withColumn(
            "HORA_FIM", 
            when(col("DATA_FIM_VALID"), 
                to_timestamp(concat(col("DATA_FIM").substr(1, 10), lit(" "), 
                                col("DATA_FIM").substr(12, 5)), "MM-dd-yyyy HH:mm"))
            .otherwise(lit(None).cast(TimestampType()))  
        ).withColumn(
            "DATA_INICIO", 
            when(col("DATA_INICIO_VALID"), 
                 to_date(col("DATA_INICIO").substr(1, 10), "MM-dd-yyyy"))
            .otherwise(lit(None).cast(DateType()))  
        ).withColumn(
            "DATA_FIM", 
            when(col("DATA_FIM_VALID"), 
                 to_date(col("DATA_FIM").substr(1, 10), "MM-dd-yyyy"))
            .otherwise(lit(None).cast(DateType()))  # Use NULL instead of 1900-01-01
        )
        
        # Drop the validation columns
        df = df_transformed.drop("DATA_INICIO_VALID", "DATA_FIM_VALID")
        
        # Clean and transform data and schema enforcement
        logger.info("\nCleaning and transforming data...")
        cleaned_df = df.select(
            col("DATA_INICIO"),
            col("HORA_INICIO"),
            col("DATA_FIM"),
            col("HORA_FIM"),
            when(col("CATEGORIA").isNull(), "Unknown Category").otherwise(col("CATEGORIA")).cast(StringType()).alias("CATEGORIA"),
            when(col("LOCAL_INICIO").isNull(), "Unknown Location").otherwise(col("LOCAL_INICIO")).cast(StringType()).alias("LOCAL_INICIO"),
            when(col("LOCAL_FIM").isNull(), "Unknown Location").otherwise(col("LOCAL_FIM")).cast(StringType()).alias("LOCAL_FIM"),        
            when(col("DISTANCIA").isNull(), 0).otherwise(col("DISTANCIA")).cast(FloatType()).alias("DISTANCIA"),
            when(col("PROPOSITO").isNull(), "Unknown Purpose").otherwise(col("PROPOSITO")).cast(StringType()).alias("PROPOSITO")
        )
        
        # Add after cleaned_df creation
        data_quality_metrics = cleaned_df.select(
            # Start date/time metrics
            sum(col("DATA_INICIO").isNull().cast("int")).alias("null_data_inicio_count"),
            sum(col("HORA_INICIO").isNull().cast("int")).alias("null_hora_inicio_count"),
            
            # End date/time metrics
            sum(col("DATA_FIM").isNull().cast("int")).alias("null_data_fim_count"),
            sum(col("HORA_FIM").isNull().cast("int")).alias("null_hora_fim_count"),
            
            # Other metrics
            sum(col("DISTANCIA").isNull().cast("int")).alias("null_distance_count"),
            count("*").alias("total_records")
        ).collect()[0]

        logger.info(f"""
        Data Quality Metrics:
        Total Records: {data_quality_metrics.total_records}

        Start Date/Time Nulls:
        - DATA_INICIO: {data_quality_metrics.null_data_inicio_count}
        - HORA_INICIO: {data_quality_metrics.null_hora_inicio_count}

        End Date/Time Nulls:
        - DATA_FIM: {data_quality_metrics.null_data_fim_count}
        - HORA_FIM: {data_quality_metrics.null_hora_fim_count}

        Other Metrics:
        - Null Distances: {data_quality_metrics.null_distance_count}
        """)

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