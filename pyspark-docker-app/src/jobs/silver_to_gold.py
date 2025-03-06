import logging
from pyspark.sql.functions import *
from src.utils.spark_utils import create_spark_session, get_last_processed_date, table_exists
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

        # Define Gold table name
        table_name = f"{LAKEHOUSE_CATALOG}.{GOLD_LAYER_DATABASE}.{GOLD_TRANSPORT_TABLE}"

        # Get the latest processed date from the Gold table
        latest_processed_date = get_last_processed_date(spark, table_name, "DT_REFE")
        logger.info(f"Latest processed date: {latest_processed_date}")

        # Read and filter the cleansed data from Silver layer
        logger.info(f"Reading source table from Silver layer: {SILVER_LAYER_DATABASE}.{SILVER_TRANSPORT_TABLE}...")
        df = spark.table(f"{LAKEHOUSE_CATALOG}.{SILVER_LAYER_DATABASE}.{SILVER_TRANSPORT_TABLE}")

        if latest_processed_date:
            df = df.filter((col("DATA_INICIO") > latest_processed_date) & col("DATA_INICIO").isNotNull() & (col("DISTANCIA") > 0))
        else:
            df = df.filter(col("DATA_INICIO").isNotNull() & (col("DISTANCIA") > 0))

        # Log filtered records count
        total_records = df.count()
        logger.info(f"Number of valid records after filtering: {total_records}")

        if total_records == 0:
            logger.info("No new data to process.")
            return

        # Create daily aggregations
        daily_stats = df.groupBy(col("DATA_INICIO").alias("DT_REFE")) \
            .agg(
                count("*").alias("QT_CORR"),
                count(when(col("CATEGORIA") == "Negocio", True)).alias("QT_CORR_NEG"),
                count(when(col("CATEGORIA") == "Pessoal", True)).alias("QT_CORR_PESS"),
                max("DISTANCIA").alias("VL_MAX_DIST"),
                min("DISTANCIA").alias("VL_MIN_DIST"),
                round(avg("DISTANCIA"), 2).alias("VL_AVG_DIST"),
                count(when(col("PROPOSITO") == "Reunião", True)).alias("QT_CORR_REUNI"),
                count(when((col("PROPOSITO") != "Unknown Purpose") & 
                      (col("PROPOSITO") != "Reunião"), True)).alias("QT_CORR_NAO_REUNI")
            ) \
            .orderBy("DT_REFE")

        # Show sample of results
        logger.info("\nSample of daily statistics:")
        daily_stats.show(5, truncate=False)

        # Create database and table if not exists
        logger.info(f"Creating Iceberg table: {table_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {LAKEHOUSE_CATALOG}.{GOLD_LAYER_DATABASE}")

        # Save new data to Gold table in append mode
        logger.info(f"\nSaving results to Gold layer: {GOLD_LAYER_DATABASE}.{GOLD_TRANSPORT_TABLE}...")

        if table_exists(spark, table_name):
            logger.info("Table already exists. Appending new data...")
            daily_stats.writeTo(table_name) \
                .using("iceberg") \
                .append()
        else:
            # Create new table
            logger.info("Table does not exist. Creating new table...")
            daily_stats.writeTo(table_name) \
                .using("iceberg") \
                .createOrReplace()
            
        logger.info("Daily statistics calculation completed successfully!")

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