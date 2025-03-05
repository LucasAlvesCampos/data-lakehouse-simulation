import logging
from pyspark.sql.functions import *
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

        # Read and filter the cleansed data from Silver layer
        logger.info(f"Reading source table from Silver layer: {SILVER_LAYER_DATABASE}.{SILVER_TRANSPORT_TABLE}...")
        df = spark.table(f"{LAKEHOUSE_CATALOG}.{SILVER_LAYER_DATABASE}.{SILVER_TRANSPORT_TABLE}") \
            .filter(col("DATA_INICIO").isNotNull() & (col("DISTANCIA") > 0))

        # Log filtered records count
        total_records = df.count()
        logger.info(f"Number of valid records after filtering: {total_records}")

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

        # Create database and table
        table_name = f"{LAKEHOUSE_CATALOG}.{GOLD_LAYER_DATABASE}.{GOLD_TRANSPORT_TABLE}"
        logger.info(f"Creating Iceberg table: {table_name}")
            
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {LAKEHOUSE_CATALOG}.{GOLD_LAYER_DATABASE}")

        # Save results to new Iceberg table in Gold layer
        logger.info(f"\nSaving results to Gold layer: {GOLD_LAYER_DATABASE}.{GOLD_TRANSPORT_TABLE}...")
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