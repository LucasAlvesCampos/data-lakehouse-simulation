import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.config.config import *

def create_spark_session():
    """Create and configure Spark session"""
    conf = (
        pyspark.SparkConf()
            .setAppName('transport_data_processing')
            .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie:nessie-spark-extensions-3.5_2.12:0.67.0')
            .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
            .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
            .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
            .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
            .set('spark.sql.catalog.nessie.ref', 'main')
            .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
            .set('spark.sql.catalog.nessie.warehouse', f's3a://{WAREHOUSE}/iceberg-warehouse')
            .set('spark.sql.catalog.nessie.s3.endpoint', AWS_S3_ENDPOINT)
            .set('spark.hadoop.fs.s3a.endpoint', AWS_S3_ENDPOINT)
            .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
            .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
            .set('spark.hadoop.fs.s3a.path.style.access', 'true')
            .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
            .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    )
    
    return SparkSession.builder.config(conf=conf).getOrCreate()

def table_exists(spark, table_name):
    try:             
        # Check if table exists first
        tables = spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}.{table_name.split('.')[1]}")
        table_exists = tables.filter(col("tableName") == table_name.split('.')[-1]).count() > 0
        
        if not table_exists:
            logging.warning(f"Table {table_name} does not exist yet.")
            return False  # <-- Return False, not None
        else:
            logging.info(f"Table {table_name} exists.")
            return True
        
    except Exception as e:
         logging.warning(f"Error checking table existence: {str(e)}")
         import traceback
         logging.warning(traceback.format_exc())
         return False  # <-- Return False, not None


def get_last_processed_date(spark, table_name, date_column):
    """Get the last processed date from the specified table and column"""
    try:
        # Check if table exists first
        if not table_exists(spark, table_name):
            logging.warning(f"Table {table_name} does not exist. Cannot get last processed date.")
            return None
            
        # Get the maximum date from the specified table and column
        result = spark.sql(f"SELECT MAX({date_column}) as max_date FROM {table_name}")
        
        # Check if result is empty or null
        if result.count() == 0 or result.first()["max_date"] is None:
            logging.warning(f"No valid dates found in {table_name}.{date_column}")
            return None
            
        latest_date = result.first()["max_date"]
        logging.info(f"Latest processed date found: {latest_date}")
        return latest_date
        
    except Exception as e:
        logging.warning(f"Error getting last processed date: {str(e)}")
        import traceback
        logging.warning(traceback.format_exc())
        return None