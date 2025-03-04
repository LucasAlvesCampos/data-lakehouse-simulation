import pytest
from src.jobs.bronze_to_silver import main as bronze_to_silver_main
from src.utils.spark_utils import create_spark_session

def test_bronze_to_silver():
    spark = create_spark_session()
    
    # Run the bronze_to_silver job
    bronze_to_silver_main()
    
    # Check if the table exists in the Silver layer
    table_exists = spark.catalog.tableExists("nessie.silver.transport_data_cleansed")
    assert table_exists, "Table does not exist in the Silver layer"
    
    # Check if the table has data
    df = spark.table("nessie.silver.transport_data_cleansed")
    assert df.count() > 0, "Table is empty in the Silver layer"
    
    spark.stop()