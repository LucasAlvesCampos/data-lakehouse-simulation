import pytest
from src.jobs.raw_to_bronze import main as raw_to_bronze_main
from src.utils.spark_utils import create_spark_session

def test_raw_to_bronze():
    spark = create_spark_session()
    
    # Run the raw_to_bronze job
    raw_to_bronze_main()
    
    # Check if the table exists in the Bronze layer
    table_exists = spark.catalog.tableExists("nessie.bronze.transport_data")
    assert table_exists, "Table does not exist in the Bronze layer"
    
    # Check if the table has data
    df = spark.table("nessie.bronze.transport_data")
    assert df.count() > 0, "Table is empty in the Bronze layer"
    
    spark.stop()