import pytest
from src.jobs.silver_to_gold import main as silver_to_gold_main
from src.utils.spark_utils import create_spark_session

def test_silver_to_gold():
    spark = create_spark_session()
    
    # Run the silver_to_gold job
    silver_to_gold_main()
    
    # Check if the table exists in the Gold layer
    table_exists = spark.catalog.tableExists("nessie.gold.info_corridas_do_dia")
    assert table_exists, "Table does not exist in the Gold layer"
    
    # Check if the table has data
    df = spark.table("nessie.gold.info_corridas_do_dia")
    assert df.count() > 0, "Table is empty in the Gold layer"
    
    spark.stop()