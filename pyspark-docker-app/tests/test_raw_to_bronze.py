import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.utils.spark_utils import create_spark_session

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing with in-memory processing."""
    spark_session = (SparkSession.builder
                     .master("local[1]")
                     .appName("raw_to_bronze_test")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.default.parallelism", "1")
                     .getOrCreate())
    yield spark_session
    spark_session.stop()

@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file with sample data."""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False, mode='w') as f:
        # Write header
        f.write("DATA_INICIO,DATA_FIM,CATEGORIA,LOCAL_INICIO,LOCAL_FIM,DISTANCIA,PROPOSITO\n")
        
        # Write sample data rows
        f.write("01-15-2016 09:30,01-15-2016 10:45,Negocio,Office,Client Site,10.5,Meeting\n")
        f.write("02-20-2016 9:30,02-20-2016 12:45,Pessoal,Home,Park,5.2,Leisure\n")
        f.write("03-10-2016 14:30,,,,,\n")  # Row with missing values
        
    yield f.name
    
    # Clean up temp file
    os.unlink(f.name)

def test_csv_loading(spark, sample_csv_file):
    """Test that CSV file loads correctly with expected schema."""
    # Load the sample CSV
    df = spark.read.option("header", "true").csv(sample_csv_file)
    
    # Check if data was loaded
    assert df.count() == 3, "CSV file should have 3 rows"
    
    # Check schema contains all expected columns
    expected_columns = ["DATA_INICIO", "DATA_FIM", "CATEGORIA", "LOCAL_INICIO", 
                        "LOCAL_FIM", "DISTANCIA", "PROPOSITO"]
    actual_columns = df.columns
    
    for column in expected_columns:
        assert column in actual_columns, f"Column {column} not found in DataFrame"

def test_metadata_addition():
    """Test that metadata columns are added correctly."""
    # Create a mock DataFrame
    spark = SparkSession.builder.getOrCreate()
    schema = StructType([
        StructField("DATA_INICIO", StringType(), True),
        StructField("DATA_FIM", StringType(), True)
    ])
    mock_df = spark.createDataFrame([
        ("01-15-2016 09:30", "01-15-2016 10:45"),
        ("02-20-2016 9:30", "02-20-2016 12:45")
    ], schema)
    
    # Add metadata columns (this would be extracted from your raw_to_bronze code)
    from pyspark.sql.functions import current_timestamp, lit
    
    result_df = mock_df.withColumn("ingestion_time", current_timestamp())
    result_df = result_df.withColumn("source_file", lit("sample.csv"))
    
    # Verify metadata columns were added
    assert "ingestion_time" in result_df.columns, "ingestion_time column not found"
    assert "source_file" in result_df.columns, "source_file column not found"
    
    # Verify row count is preserved
    assert result_df.count() == mock_df.count(), "Row count should be preserved"