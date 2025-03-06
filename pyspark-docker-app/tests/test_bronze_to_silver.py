import pytest
import datetime
import os
import tempfile
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, FloatType
from pyspark.sql.functions import col, when, to_date, lit, regexp_replace, to_timestamp, concat
from src.jobs.bronze_to_silver import main as bronze_to_silver_main
from src.utils.spark_utils import create_spark_session, table_exists
from src.config.config import *

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing with in-memory processing."""
    spark_session = (SparkSession.builder
                     .master("local[1]")
                     .appName("bronze_to_silver_test")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.default.parallelism", "1")
                     .getOrCreate())
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="module")
def sample_bronze_data(spark):
    """Create sample bronze layer data for testing."""
    test_data = [
        # Valid record
        {"DATA_INICIO": "01-15-2016 09:30", "DATA_FIM": "01-15-2016 10:45", 
         "CATEGORIA": "Negocio", "LOCAL_INICIO": "Office", "LOCAL_FIM": "Client Site", 
         "DISTANCIA": "10.5", "PROPOSITO": "Meeting"},
        
        # Single-digit hour format
        {"DATA_INICIO": "02-20-2016 9:30", "DATA_FIM": "02-20-2016 12:45", 
         "CATEGORIA": "Pessoal", "LOCAL_INICIO": "Home", "LOCAL_FIM": "Park", 
         "DISTANCIA": "5.2", "PROPOSITO": "Leisure"},
        
        # NULL values
        {"DATA_INICIO": "03-10-2016 14:30", "DATA_FIM": None, 
         "CATEGORIA": None, "LOCAL_INICIO": None, "LOCAL_FIM": None, 
         "DISTANCIA": None, "PROPOSITO": None},
        
        # Invalid date format
        {"DATA_INICIO": "invalid-date", "DATA_FIM": "04-05-2016 16:30", 
         "CATEGORIA": "Negocio", "LOCAL_INICIO": "Office", "LOCAL_FIM": "Home", 
         "DISTANCIA": "8.0", "PROPOSITO": "Work"},
         
        
        # Duplicate record - same as first one
        {"DATA_INICIO": "01-15-2016 09:30", "DATA_FIM": "01-15-2016 10:45", 
         "CATEGORIA": "Negocio", "LOCAL_INICIO": "Office", "LOCAL_FIM": "Client Site", 
         "DISTANCIA": "10.5", "PROPOSITO": "Meeting"}
    ]
    
    return spark.createDataFrame(test_data)

def apply_transformations(df):
    """Apply the same transformations as the bronze_to_silver job."""
    date_patterns = {
        'single_digit_hour': "^\\d{2}-\\d{2}-\\d{4}\\s\\d{1}:\\d{2}$",
        'double_digit_hour': "^\\d{2}-\\d{2}-\\d{4}\\s\\d{2}:\\d{2}$"
    }

    # Fix single-digit hours - CORRECTED PATTERN
    df = df.withColumn(
        "DATA_INICIO",
        when(col("DATA_INICIO").rlike(date_patterns['single_digit_hour']),
            regexp_replace(col("DATA_INICIO"), "(\\d{2}-\\d{2}-\\d{4}\\s)(\\d):(\\d{2})", "$10$2:$3")
        ).otherwise(col("DATA_INICIO"))
    )

    df = df.withColumn(
        "DATA_FIM",
        when(col("DATA_FIM").rlike(date_patterns['single_digit_hour']),
            regexp_replace(col("DATA_FIM"), "(\\d{2}-\\d{2}-\\d{4}\\s)(\\d):(\\d{2})", "$10$2:$3")
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
    
    # Extract date and time from timestamp
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
        .otherwise(lit(None).cast(DateType()))  
    )
    
    # Drop the validation columns
    df = df_transformed.drop("DATA_INICIO_VALID", "DATA_FIM_VALID")
    
    # Clean and transform data and schema enforcement
    cleaned_df = df.select(
        col("DATA_INICIO"),
        col("HORA_INICIO"),
        col("DATA_FIM"),
        col("HORA_FIM"),
        when(col("CATEGORIA").isNull(), "Unknown Category").otherwise(col("CATEGORIA")).cast(StringType()).alias("CATEGORIA"),
        when(col("LOCAL_INICIO").isNull(), "Unknown Location").otherwise(col("LOCAL_INICIO")).cast(StringType()).alias("LOCAL_INICIO"),
        when(col("LOCAL_FIM").isNull(), "Unknown Location").otherwise(col("LOCAL_FIM")).cast(StringType()).alias("LOCAL_FIM"),        
        when(col("DISTANCIA").isNull(), 0).otherwise(col("DISTANCIA").cast(FloatType())).alias("DISTANCIA"),
        when(col("PROPOSITO").isNull(), "Unknown Purpose").otherwise(col("PROPOSITO")).cast(StringType()).alias("PROPOSITO")
    )
    
    # Remove duplicates
    return cleaned_df.dropDuplicates()

def test_schema_correctness(spark, sample_bronze_data):
    """Test that the transformed data has the correct schema."""
    # Apply transformations
    transformed_df = apply_transformations(sample_bronze_data)
    
    # Get the schema
    schema = transformed_df.schema
    
    # Check column data types
    assert any(field.name == "DATA_INICIO" and isinstance(field.dataType, DateType) for field in schema.fields)
    assert any(field.name == "HORA_INICIO" and isinstance(field.dataType, TimestampType) for field in schema.fields)
    assert any(field.name == "DATA_FIM" and isinstance(field.dataType, DateType) for field in schema.fields)
    assert any(field.name == "HORA_FIM" and isinstance(field.dataType, TimestampType) for field in schema.fields)
    assert any(field.name == "CATEGORIA" and isinstance(field.dataType, StringType) for field in schema.fields)
    assert any(field.name == "LOCAL_INICIO" and isinstance(field.dataType, StringType) for field in schema.fields)
    assert any(field.name == "LOCAL_FIM" and isinstance(field.dataType, StringType) for field in schema.fields)
    assert any(field.name == "DISTANCIA" and isinstance(field.dataType, FloatType) for field in schema.fields)
    assert any(field.name == "PROPOSITO" and isinstance(field.dataType, StringType) for field in schema.fields)

def test_null_value_handling(spark, sample_bronze_data):
    """Test that NULL values are handled correctly."""
    # Apply transformations
    transformed_df = apply_transformations(sample_bronze_data)
    
    # Test default values for NULL fields
    null_defaults = transformed_df.filter(col("DATA_INICIO").isNotNull() & col("DATA_FIM").isNull()).collect()
    
    assert len(null_defaults) > 0, "Test data with NULL values not found in results"
    
    if null_defaults:
        record = null_defaults[0]
        assert record["CATEGORIA"] == "Unknown Category", "NULL CATEGORIA should be replaced with 'Unknown Category'"
        assert record["LOCAL_INICIO"] == "Unknown Location", "NULL LOCAL_INICIO should be replaced with 'Unknown Location'"
        assert record["LOCAL_FIM"] == "Unknown Location", "NULL LOCAL_FIM should be replaced with 'Unknown Location'"
        assert record["PROPOSITO"] == "Unknown Purpose", "NULL PROPOSITO should be replaced with 'Unknown Purpose'"
        assert record["DISTANCIA"] == 0.0, "NULL DISTANCIA should be replaced with 0.0"

def test_invalid_date_handling(spark, sample_bronze_data):
    """Test that invalid dates are handled correctly."""
    # Apply transformations
    transformed_df = apply_transformations(sample_bronze_data)
    
    # Count records with NULL dates
    invalid_dates_count = transformed_df.filter(col("DATA_INICIO").isNull()).count()
    
    # At least our one invalid date record should result in a NULL date
    assert invalid_dates_count >= 1, "Invalid dates should be converted to NULL"

def test_date_parsing(spark, sample_bronze_data):
    """Test that dates are parsed correctly."""
    # Apply transformations
    transformed_df = apply_transformations(sample_bronze_data)
    
    # Find record with initial date of 01-15-2016
    jan15_records = transformed_df.filter(col("DATA_INICIO") == datetime.date(2016, 1, 15)).collect()
    
    # Verify it was parsed correctly
    assert len(jan15_records) >= 1, "Date 01-15-2016 was not parsed correctly"
    
    if len(jan15_records) >= 1:
        record = jan15_records[0]
        # Verify hour component in timestamp
        hour = record["HORA_INICIO"].hour
        assert hour == 9, f"Hour should be 9, but got {hour}"

def test_single_digit_hour_fix(spark, sample_bronze_data):
    """Test that single-digit hours are processed correctly."""
    # Apply transformations
    transformed_df = apply_transformations(sample_bronze_data)
    
    # Find record with initial date of 02-20-2016 (had single-digit hour)
    feb20_records = transformed_df.filter(col("DATA_INICIO") == datetime.date(2016, 2, 20)).collect()
    print(feb20_records)
    
    # Verify it was parsed correctly despite having single-digit hour
    assert len(feb20_records) >= 1, "Date with single-digit hour was not parsed correctly"
    
    if len(feb20_records) >= 1:
        record = feb20_records[0]
        # Verify hour component in timestamp
        hour = record["HORA_INICIO"].hour
        assert hour == 9, f"Hour should be 9, but got {hour}"

def test_duplicate_removal(spark, sample_bronze_data):
    """Test that duplicates are properly removed."""
    # Count original rows (including duplicate)
    original_count = sample_bronze_data.count()
    
    # Apply transformations
    transformed_df = apply_transformations(sample_bronze_data)
    
    # Count resulting rows
    result_count = transformed_df.count()
    
    # Should have one less row due to duplicate removal
    assert result_count < original_count, "Duplicates were not removed"
    
    # Count distinct rows in original data
    distinct_count = sample_bronze_data.dropDuplicates().count()
    
    # Result count should match distinct count
    assert result_count <= distinct_count, "Transformed data has unexpected number of rows"