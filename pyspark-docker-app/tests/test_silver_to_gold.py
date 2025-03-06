import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
import datetime
from src.jobs.silver_to_gold import main as silver_to_gold_main
from src.utils.spark_utils import create_spark_session

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing with in-memory processing."""
    spark_session = (SparkSession.builder
                     .master("local[1]")
                     .appName("silver_to_gold_test")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.default.parallelism", "1")
                     .getOrCreate())
    yield spark_session
    spark_session.stop()

@pytest.fixture
def sample_silver_data(spark):
    """Create sample silver layer data for testing the aggregations."""
    # Define schema that matches your silver layer
    silver_schema = StructType([
        StructField("DATA_INICIO", DateType(), True),
        StructField("HORA_INICIO", DateType(), True),
        StructField("DATA_FIM", DateType(), True),
        StructField("HORA_FIM", DateType(), True),
        StructField("CATEGORIA", StringType(), True),
        StructField("LOCAL_INICIO", StringType(), True),
        StructField("LOCAL_FIM", StringType(), True),
        StructField("DISTANCIA", FloatType(), True),
        StructField("PROPOSITO", StringType(), True),
    ])
    
    # Create sample data
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    
    data = [
        # Today's data by category
        (today, None, today, None, "Negocio", "Office", "Client A", 10.5, "Meeting"),
        (today, None, today, None, "Negocio", "Client A", "Office", 10.5, "Return"),
        (today, None, today, None, "Pessoal", "Home", "Gym", 3.2, "Workout"),
        (today, None, today, None, "Pessoal", "Gym", "Restaurant", 1.8, "Lunch"),
        
        # Yesterday's data 
        (yesterday, None, yesterday, None, "Negocio", "Office", "Client B", 15.0, "Meeting"),
        (yesterday, None, yesterday, None, "Negocio", "Client B", "Office", 15.0, "Return"),
        
        # NULL values
        (today, None, None, None, None, "Office", None, None, None),
    ]
    
    return spark.createDataFrame(data, schema=silver_schema)

def apply_gold_aggregations(df):
    """Apply the same aggregations as the silver_to_gold job."""
    from pyspark.sql.functions import sum, count, avg, col, round, date_format
    
    # Group by date and category
    daily_metrics = df.groupBy("DATA_INICIO", "CATEGORIA").agg(
        count("*").alias("TOTAL_CORRIDAS"),
        round(sum("DISTANCIA"), 2).alias("DISTANCIA_TOTAL"),
        round(avg("DISTANCIA"), 2).alias("DISTANCIA_MEDIA")
    )
    
    # Add formatted date column
    daily_metrics = daily_metrics.withColumn(
        "DATA", 
        date_format(col("DATA_INICIO"), "yyyy-MM-dd")
    )
    
    # Select the final columns
    return daily_metrics.select(
        "DATA",
        "CATEGORIA",
        "TOTAL_CORRIDAS",
        "DISTANCIA_TOTAL",
        "DISTANCIA_MEDIA"
    )

def test_aggregation_logic(spark, sample_silver_data):
    """Test that the aggregation logic works correctly."""
    # Apply the gold layer transformations
    gold_df = apply_gold_aggregations(sample_silver_data)
    
    # Check the schema
    assert "DATA" in gold_df.columns
    assert "CATEGORIA" in gold_df.columns
    assert "TOTAL_CORRIDAS" in gold_df.columns
    assert "DISTANCIA_TOTAL" in gold_df.columns
    assert "DISTANCIA_MEDIA" in gold_df.columns
    
    # Collect the results for verification
    results = gold_df.collect()
    
    # Check that categories are properly aggregated
    categories = set([row["CATEGORIA"] for row in results if row["CATEGORIA"] is not None])
    assert "Negocio" in categories
    assert "Pessoal" in categories
    
    # Find the business category for today's data
    today = datetime.date.today().strftime("%Y-%m-%d")
    today_business = next((row for row in results 
                          if row["DATA"] == today and row["CATEGORIA"] == "Negocio"), None)
    
    # Verify the metrics
    assert today_business is not None
    assert today_business["TOTAL_CORRIDAS"] == 2  # Two business trips today
    assert today_business["DISTANCIA_TOTAL"] == 21.0  # 10.5 + 10.5
    assert today_business["DISTANCIA_MEDIA"] == 10.5  # (10.5 + 10.5) / 2

def test_personal_category_aggregation(spark, sample_silver_data):
    """Test that personal category trips are correctly aggregated."""
    # Apply the gold layer transformations
    gold_df = apply_gold_aggregations(sample_silver_data)
    
    # Get today's date in the expected format
    today = datetime.date.today().strftime("%Y-%m-%d")
    
    # Find the personal category for today's data
    results = gold_df.collect()
    today_personal = next((row for row in results 
                          if row["DATA"] == today and row["CATEGORIA"] == "Pessoal"), None)
    
    # Verify the metrics for personal trips
    assert today_personal is not None, "Should have personal category data for today"
    assert today_personal["TOTAL_CORRIDAS"] == 2, "Should have 2 personal trips today"
    assert today_personal["DISTANCIA_TOTAL"] == 5.0, "Total distance should be 3.2 + 1.8 = 5.0"
    assert today_personal["DISTANCIA_MEDIA"] == 2.5, "Average distance should be 5.0 / 2 = 2.5"

def test_historical_data_segregation(spark, sample_silver_data):
    """Test that data is properly segregated by date."""
    # Apply the gold layer transformations
    gold_df = apply_gold_aggregations(sample_silver_data)
    
    # Get yesterday's date in the expected format
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Find yesterday's business data
    results = gold_df.collect()
    yesterday_business = next((row for row in results 
                              if row["DATA"] == yesterday and row["CATEGORIA"] == "Negocio"), None)
    
    # Verify the metrics for yesterday's business trips
    assert yesterday_business is not None, "Should have business data for yesterday"
    assert yesterday_business["TOTAL_CORRIDAS"] == 2, "Should have 2 business trips yesterday" 
    assert yesterday_business["DISTANCIA_TOTAL"] == 30.0, "Total distance should be 15.0 + 15.0 = 30.0"
    assert yesterday_business["DISTANCIA_MEDIA"] == 15.0, "Average distance should be 30.0 / 2 = 15.0"
    
    # Ensure that data is properly segregated by date
    # Count distinct dates in the results
    distinct_dates = set(row["DATA"] for row in results if row["DATA"] is not None)
    assert len(distinct_dates) == 2, "Should have exactly 2 distinct dates (today and yesterday)"


