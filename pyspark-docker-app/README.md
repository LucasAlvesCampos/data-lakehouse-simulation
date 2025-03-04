# PySpark Data Lakehouse Application

## Overview

This PySpark application implements a modern data lakehouse architecture using Apache Spark, Iceberg, Nessie, and MinIO. The project demonstrates end-to-end data processing using the medallion architecture pattern (raw → bronze → silver → gold).

## Architecture Components

- **Apache Spark**: Distributed data processing engine
- **Apache Iceberg**: Table format for large analytic datasets
- **Project Nessie**: Git-like versioning for data lakes
- **MinIO**: S3-compatible object storage
- **Dremio**: Data lakehouse platform and SQL query engine

## Project Structure

```
pyspark-docker-app/
├── Dockerfile              # Docker container configuration
├── requirements.txt        # Python dependencies
├── src/
│   ├── config/
│   │   └── config.py      # Configuration and environment variables
│   ├── data/
│   │   └── info_transportes.csv  # Sample transport data
│   ├── jobs/
│   │   ├── data_pipeline.py      # Pipeline orchestrator
│   │   ├── raw_to_bronze.py      # Raw ingestion job
│   │   ├── bronze_to_silver.py   # Data cleansing job
│   │   └── silver_to_gold.py     # Aggregation job
│   └── utils/
│       └── spark_utils.py        # Spark session utilities
└── tests/                       # Unit tests for each job
```

## Data Flow

1. **Raw to Bronze**: Initial data ingestion
   - Reads CSV files
   - Preserves original data
   - Adds metadata columns

2. **Bronze to Silver**: Data cleansing
   - Date/time parsing
   - Data type conversions
   - Null handling
   - Basic validations

3. **Silver to Gold**: Business metrics
   - Daily aggregations
   - Category-wise statistics
   - Performance indicators

## Configuration

### Environment Variables

```env
# Nessie Configuration
NESSIE_URI=http://nessie:19120/api/v1

# MinIO Configuration
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_S3_ENDPOINT=http://minioserver:9000

# Data Location
WAREHOUSE=warehouse

# Layer Configuration
LAKEHOUSE_CATALOG=nessie
BRONZE_LAYER_DATABASE=bronze
SILVER_LAYER_DATABASE=silver
GOLD_LAYER_DATABASE=gold

# Table Names
BRONZE_TRANSPORT_TABLE=transport_data
SILVER_TRANSPORT_TABLE=transport_data_cleansed
GOLD_TRANSPORT_TABLE=info_corridas_do_dia
```

## Running the Application

### Prerequisites

- Docker and Docker Compose
- At least 8GB RAM available

### Quick Start

1. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configurations
```

2. Run the application:
```bash
# Start all services
docker-compose up -d

# Run the complete pipeline
docker-compose run pyspark-app spark-submit /app/src/jobs/data_pipeline.py
```

### Running Individual Jobs

```bash
# Raw to Bronze
docker-compose run pyspark-app spark-submit /app/src/jobs/raw_to_bronze.py

# Bronze to Silver
docker-compose run pyspark-app spark-submit /app/src/jobs/bronze_to_silver.py

# Silver to Gold
docker-compose run pyspark-app spark-submit /app/src/jobs/silver_to_gold.py
```

## Testing

Run the test suite:

```bash
docker-compose run pyspark-app pytest
```

## Service Access Points

- Spark UI: http://localhost:8080
- MinIO Console: http://localhost:9001
- Nessie API: http://localhost:19120/api/v1
- Dremio UI: http://localhost:9047

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify all services are running: `docker-compose ps`
   - Check service logs: `docker-compose logs [service-name]`

2. **Data Not Found**
   - Verify MinIO credentials in `.env`
   - Check if data files exist in the correct S3 path

3. **Pipeline Failures**
   - Check job logs for detailed error messages
   - Verify data quality and schema compatibility

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License.