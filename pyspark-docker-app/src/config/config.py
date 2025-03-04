import os

# Define sensitive variables
WAREHOUSE = os.environ.get("WAREHOUSE", "warehouse")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://minioserver:9000")
NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/api/v1")

# Define database and table names
BRONZE_LAYER_DATABASE = os.environ.get("BRONZE_LAYER_DATABASE", "bronze")
SILVER_LAYER_DATABASE = os.environ.get("SILVER_LAYER_DATABASE", "silver")
GOLD_LAYER_DATABASE = os.environ.get("GOLD_LAYER_DATABASE", "gold")
BRONZE_TRANSPORT_TABLE = os.environ.get("BRONZE_TRANSPORT_TABLE", "transport_data")
SILVER_TRANSPORT_TABLE = os.environ.get("SILVER_TRANSPORT_TABLE", "transport_data_cleansed")
GOLD_TRANSPORT_TABLE = os.environ.get("GOLD_TRANSPORT_TABLE", "info_corridas_do_dia")