FROM bitnami/spark:3.5

USER root

# Install Python, pip and curl
RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Download Iceberg and Dependencies
RUN cd /opt/bitnami/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
    curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.178/bundle-2.17.178.jar && \
    curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.17.178/url-connection-client-2.17.178.jar && \
    curl -O https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions-3.5_2.12/0.67.0/nessie-spark-extensions-3.5_2.12-0.67.0.jar

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Install testing dependencies
RUN pip3 install pytest pytest-cov

# Copy the application code
COPY src/ /app/src/

# Copy test files
COPY tests/ /app/tests/

# Switch back to non-root user
USER 1001

# Command to run the application
CMD ["spark-submit", "/app/src/jobs/raw_to_bronze.py"]