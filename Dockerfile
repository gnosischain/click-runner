FROM python:3.11-slim

# Install build tools for compiling lz4/snappy if needed for clickhouse-connect
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY run_queries.py .

# Copy the queries folder
COPY queries queries

ENTRYPOINT ["python", "run_queries.py"]
CMD ["--help"]
