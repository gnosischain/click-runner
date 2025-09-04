FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY run_queries.py .
COPY __init__.py .
COPY ingestors/ ./ingestors/
COPY utils/ ./utils/

# Create directory for queries
COPY queries/ ./queries/

# Set execute permissions
RUN chmod +x run_queries.py

ENTRYPOINT ["python", "run_queries.py"]