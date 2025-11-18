# Match our 3.11 environment
FROM python:3.11-slim

#Sets the working directory in docker container to "app"
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

#Copy Python Packages from requirements.txt
COPY data-pipeline/requirements.txt .


# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Copy application code - MAINTAIN ORIGINAL STRUCTURE
COPY data-pipeline/src/ ./src 
COPY data-pipeline/config/ ./config/
COPY .env .


# Create necessary directories
RUN mkdir -p data/raw data/processed logs reports prefect-storage


# Set Python path to find our modules
ENV PYTHONPATH=/app/src

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser


# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"


# Default command
CMD ["python", "src/orchestration/prefect_flows.py"]

