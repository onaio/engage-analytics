# Combined dbt + dataexport image for scheduled jobs
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN pip install --no-cache-dir uv

# Copy dbt project
COPY dbt/ /app/dbt/

# Install dbt dependencies
WORKDIR /app/dbt
RUN uv pip install --system dbt-core dbt-postgres
RUN uv run dbt deps --profiles-dir .

# Copy dataexport project
COPY dataexport/ /app/dataexport/

# Install dataexport dependencies
WORKDIR /app/dataexport
RUN uv pip install --system .

# Create non-root user
RUN useradd --create-home --shell /bin/bash app \
    && mkdir -p /app/dataexport/exports \
    && chown -R app:app /app

USER app

WORKDIR /app

# Default command runs dbt then exports to S3
CMD ["sh", "-c", "cd /app/dbt && uv run dbt run --profiles-dir . && cd /app/dataexport && uv run python export_to_s3.py --type both --delete-local"]
