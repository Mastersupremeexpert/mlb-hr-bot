FROM python:3.12-slim

WORKDIR /app

# System deps for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create runtime directories
RUN mkdir -p logs exports models/saved

# Railway injects PORT env var automatically
CMD ["sh", "-c", "python -m uvicorn dashboard.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
