FROM python:3.11-slim

WORKDIR /app

# System deps for psycopg2 and numpy/xgboost build tools
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# Create runtime directories
RUN mkdir -p logs exports models/saved data

# Railway injects PORT env var automatically
CMD sh -c "python -m uvicorn dashboard.app:app --host 0.0.0.0 --port ${PORT:-8000}"
