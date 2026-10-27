FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/app/src:/app
ENV DBT_PROFILES_DIR=/app/dbt
ENV WAREHOUSE_TARGET=duckdb
ENV DUCKDB_PATH=/app/data/student_journey.duckdb

CMD ["bash", "-c", "make all && (uvicorn src.api:app --host 0.0.0.0 --port 8130 &) && streamlit run src/dashboard.py --server.port 8501 --server.address 0.0.0.0"]
