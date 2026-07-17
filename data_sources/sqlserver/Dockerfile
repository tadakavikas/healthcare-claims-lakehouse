FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pymssql
COPY init_databases.py feed_databases.py shared_ids.py .
