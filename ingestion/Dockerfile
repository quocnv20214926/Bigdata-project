# Replayer image. Build context = repository root (see infra/compose.stream.yml).
FROM python:3.13-slim
WORKDIR /app
COPY ingestion/requirements.txt ingestion/requirements.txt
RUN pip install --no-cache-dir -r ingestion/requirements.txt
COPY common common
COPY ingestion ingestion
ENTRYPOINT ["python", "-m", "ingestion.replayer"]
