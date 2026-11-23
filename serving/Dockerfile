# Recommendation API image. Build context = repository root (see infra/compose.stream.yml).
FROM python:3.13-slim
WORKDIR /app
COPY serving/requirements.txt serving/requirements.txt
RUN pip install --no-cache-dir -r serving/requirements.txt
COPY common common
COPY serving serving
# Two worker processes share their Prometheus counters through files in this
# folder (serving/app/metrics.py); it is emptied at every start.
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus
EXPOSE 8000
CMD ["sh", "-c", "rm -rf \"$PROMETHEUS_MULTIPROC_DIR\" && mkdir -p \"$PROMETHEUS_MULTIPROC_DIR\" && exec uvicorn serving.app.main:app --host 0.0.0.0 --port 8000 --workers 2"]
