# Prometheus exporter of the speed layer. Build context = repository root.
FROM python:3.13-slim
WORKDIR /app
COPY streaming/monitoring/requirements.txt streaming/monitoring/requirements.txt
RUN pip install --no-cache-dir -r streaming/monitoring/requirements.txt
COPY streaming/__init__.py streaming/__init__.py
COPY streaming/monitoring streaming/monitoring
EXPOSE 9108
CMD ["python", "-m", "streaming.monitoring.exporter"]
