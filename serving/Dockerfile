# Recommendation API image. Build context = repository root (see infra/compose.stream.yml).
FROM python:3.13-slim
WORKDIR /app
COPY serving/requirements.txt serving/requirements.txt
RUN pip install --no-cache-dir -r serving/requirements.txt
COPY common common
COPY serving serving
EXPOSE 8000
CMD ["uvicorn", "serving.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
