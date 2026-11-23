FROM python:3.13-slim
WORKDIR /app
COPY ui/requirements.txt ui/requirements.txt
RUN pip install --no-cache-dir -r ui/requirements.txt
# common/ holds the Kafka message contract (Event.to_json) and the Redis key names.
COPY common common
COPY ui ui
COPY docs/*.json docs/
COPY docs/loadtest docs/loadtest
ENV PYTHONPATH=/app
EXPOSE 8501
CMD ["streamlit", "run", "ui/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--browser.gatherUsageStats=false"]
