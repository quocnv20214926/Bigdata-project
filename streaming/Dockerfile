FROM apache/spark:3.5.1-python3
USER root
WORKDIR /app
COPY common /app/common
COPY streaming /app/streaming
ENV PYTHONPATH=/app
RUN pip install --no-cache-dir redis==5.0.8
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "/app/streaming/main.py"]
