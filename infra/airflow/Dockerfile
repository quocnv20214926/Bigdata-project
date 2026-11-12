# Airflow for the nightly batch (Pair 1, week 5): Airflow plus what batch/nightly.py needs to run
# Spark in local mode inside this container (Java 17, PySpark, numpy, pandas, pyarrow, redis).
# Built from the repository root by infra/compose.batch.yml; the code itself is mounted, not copied.
FROM apache/airflow:2.10.5-python3.11

USER root
# The JRE's post-install step expects the man page folder, which the slim base image lacks.
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
COPY batch/requirements.txt /tmp/requirements-batch.txt
# pytest: batch/tests/test_dag.py runs inside this container.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pytest -r /tmp/requirements-batch.txt

# Fetch the s3a jars once at build time (same versions as S3A_PACKAGES in batch/spark_session.py),
# so the tasks do not download 280 MB on every run.
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262').getOrCreate().stop()"
