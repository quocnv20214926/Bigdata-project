"""SparkSession factory for the batch layer.

Runs locally (`local[*]`, default) or on the cluster from infra/compose.batch.yml
(SPARK_MASTER_URL=spark://spark-master:7077). MinIO is reached through s3a:// with
the settings in common/config.py; the Hadoop AWS jars are downloaded on first use.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from common.config import settings

# Must match the Hadoop version bundled with PySpark / Spark 3.5 (3.3.4).
S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def get_spark(app_name: str, master: str | None = None, driver_memory: str | None = None,
              s3: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master or os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.driver.memory", driver_memory or os.getenv("SPARK_DRIVER_MEMORY", "8g"))
        .config("spark.sql.session.timeZone", "UTC")
        # Re-running one month only replaces that month's event_date partitions.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Shuffle files are deleted only after the driver garbage-collects their references;
        # the default 30 min lets ALS iterations pile up tens of GB on disk.
        .config("spark.cleaner.periodicGC.interval", "1min")
    )
    if os.getenv("SPARK_IVY_DIR"):
        builder = builder.config("spark.jars.ivy", os.environ["SPARK_IVY_DIR"])
    if s3:
        builder = (
            builder
            .config("spark.jars.packages", S3A_PACKAGES)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.s3_endpoint.startswith("https")).lower())
        )
    return builder.getOrCreate()
