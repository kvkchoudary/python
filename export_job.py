"""Configuration-driven HDFS/Spark-SQL export to S3.

Operational boundaries:
* Runs as a Spark driver on an edge node; AutoSys/launcher are external.
* Uses Hive metastore configured in the Spark deployment.
* Uses Hadoop S3A for S3 access, with credentials injected from secret files.
* V1 supports one active writer per target; no concurrent writes.
* Uses run-specific staging and a stable Hive table name when registration is enabled.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

import oracledb
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

LOG = logging.getLogger("spark_export")
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ALLOWED_FORMATS = {"PARQUET", "AVRO", "CSV", "JSON"}
ALLOWED_MODES = {"APPEND", "OVERWRITE"}


class JobError(Exception):
    """Non-retryable job failure."""


class AccessDenied(JobError):
    """FID does not have permission for the configured destination."""


@dataclasses.dataclass(frozen=True)
class ExportConfig:
    config_id: int
    consumer_name: str
    hierarchy_path: str
    data_type: str
    source_hdfs_path: Optional[str]
    spark_sql: Optional[str]
    fid: str
    target_s3_path: str
    output_format: str
    write_mode: str
    register_hive_table: bool
    hive_database: Optional[str]
    hive_table: Optional[str]
    schema_json: Optional[str]
    partition_columns: tuple[str, ...]

    @property
    def source_kind(self) -> str:
        return "COPY" if self.source_hdfs_path else "SQL"


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise JobError(f"Missing required environment variable: {name}")
    return value


def read_secret_file(env_name: str) -> str:
    """Avoid placing secrets in source, config DB, or command-line arguments."""
    path = Path(required_env(env_name))
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise JobError(f"Empty secret file configured by {env_name}")
    return value


def retry_transient(operation: Callable, attempts: int = 3, base_delay: float = 1.0):
    """Retry only exceptions explicitly identified by the caller as transient."""
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except (requests.Timeout, requests.ConnectionError, oracledb.OperationalError) as exc:
            if attempt == attempts:
                raise JobError(f"Transient operation failed after {attempts} attempts") from exc
            delay = base_delay * (2 ** (attempt - 1))
            LOG.warning("Transient failure attempt=%s/%s retry_in=%ss", attempt, attempts, delay)
            time.sleep(delay)


def load_config(consumer: str, hierarchy: str, data_type: str) -> ExportConfig:
    """Use bound variables to prevent SQL injection in configuration lookup."""
    dsn = required_env("ORACLE_DSN")
    username = required_env("ORACLE_USER")
    password = read_secret_file("ORACLE_PASSWORD_FILE")
    sql = """SELECT CONFIG_ID, CONSUMER_NAME, HIERARCHY_PATH, DATA_TYPE,
                    SOURCE_HDFS_PATH, SPARK_SQL, FID, TARGET_S3_PATH,
                    OUTPUT_FORMAT, WRITE_MODE, REGISTER_HIVE_TABLE,
                    HIVE_DATABASE, HIVE_TABLE, SCHEMA_JSON, PARTITION_COLUMNS
             FROM SPARK_EXPORT_CONFIG
             WHERE CONSUMER_NAME=:consumer AND HIERARCHY_PATH=:hierarchy
               AND DATA_TYPE=:data_type AND IS_ACTIVE='Y'"""

    def query():
        with oracledb.connect(user=username, password=password, dsn=dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, consumer=consumer, hierarchy=hierarchy, data_type=data_type)
                rows = cursor.fetchmany(2)
                if len(rows) != 1:
                    raise JobError(f"Expected one active config, found {len(rows)}")
                row = [v.read() if hasattr(v, "read") else v for v in rows[0]]
                return ExportConfig(
                    config_id=row[0], consumer_name=row[1], hierarchy_path=row[2],
                    data_type=row[3], source_hdfs_path=row[4], spark_sql=row[5],
                    fid=row[6], target_s3_path=row[7], output_format=row[8],
                    write_mode=row[9], register_hive_table=(row[10] == "Y"),
                    hive_database=row[11], hive_table=row[12], schema_json=row[13],
                    partition_columns=tuple(x.strip() for x in (row[14] or "").split(",") if x.strip())
                )
    return retry_transient(query)


def validate_config(c: ExportConfig) -> None:
    """Reject ambiguous, unsafe, or unsupported configuration early."""
    if bool(c.source_hdfs_path) == bool(c.spark_sql):
        raise JobError("Exactly one of SOURCE_HDFS_PATH or SPARK_SQL must be populated")
    if c.output_format not in ALLOWED_FORMATS or c.write_mode not in ALLOWED_MODES:
        raise JobError("Unsupported output format or write mode")
    u = urlparse(c.target_s3_path)
    if u.scheme != "s3a" or not u.netloc or not u.path.strip("/"):
        raise JobError("TARGET_S3_PATH must be s3a://bucket/nonempty-prefix")
    if c.source_hdfs_path and urlparse(c.source_hdfs_path).scheme != "hdfs":
        raise JobError("Copy source must be an hdfs:// path")
    if c.spark_sql:
        # Guardrail only, NOT a complete SQL security boundary. See README.
        s = c.spark_sql.strip().rstrip(";").strip()
        if not re.match(r"^(SELECT|WITH)\b", s, re.I) or ";" in s:
            raise JobError("Only single SELECT/WITH SQL statements are supported")
    if c.register_hive_table:
        if not c.hive_database or not c.hive_table:
            raise JobError("Hive database/table required for registration")
        for name in (c.hive_database, c.hive_table):
            if not IDENT.fullmatch(name):
                raise JobError("Unsafe Hive identifier")
    if c.source_kind == "COPY" and c.partition_columns:
        raise JobError("PARTITION_COLUMNS is only supported for SQL output in V1")
    if c.register_hive_table and c.output_format == "JSON":
        raise JobError("JSON Hive registration unsupported without an approved SerDe")
    if c.register_hive_table and c.output_format == "CSV":
        raise JobError("CSV Hive registration deferred: quoted/newline-safe SerDe required")


def authorize_fid(c: ExportConfig) -> None:
    """Fail closed on timeout, malformed response, or explicit denial."""
    url = required_env("AUTH_API_URL")
    token = read_secret_file("AUTH_API_TOKEN_FILE")
    def invoke():
        response = requests.post(url, json={"fid": c.fid, "s3Path": c.target_s3_path},
                                 headers={"Authorization": f"Bearer {token}"}, timeout=(5, 20))
        if response.status_code in (401, 403):
            raise AccessDenied("Authorization API rejected identity")
        if response.status_code in (429, 500, 502, 503, 504):
            raise requests.ConnectionError("Authorization service transient error")
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise JobError("Invalid authorization API response") from exc
        if payload.get("authorized") is not True:
            raise AccessDenied("FID not authorized for destination")
    retry_transient(invoke)


def create_spark() -> SparkSession:
    """Hive support requires compatible Spark/Hive configuration at deployment."""
    return SparkSession.builder.appName("ConfigurationDrivenS3Export").enableHiveSupport().getOrCreate()


def configure_s3a(spark: SparkSession) -> None:
    """Use secret files; do not log credential values."""
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hconf.set("fs.s3a.access.key", read_secret_file("AWS_ACCESS_KEY_FILE"))
    hconf.set("fs.s3a.secret.key", read_secret_file("AWS_SECRET_KEY_FILE"))
    hconf.set("fs.s3a.endpoint.region", required_env("AWS_REGION"))
    # For distributed SQL writers, executors must receive secure credentials too.
    # Cluster-specific credential propagation must be validated before production use.


def hadoop_fs(spark: SparkSession, uri: str):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs, path


def exists(spark: SparkSession, uri: str) -> bool:
    fs, p = hadoop_fs(spark, uri)
    return bool(fs.exists(p))


def join_uri(base: str, suffix: str) -> str:
    return base.rstrip("/") + "/" + suffix.lstrip("/")


def copy_hdfs_tree(spark: SparkSession, source: str, destination: str) -> None:
    """Copy raw bytes, preserving relative file paths. No format conversion."""
    jvm = spark.sparkContext._jvm
    src_fs, src = hadoop_fs(spark, source)
    dst_fs, dst = hadoop_fs(spark, destination)
    if not src_fs.exists(src):
        raise JobError("HDFS source does not exist")
    if dst_fs.exists(dst):
        raise JobError("Staging destination already exists")
    if not dst_fs.mkdirs(dst):
        raise JobError("Unable to create staging destination")

    def copy_entry(path, relative: str):
        status = src_fs.getFileStatus(path)
        if status.isDirectory():
            for child in src_fs.listStatus(path):
                child_name = str(child.getPath().getName())
                # Skip Hadoop/Spark marker files; include data files and partition folders.
                if child_name.startswith("_") or child_name.startswith("."):
                    continue
                copy_entry(child.getPath(), join_uri(relative, child_name) if relative else child_name)
        else:
            target = jvm.org.apache.hadoop.fs.Path(join_uri(destination, relative))
            dst_fs.mkdirs(target.getParent())
            # Hadoop FileUtil copies bytes across filesystems without Spark deserialization.
            ok = jvm.org.apache.hadoop.fs.FileUtil.copy(src_fs, path, dst_fs, target,
                                                        False, spark.sparkContext._jsc.hadoopConfiguration())
            if not ok:
                raise JobError(f"Copy failed for relative path: {relative}")

    if src_fs.getFileStatus(src).isFile():
        copy_entry(src, str(src.getName()))
    else:
        copy_entry(src, "")


def write_spark_sql(spark: SparkSession, c: ExportConfig, staging: str):
    """Run SQL and write its DataFrame to a run-specific staging prefix."""
    df = spark.sql(c.spark_sql)
    writer = df.write.mode("errorifexists").format(c.output_format.lower())
    if c.output_format == "CSV":
        writer = writer.option("header", "true")
    if c.partition_columns:
        missing = set(c.partition_columns) - set(df.columns)
        if missing:
            raise JobError(f"Missing partition columns: {sorted(missing)}")
        writer = writer.partitionBy(*c.partition_columns)
    writer.save(staging)
    return df.schema


def infer_schema(spark: SparkSession, c: ExportConfig, source: str) -> StructType:
    """Use explicit schema for text data; binary formats carry schema metadata."""
    if c.schema_json:
        return StructType.fromJson(json.loads(c.schema_json))
    if c.output_format in ("CSV", "JSON"):
        raise JobError("SCHEMA_JSON required for CSV/JSON Hive registration")
    return spark.read.format(c.output_format.lower()).load(source).schema


def validate_copy_format(c: ExportConfig, spark: SparkSession) -> None:
    """Inspect readable binary formats; CSV/JSON need operational source validation."""
    if c.output_format in ("PARQUET", "AVRO"):
        spark.read.format(c.output_format.lower()).load(c.source_hdfs_path).schema
    # Text-file content and mixed formats cannot be reliably validated from extension alone.


def sql_type(field) -> str:
    return field.dataType.simpleString()


def hive_ddl(spark: SparkSession, c: ExportConfig, location: str, schema: StructType) -> str:
    """Generate an external Hive table DDL; never concatenate unvalidated identifiers."""
    if not schema.fields:
        raise JobError("Cannot register empty schema")
    partition_set = set(c.partition_columns)
    columns = [f"`{f.name}` {sql_type(f)}" for f in schema.fields if f.name not in partition_set]
    partitions = [f"`{f.name}` {sql_type(f)}" for f in schema.fields if f.name in partition_set]
    for field in schema.fields:
        if not IDENT.fullmatch(field.name):
            raise JobError("Unsupported Hive column name")
    if c.output_format == "PARQUET":
        storage = "STORED AS PARQUET"
    elif c.output_format == "AVRO":
        storage = "STORED AS AVRO"
    elif c.output_format == "CSV":
        storage = "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
    else:
        # JSON Hive SerDe is not universally installed; fail rather than create a broken table.
        raise JobError("JSON Hive registration requires an approved JSON SerDe; unsupported in V1")
    part_clause = f" PARTITIONED BY ({', '.join(partitions)})" if partitions else ""
    return (f"CREATE EXTERNAL TABLE `{c.hive_database}`.`{c.hive_table}` "
            f"({', '.join(columns)}){part_clause} {storage} LOCATION '{location}'")


def register_hive(spark: SparkSession, c: ExportConfig, location: str, schema: StructType) -> None:
    """V1 registers new tables or verifies an existing table points to the same location.

    Existing-table schema/partition reconciliation is deliberately NOT automated.
    """
    table = f"`{c.hive_database}`.`{c.hive_table}`"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{c.hive_database}`")
    if spark.catalog.tableExists(f"{c.hive_database}.{c.hive_table}"):
        existing = spark.table(f"{c.hive_database}.{c.hive_table}").schema
        if existing != schema:
            raise JobError("Existing Hive table schema differs; manual migration required")
        # The location must be checked separately to avoid silently pointing at stale data.
        rows = spark.sql(f"DESCRIBE FORMATTED {table}").collect()
        current = next((str(r[1]).strip() for r in rows if str(r[0]).strip() == "Location"), None)
        if current != location:
            raise JobError("Existing Hive table location differs; explicit publication update required")
        return
    spark.sql(hive_ddl(spark, c, location, schema))
    if c.partition_columns:
        spark.sql(f"MSCK REPAIR TABLE {table}")


def publish_run(spark: SparkSession, c: ExportConfig, staging: str, run_id: str) -> str:
    """Publish files by copy, never assume S3 rename is atomic.

    V1: overwrite replaces live prefix, append adds run-namespaced files.
    Publication is NOT transactional. See README for production limitations.
    """
    target = c.target_s3_path.rstrip("/")
    fs, stage = hadoop_fs(spark, staging)
    _, live = hadoop_fs(spark, target)
    if not fs.exists(stage):
        raise JobError("Missing staging output")
    if c.write_mode == "OVERWRITE" and fs.exists(live):
        if not fs.delete(live, True):
            raise JobError("Unable to remove existing destination")
    if not fs.exists(live):
        fs.mkdirs(live)
    # Publish recursively, retaining partition directory structure. For append,
    # prefix filenames with the stable run ID to prevent collisions.
    # This is NOT an atomic multi-object commit (documented V1 limitation).
    jvm = spark.sparkContext._jvm
    def publish_entry(path, relative):
        status = fs.getFileStatus(path)
        if status.isDirectory():
            for child in fs.listStatus(path):
                name = str(child.getPath().getName())
                if name.startswith("_") or name.startswith("."):
                    continue
                publish_entry(child.getPath(), f"{relative}/{name}" if relative else name)
        else:
            parts = relative.rsplit("/", 1)
            filename = parts[-1]
            parent = parts[0] + "/" if len(parts) == 2 else ""
            if c.write_mode == "APPEND":
                filename = f"{run_id}_{filename}"
            dest = jvm.org.apache.hadoop.fs.Path(join_uri(target, parent + filename))
            fs.mkdirs(dest.getParent())
            if fs.exists(dest):
                raise JobError("Output object already exists; refusing to duplicate run")
            if not jvm.org.apache.hadoop.fs.FileUtil.copy(fs, path, fs, dest, False,
                                                           spark.sparkContext._jsc.hadoopConfiguration()):
                raise JobError(f"Publish failed for {relative}")
    publish_entry(stage, "")
    return target


def run_job(args) -> None:
    run_id = args.run_id or uuid.uuid4().hex
    c = load_config(args.consumer, args.hierarchy, args.data_type)
    validate_config(c)
    LOG.info("Starting run=%s config_id=%s source=%s mode=%s", run_id, c.config_id, c.source_kind, c.write_mode)
    authorize_fid(c)
    spark = create_spark()
    try:
        configure_s3a(spark)
        staging = join_uri(c.target_s3_path.rstrip("/") + "__staging", run_id)
        if exists(spark, staging):
            raise JobError("Staging run already exists; investigate before retry")
        if c.source_kind == "COPY":
            validate_copy_format(c, spark)
            copy_hdfs_tree(spark, c.source_hdfs_path, staging)
            schema = infer_schema(spark, c, c.source_hdfs_path) if c.register_hive_table else None
        else:
            schema = write_spark_sql(spark, c, staging)
        LOG.info("Staging complete run=%s", run_id)
        published = publish_run(spark, c, staging, run_id)
        if c.register_hive_table:
            register_hive(spark, c, published, schema)
        LOG.info("SUCCESS run=%s destination=%s", run_id, published)
        # Staging is retained for troubleshooting; clean via controlled lifecycle policy.
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Configuration-driven PySpark export")
    parser.add_argument("--consumer", required=True)
    parser.add_argument("--hierarchy", required=True)
    parser.add_argument("--data-type", required=True)
    parser.add_argument("--run-id", help="Stable scheduler execution ID for traceability")
    args = parser.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        run_job(args)
        return 0
    except AccessDenied as exc:
        LOG.error("ACCESS_DENIED: %s", exc)
        return 3
    except Exception:
        LOG.exception("EXPORT_FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
