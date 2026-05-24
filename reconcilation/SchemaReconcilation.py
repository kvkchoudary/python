from __future__ import annotations

from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Iterator
import requests
import logging
import argparse
import sys
from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -----------------------------
# Domain Models
# -----------------------------

@dataclass(frozen=True)
class OracleColumnMetadata:
    table_id: int
    table_name: str
    column_name: str
    column_type: str
    placeholder_1: str | None = None
    placeholder_2: str | None = None
    placeholder_3: str | None = None
    placeholder_4: str | None = None
    placeholder_5: str | None = None


@dataclass(frozen=True)
class HiveColumnMetadata:
    table_name: str
    column_name: str
    column_type: str


@dataclass
class SnapshotColumn:
    table_id: int
    table_name: str
    column_name: str
    column_type: str
    placeholder_1: str | None = None
    placeholder_2: str | None = None
    placeholder_3: str | None = None
    placeholder_4: str | None = None
    placeholder_5: str | None = None


@dataclass
class TableSnapshot:
    table_id: int
    table_name: str
    reason: str
    columns: list[SnapshotColumn]


# -----------------------------
# Normalization
# -----------------------------

def normalize_table_name(table_name: str) -> str:
    return table_name.strip().upper()


def normalize_column_name(column_name: str) -> str:
    return column_name.strip().upper()


def normalize_type(data_type: str) -> str:
    """
    Important place for business rules.

    Example:
    Oracle VARCHAR2(100) may need to match Hive STRING.
    Oracle NUMBER may need to match Hive DECIMAL.
    """
    if data_type is None:
        return ""

    t = data_type.strip().upper()
    t = t.replace(" ", "")

    type_mapping = {
        "VARCHAR2": "STRING",
        "VARCHAR": "STRING",
        "CHAR": "STRING",
        "CLOB": "STRING",
        "NUMBER": "DECIMAL",
        "INTEGER": "INT",
    }

    for oracle_type, hive_type in type_mapping.items():
        if t.startswith(oracle_type):
            return hive_type

    return t


def build_signature(columns: Iterable[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    """
    Sort columns by name if column order does not matter.
    If column order matters, remove sorted().
    """
    return tuple(sorted(
        (normalize_column_name(col), normalize_type(dtype))
        for col, dtype in columns
    ))


# -----------------------------
# Oracle REST Client
# -----------------------------

class OracleMetadataApiClient:

    def __init__(self, base_url: str, timeout_seconds: int = 60):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def stream_oracle_metadata(self) -> Iterator[OracleColumnMetadata]:
        """
        Assumption:
        API returns paginated JSON like:
        {
          "items": [...],
          "nextPageToken": "abc"
        }
        """
        next_page_token = None

        while True:
            params = {}
            if next_page_token:
                params["pageToken"] = next_page_token

            response = requests.get(
                f"{self.base_url}/oracle/metadata",
                params=params,
                timeout=self.timeout_seconds
            )
            response.raise_for_status()

            payload = response.json()

            for item in payload.get("items", []):
                yield OracleColumnMetadata(
                    table_id=item["table_id"],
                    table_name=item["table_name"],
                    column_name=item["column_name"],
                    column_type=item["column_type"],
                    placeholder_1=item.get("placeholder_1"),
                    placeholder_2=item.get("placeholder_2"),
                    placeholder_3=item.get("placeholder_3"),
                    placeholder_4=item.get("placeholder_4"),
                    placeholder_5=item.get("placeholder_5"),
                )

            next_page_token = payload.get("nextPageToken")
            if not next_page_token:
                break

    def stream_snapshots_to_oracle(self, snapshots: Iterable[TableSnapshot], batch_size: int = 500) -> None:
        batch = []

        for snapshot in snapshots:
            batch.append({
                "table_id": snapshot.table_id,
                "table_name": snapshot.table_name,
                "reason": snapshot.reason,
                "columns": [asdict(c) for c in snapshot.columns]
            })

            if len(batch) >= batch_size:
                self._send_snapshot_batch(batch)
                batch.clear()

        if batch:
            self._send_snapshot_batch(batch)

    def _send_snapshot_batch(self, batch: list[dict]) -> None:
        response = requests.post(
            f"{self.base_url}/oracle/snapshots",
            json={"snapshots": batch},
            timeout=self.timeout_seconds
        )
        response.raise_for_status()
        logger.info("Sent snapshot batch. size=%s", len(batch))


# -----------------------------
# Hive Metadata Reader
# -----------------------------

class HiveMetadataReader:

    def __init__(self, spark, max_workers: int = 10):
        self.spark = spark
        self.max_workers = max_workers

    def get_table_columns(self, database: str, table_name: str) -> list[HiveColumnMetadata]:
        columns = self.spark.catalog.listColumns(table_name, database)

        return [
            HiveColumnMetadata(
                table_name=table_name,
                column_name=c.name,
                column_type=c.dataType
            )
            for c in columns
        ]

    def load_hive_metadata(
        self,
        database: str,
        table_names: Iterable[str]
    ) -> dict[str, list[HiveColumnMetadata]]:

        table_names = list(table_names)
        hive_metadata: dict[str, list[HiveColumnMetadata]] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.get_table_columns, database, table): table
                for table in table_names
            }

            for future in as_completed(futures):
                table = futures[future]
                normalized_table = normalize_table_name(table)

                try:
                    hive_metadata[normalized_table] = future.result()
                except Exception as e:
                    logger.exception("Failed to fetch Hive metadata for table=%s", table)
                    #hive_metadata[normalized_table] = []
                    raise

        return hive_metadata


# -----------------------------
# Index Builders
# -----------------------------

def build_oracle_index(
    oracle_columns: Iterable[OracleColumnMetadata]
) -> tuple[
    dict[str, list[OracleColumnMetadata]],
    dict[str, tuple[tuple[str, str], ...]]
]:

    oracle_by_table: dict[str, list[OracleColumnMetadata]] = {}

    for col in oracle_columns:
        table_key = normalize_table_name(col.table_name)
        oracle_by_table.setdefault(table_key, []).append(col)

    oracle_signature_by_table = {
        table: build_signature((c.column_name, c.column_type) for c in cols)
        for table, cols in oracle_by_table.items()
    }

    return oracle_by_table, oracle_signature_by_table


def build_hive_signature_index(
    hive_by_table: dict[str, list[HiveColumnMetadata]]
) -> dict[str, tuple[tuple[str, str], ...]]:

    return {
        table: build_signature((c.column_name, c.column_type) for c in cols)
        for table, cols in hive_by_table.items()
    }


# -----------------------------
# Reconciliation Engine
# -----------------------------

class SchemaReconciler:

    def reconcile(
        self,
        oracle_by_table: dict[str, list[OracleColumnMetadata]],
        oracle_signature_by_table: dict[str, tuple[tuple[str, str], ...]],
        hive_by_table: dict[str, list[HiveColumnMetadata]],
        hive_signature_by_table: dict[str, tuple[tuple[str, str], ...]]
    ) -> list[TableSnapshot]:

        snapshots: list[TableSnapshot] = []

        for table_key, oracle_signature in oracle_signature_by_table.items():

            hive_signature = hive_signature_by_table.get(table_key)

            if hive_signature is None:
                snapshots.append(
                    self._create_missing_hive_snapshot(
                        table_key,
                        oracle_by_table[table_key]
                    )
                )
                continue

            if oracle_signature != hive_signature:
                snapshots.append(
                    self._create_hive_based_snapshot(
                        table_key,
                        oracle_by_table[table_key],
                        hive_by_table[table_key]
                    )
                )

        return snapshots

    def _create_hive_based_snapshot(
        self,
        table_key: str,
        oracle_columns: list[OracleColumnMetadata],
        hive_columns: list[HiveColumnMetadata]
    ) -> TableSnapshot:

        table_id = oracle_columns[0].table_id
        oracle_template = oracle_columns[0]

        snapshot_columns = [
            SnapshotColumn(
                table_id=table_id,
                table_name=table_key,
                column_name=hive_col.column_name,
                column_type=hive_col.column_type,
                placeholder_1=oracle_template.placeholder_1,
                placeholder_2=oracle_template.placeholder_2,
                placeholder_3=oracle_template.placeholder_3,
                placeholder_4=oracle_template.placeholder_4,
                placeholder_5=oracle_template.placeholder_5,
            )
            for hive_col in hive_columns
        ]

        return TableSnapshot(
            table_id=table_id,
            table_name=table_key,
            reason="SCHEMA_DIFFERENCE",
            columns=snapshot_columns
        )

    def _create_missing_hive_snapshot(
        self,
        table_key: str,
        oracle_columns: list[OracleColumnMetadata]
    ) -> TableSnapshot:

        table_id = oracle_columns[0].table_id

        snapshot_columns = [
            SnapshotColumn(
                table_id=c.table_id,
                table_name=c.table_name,
                column_name=c.column_name,
                column_type=c.column_type,
                placeholder_1=c.placeholder_1,
                placeholder_2=c.placeholder_2,
                placeholder_3=c.placeholder_3,
                placeholder_4=c.placeholder_4,
                placeholder_5=c.placeholder_5,
            )
            for c in oracle_columns
        ]

        return TableSnapshot(
            table_id=table_id,
            table_name=table_key,
            reason="TABLE_MISSING_IN_HIVE",
            columns=snapshot_columns
        )


# -----------------------------
# Main Orchestration
# -----------------------------

def run_reconciliation(
    spark,
    hive_database: str,
    oracle_api_base_url: str,
    hive_max_workers: int = 5
) -> None:

    api_client = OracleMetadataApiClient(oracle_api_base_url)

    logger.info("Loading Oracle metadata from REST API")
    oracle_columns = list(api_client.stream_oracle_metadata())

    logger.info("Oracle metadata columns loaded: %s", len(oracle_columns))

    oracle_by_table, oracle_signature_by_table = build_oracle_index(oracle_columns)

    #oracle_table_names = list(oracle_by_table.keys())
    oracle_table_names = [
    cols[0].table_name
    for cols in oracle_by_table.values()
    ]

    logger.info("Oracle tables to compare: %s", len(oracle_table_names))

    hive_reader = HiveMetadataReader(
        spark=spark,
        max_workers=hive_max_workers
    )

    logger.info("Loading Hive metadata using Spark Catalog API")
    hive_by_table = hive_reader.load_hive_metadata(
        database=hive_database,
        table_names=oracle_table_names
    )

    hive_signature_by_table = build_hive_signature_index(hive_by_table)

    logger.info("Reconciling Oracle schema with Hive schema")
    reconciler = SchemaReconciler()

    snapshots = reconciler.reconcile(
        oracle_by_table=oracle_by_table,
        oracle_signature_by_table=oracle_signature_by_table,
        hive_by_table=hive_by_table,
        hive_signature_by_table=hive_signature_by_table
    )

    logger.info("Snapshots to send: %s", len(snapshots))

    api_client.stream_snapshots_to_oracle(snapshots)

    logger.info("Schema reconciliation completed")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Hive vs Oracle schema reconciliation job"
    )

    parser.add_argument("--hive-database", required=True)
    parser.add_argument("--oracle-api-base-url", required=True)
    parser.add_argument("--hive-max-workers", type=int, default=10)
    parser.add_argument("--spark-app-name", default="HiveOracleSchemaReconciliation")

    return parser.parse_args()


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )


def main():
    args = parse_args()

    spark = None

    try:
        logger.info("Starting Hive Oracle schema reconciliation job")

        spark = create_spark_session(args.spark_app_name)

        run_reconciliation(
            spark=spark,
            hive_database=args.hive_database,
            oracle_api_base_url=args.oracle_api_base_url,
            hive_max_workers=args.hive_max_workers
        )

        logger.info("Job completed successfully")
        return 0

    except Exception:
        logger.exception("Job failed")
        return 1

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    sys.exit(main())