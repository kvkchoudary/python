# Configuration-driven PySpark export — V1 reference implementation

## Scope
Oracle lookup by consumer/hierarchy/data type; FID + S3 path authorization; HDFS file byte-copy or Spark SQL export; Parquet/Avro/CSV/JSON output for SQL; S3A authentication using secret files; APPEND/OVERWRITE; optional Hive external-table registration; logging and bounded retry of transient Oracle/API errors.

**Status: reference implementation, not production-ready.** In particular, publication of multiple S3 objects is **not atomic**, and overwrite deletes the previous live prefix before copying the replacement. Consumers may observe incomplete data. Do not deploy for production writes until the publication protocol is hardened. Also, a job failure after partial publication cannot safely be retried automatically; an operator must reconcile the target/staging objects. A stable run ID alone does not provide exactly-once semantics.

## Configuration
Run `01_oracle_schema.sql` as a suitably privileged Oracle user. Oracle 19c+ assumed. The table allows one record per triple, whether active or inactive. `SOURCE_HDFS_PATH` and `SPARK_SQL` are mutually exclusive (validated by Python; CLOB constraints vary by Oracle version). `OUTPUT_FORMAT` on COPY is an assertion of source format; the files are not converted.

Required environment variables (do not print or log secrets):

- `ORACLE_DSN`, `ORACLE_USER`, `ORACLE_PASSWORD_FILE`
- `AUTH_API_URL`, `AUTH_API_TOKEN_FILE`
- `AWS_ACCESS_KEY_FILE`, `AWS_SECRET_KEY_FILE`, `AWS_REGION`

Each `*_FILE` points to a securely mounted, permission-restricted file containing one secret. Avoid passing secrets in CLI flags, job definitions, or Oracle rows. If the deployment supports IAM roles or short-lived credentials, prefer those in the next revision.

## Submit example

```bash
spark-submit \
  --master yarn --deploy-mode client \
  --conf spark.sql.catalogImplementation=hive \
  export_job.py \
  --consumer consumer_a \
  --hierarchy /finance/daily \
  --data-type transactions \
  --run-id autosys-execution-123
```

The cluster must supply compatible `hadoop-aws`, AWS SDK, and `spark-avro` dependencies, Hive Metastore connectivity, network access to S3, and secure credential propagation to executors. These depend on the actual Spark/Hadoop distribution. Test `spark.read.parquet('s3a://...')` and a small write before deploying.

## FID authorization contract (assumed)

POST `AUTH_API_URL` with JSON `{ "fid": "...", "s3Path": "s3a://bucket/prefix" }` and bearer token. Success requires HTTP 2xx and JSON `{ "authorized": true }`. 401/403 and `authorized:false` fail closed. 429/5xx/timeouts are retried with bounded backoff. Confirm the actual API request and response with the owning team.

## Hive registration

Uses Spark `enableHiveSupport()` and Hive Metastore. Supports newly created **Parquet and Avro** external tables. SQL result schema comes from Spark; COPY schema from file metadata. For an existing table, the code checks schema and location and refuses incompatible changes. Partitioned SQL output calls `MSCK REPAIR TABLE` after registration. CSV/JSON output is supported for **storage only**, not Hive registration in V1; production Hive CSV/JSON needs agreed SerDe, schema, and escaping rules. `SCHEMA_JSON` uses Spark `StructType.json()` representation, not a JSON Schema document.

Hive metastore must be able to resolve the S3 URI, and querying Spark processes need S3 read permissions. No direct SQL connection to Hive Metastore's backing relational database is needed.

## V1 restrictions / required hardening

1. **Non-atomic S3 publication:** staged data is copied into the live prefix; overwrite removes old data first. Use versioned immutable prefixes plus a controlled metadata-pointer switch or manifest-based publication before production. For Hive external tables, updating table/partition locations can be a controlled publish step, but multi-table and multi-partition switches are not atomic.
2. **Retry safety:** only Oracle connectivity and authorization-service transient errors are retried. Spark writes and publication are not blindly retried. Persist run IDs and audit status, detect partial writes, and implement explicit recovery before enabling AutoSys job-level retries.
3. **Concurrent writers:** enforce one writer per target through scheduler or distributed lock; not implemented in V1.
4. **Authorization scope:** FID API is business authorization; S3 IAM is separate. API must authorize the exact destination and operation, including subprefixes and overwrite if needed.
5. **Configured SQL:** the SELECT/WITH check is a convenience guard, not a SQL parser or security sandbox. Execute only approved SQL under a least-privilege Spark/Hive identity; review SQL before activation.
6. **Copy validation:** Parquet/Avro schema is inspected; CSV/JSON bytes are copied without deep validation. Ensure source format, schema, partition layout and file naming are governed externally.
7. **Metadata correctness:** table partitioning and schema evolution require more extensive validation; `MSCK REPAIR` may not handle all partition layouts or operational scale.
8. **Output format:** Spark writes directories of `part-*` files, not one file. File count and compression should be configurable later. For large datasets, avoid `coalesce(1)`.
9. **Audit:** currently logging only. Add a persistent execution/audit table with state transitions, metrics, and recovery operations.
10. **S3 credentials:** `SimpleAWSCredentialsProvider` is for the explicitly selected static key/secret design. Confirm secure propagation to distributed executors; never expose Hadoop config in logs or UI.
11. **Lifecycle:** staging prefixes are intentionally retained for diagnosis; add controlled cleanup and retention policies.
12. **Configuration edits:** add change approval/versioning and target prefix ownership checks before production.

## Suggested next iteration

Implement immutable run output prefixes, an Oracle job execution ledger, exclusive destination locking, commit/rollback/reconciliation, catalog metadata publication, and integration tests on a representative Spark/Hadoop/Hive cluster. These are prerequisites for safe append/overwrite with retries.
