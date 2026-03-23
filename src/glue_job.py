"""
AWS Glue ETL job -- processes hit-level data at scale using Spark.

Medallion layout (see ``src/s3_data_layers.py``):
  * **Landing** (``--input_path``): raw hit-level **Parquet** (default) or legacy TSV
  * **Staging** (optional ``--partitioned_hits_path``): partitioned Parquet hits (silver)
  * **Curated** (``--output_path``): aggregated keyword performance Parquet (gold)

Reads a tab-separated file from S3, attributes search-engine revenue
to keywords using running last-touch attribution, and writes the
aggregated results back to S3.

Attribution model (matches the core SearchKeywordAnalyzer):
    For each purchase, find the most recent search-engine referrer
    for that visitor *prior to or at* the purchase hit.  This correctly
    handles visitors who search multiple keywords across multiple purchases.

Glue job parameters:
    --input_path   s3://bucket/input/  (base prefix; see partition_* below)
    --output_path  s3://bucket/output/
    --partition_dt          optional YYYY-MM-DD — narrows read to .../dt=<dt>/ (partition pruning vs full bucket)
    --partition_hour        optional HH — requires partition_dt; narrows to .../hour=<HH>/
    --partition_minute      optional — requires partition_hour; must match a bucket for --partition_interval_minutes
    --partition_interval_minutes  default 15 — must match Lambda env PARTITION_INTERVAL_MINUTES
    --landing_format        optional parquet|tsv — default parquet; tsv = tab-separated with header
    --partitioned_hits_path optional — raw hits as Parquet, partitionBy(dt,hour,minute)
    --enable_large_job_optimizations  optional true|false — Spark AQE + shuffle tuning
    --shuffle_partitions   optional (default 200) — used when large-job optimizations on
    --curated_output_partitions  optional (default 1) — Parquet files for gold output (>1 for huge aggregates)
    --visitor_repartition_partitions  optional (default 0) — repartition by visitor_id before windows if >0
    --staging_repartition_partitions  optional (default 0) — repartition before silver write if >0
    --s3_recursive_list  optional (default true) — if false, S3 CSV read uses recurse=false (faster listing
        when all files are direct children of ``resolved_input``; do not use if you have hour=/minute= subfolders)

Reads via Glue DynamicFrame (CSV) so listing is scoped to the resolved path. Curated Parquet write
defaults to a single file (``--curated_output_partitions 1``); increase for large aggregate outputs.
Optional ``--enable_large_job_optimizations`` tunes shuffle / AQE; ``--visitor_repartition_partitions``
repartitions by ``visitor_id`` before window functions (helps big shuffles / skew tuning).

When ``--partitioned_hits_path`` is set, the staging branch and the main pipeline both consume
``base`` — the job persists ``base`` once to avoid scanning/parsing the landing data twice.
"""

import re
import sys
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

# Keep aligned with ``partition_time.DEFAULT_PARTITION_INTERVAL_MINUTES`` (Glue script is standalone on S3).
PARTITION_INTERVAL_DEFAULT = 15

_PG_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_pg_identifier(name: str, label: str) -> str:
    if not name or not _PG_IDENT.fullmatch(name):
        raise ValueError(f"Invalid {label} identifier {name!r} (use letters, numbers, underscore; no spaces).")
    return name


# NOTE: SEARCH_ENGINE_QUERY_PARAMS is hardcoded for this demonstration to ensure
# the Glue script stays self-contained when uploaded to S3 (no import from src/).
#
# Production scaling strategy:
# In a high-volume environment (1,000+ engines), move this mapping to an external
# metadata store (e.g., AWS AppConfig or S3-backed JSON) for updates without redeploy.
SEARCH_ENGINE_QUERY_PARAMS = {
    "google": ["q"],
    "bing": ["q"],
    "yahoo": ["p"],
    "msn": ["q"],
    "ask": ["q", "ask"],
    "aol": ["q", "query"],
    "duckduckgo": ["q"],
    "baidu": ["wd", "word"],
    "yandex": ["text"],
}


def _optional_arg(name: str, default: str = "") -> str:
    """
    Read ``--name <value>`` from argv. If the token after ``--name`` is another flag
    (starts with ``--``), treat the value as missing — Glue/Airflow often emit adjacent
    ``--partition_hour`` ``--partition_minute`` when an optional value is empty, and
    naive parsing would otherwise swallow ``--partition_hour`` as ``partition_minute``.
    """
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            nxt = sys.argv[idx + 1]
            if not nxt.startswith("--"):
                return nxt
    return default


def _valid_minute_buckets(interval_minutes: int) -> set[str]:
    """Aligned with ``partition_time.valid_minute_bucket_strings`` (no import — Glue script is standalone)."""
    if interval_minutes < 1 or interval_minutes > 60:
        raise ValueError(f"partition_interval_minutes must be 1–60; got {interval_minutes!r}")
    return {str(m).zfill(2) for m in range(0, 60, interval_minutes)}


def _resolved_input_path(
    base: str,
    partition_dt: str,
    partition_hour: str,
    partition_minute: str,
    partition_interval_minutes: int,
) -> str:
    """
    Narrow S3 read to Hive-style prefixes to avoid listing the entire bucket.

    If partition_dt is empty, returns base (full recursive scan — dev / legacy).
    """
    base = base.rstrip("/")
    pd = (partition_dt or "").strip()
    if not pd:
        return base
    out = f"{base}/dt={pd}"
    ph = (partition_hour or "").strip()
    if ph:
        out += f"/hour={ph.zfill(2)}"
        pm = (partition_minute or "").strip()
        if pm:
            mb = pm.zfill(2)
            allowed = _valid_minute_buckets(partition_interval_minutes)
            if mb not in allowed:
                raise ValueError(
                    f"partition_minute must be one of {sorted(allowed)} "
                    f"(interval={partition_interval_minutes}m); got {mb!r}"
                )
            out += f"/minute={mb}"
    return out


@F.udf(returnType=StructType([
    StructField("engine_domain", StringType()),
    StructField("keyword", StringType()),
]))
def parse_search_referrer(referrer):
    """UDF: extract (engine_domain, keyword) from a search-engine referrer URL."""
    if not referrer:
        return None
    try:
        parsed = urlparse(referrer)
    except ValueError:
        return None

    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return None

    engine_name = None
    for engine in SEARCH_ENGINE_QUERY_PARAMS:
        if engine in hostname:
            engine_name = engine
            break
    if engine_name is None:
        return None

    parts = hostname.split(".")
    engine_domain = ".".join(parts[-2:]) if len(parts) >= 2 else hostname

    query_params = parse_qs(parsed.query)
    for param in SEARCH_ENGINE_QUERY_PARAMS[engine_name]:
        values = query_params.get(param)
        if values:
            keyword = values[0].strip().lower()
            if keyword:
                return (engine_domain, keyword)
    return None


@F.udf(returnType=StringType())
def parse_product_revenue(product_list):
    """UDF: sum revenue from all products in the product_list field."""
    if not product_list:
        return "0.0"
    total = 0.0
    for product in product_list.split(","):
        fields = product.split(";")
        if len(fields) >= 4 and fields[3].strip():
            try:
                total += float(fields[3].strip())
            except ValueError:
                pass
    return str(total)


def _apply_spark_large_dataset_conf(
    spark,
    *,
    enable: bool,
    shuffle_partitions: int,
) -> None:
    """Tune Spark SQL for heavier shuffles (Glue 4 / Spark 3.x)."""
    if not enable:
        return
    spark.conf.set("spark.sql.shuffle.partitions", str(max(1, shuffle_partitions)))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])
    sync_db_sinks = _optional_arg("sync_db_sinks", "false").lower() == "true"
    db_host = _optional_arg("db_host")
    db_port = int(_optional_arg("db_port", "5432"))
    db_name = _optional_arg("db_name")
    db_secret_arn = _optional_arg("db_secret_arn")
    db_fact_table = _optional_arg("db_fact_table", "fact_keyword_performance")
    db_ai_table = _optional_arg("db_ai_table", "ai_keyword_insights")
    partitioned_hits_path = _optional_arg("partitioned_hits_path", "").strip()
    partition_dt = _optional_arg("partition_dt", "")
    partition_hour = _optional_arg("partition_hour", "")
    partition_minute = _optional_arg("partition_minute", "")
    partition_interval_minutes = int(
        _optional_arg("partition_interval_minutes", str(PARTITION_INTERVAL_DEFAULT))
    )
    if partition_interval_minutes < 1 or partition_interval_minutes > 60:
        raise ValueError(
            f"partition_interval_minutes must be 1–60; got {partition_interval_minutes!r}"
        )

    enable_large_job_optimizations = (
        _optional_arg("enable_large_job_optimizations", "false").lower() == "true"
    )
    shuffle_partitions = int(_optional_arg("shuffle_partitions", "200"))
    curated_output_partitions = int(_optional_arg("curated_output_partitions", "1"))
    visitor_repartition_partitions = int(_optional_arg("visitor_repartition_partitions", "0"))
    staging_repartition_partitions = int(_optional_arg("staging_repartition_partitions", "0"))

    if curated_output_partitions < 1:
        raise ValueError("curated_output_partitions must be >= 1")
    if shuffle_partitions < 1:
        raise ValueError("shuffle_partitions must be >= 1")
    if visitor_repartition_partitions < 0:
        raise ValueError("visitor_repartition_partitions must be >= 0")
    if staging_repartition_partitions < 0:
        raise ValueError("staging_repartition_partitions must be >= 0")

    s3_recursive_list = _optional_arg("s3_recursive_list", "true").lower() in ("true", "1", "yes")
    landing_format = _optional_arg("landing_format", "parquet").lower().strip()
    if landing_format not in ("parquet", "tsv"):
        raise ValueError(f"landing_format must be 'parquet' or 'tsv'; got {landing_format!r}")

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    # hit_time_gmt is Unix seconds UTC — keep session TZ UTC for dt/hour/minute.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    _apply_spark_large_dataset_conf(
        spark,
        enable=enable_large_job_optimizations,
        shuffle_partitions=shuffle_partitions,
    )
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    resolved_input = _resolved_input_path(
        args["input_path"],
        partition_dt,
        partition_hour,
        partition_minute,
        partition_interval_minutes,
    )

    def _normalize_columns(spark_df):
        """Lowercase column names so Parquet/TSV paths match the Spark pipeline."""
        out = spark_df
        for c in spark_df.columns:
            lc = c.lower()
            if c != lc:
                out = out.withColumnRenamed(c, lc)
        return out

    def _require_hit_columns(spark_df):
        need = {
            "hit_time_gmt",
            "ip",
            "user_agent",
            "referrer",
            "event_list",
            "product_list",
        }
        have = set(spark_df.columns)
        missing = sorted(need - have)
        if missing:
            raise ValueError(
                f"Landing data missing columns {missing}. Found: {sorted(have)}"
            )

    if landing_format == "parquet":
        # Recursive read of *.parquet under resolved_input (Hive-style prefixes).
        # pathGlobFilter avoids failing when a non-Parquet file (e.g. legacy .tsv) exists in the same folder.
        df = (
            spark.read.option("pathGlobFilter", "*.parquet")
            .option("recursiveFileLookup", "true")
            .parquet(resolved_input)
        )
    else:
        # Glue DynamicFrame: path pruning uses `resolved_input` only. `recurse` controls S3 listing depth
        # false = non-recursive list (flat folders only; faster when there are no subfolders).
        dyf = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [resolved_input],
                "recurse": s3_recursive_list,
            },
            format="csv",
            format_options={
                "withHeader": True,
                "separator": "\t",
            },
        )
        df = dyf.toDF()

    df = _normalize_columns(df)
    _require_hit_columns(df)
    # Parquet may infer event_list/product_list as numeric; window/UDF logic expects strings.
    for c in ("event_list", "product_list", "referrer", "user_agent", "ip"):
        df = df.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit("")))

    base = (
        df.withColumn(
            "hit_ts",
            # Ensure malformed/non-numeric timestamps don't become NULL,
            # which would make window ordering unstable. Parquet may store
            # hit_time_gmt as long; CSV as string — normalize via string first.
            F.when(
                F.col("hit_time_gmt").cast("string").rlike(r"^\d+$"),
                F.col("hit_time_gmt").cast("long"),
            ).otherwise(F.lit(0).cast("long")),
        )
        .withColumn(
            "event_ts",
            F.to_timestamp(F.from_unixtime(F.col("hit_ts")), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("dt", F.date_format(F.col("event_ts"), "yyyy-MM-dd"))
        .withColumn("hour", F.date_format(F.col("event_ts"), "HH"))
        .withColumn(
            "minute",
            F.lpad(
                (
                    (
                        F.floor(F.minute(F.col("event_ts")) / F.lit(float(partition_interval_minutes)))
                        * F.lit(float(partition_interval_minutes))
                    )
                    .cast("int")
                ).cast("string"),
                2,
                "0",
            ),
        )
    )

    # Staging + main path both need `base`; persist once to avoid double CSV scan when staging is on.
    base_persisted = False
    try:
        if partitioned_hits_path:
            base = base.persist(StorageLevel.MEMORY_AND_DISK)
            base_persisted = True
            staging_out = base.drop("event_ts")
            if staging_repartition_partitions > 0:
                # Align shuffle keys with Hive partition columns (matches partitionBy order) for fewer shuffles vs hash-only.
                staging_out = staging_out.repartition(
                    staging_repartition_partitions, "dt", "hour", "minute"
                )
            staging_out.write.mode("overwrite").partitionBy("dt", "hour", "minute").parquet(
                partitioned_hits_path
            )

        hits = (
            base.withColumn("visitor_id", F.concat_ws("|", F.col("ip"), F.col("user_agent")))
            .withColumn("search_info", parse_search_referrer(F.col("referrer")))
        )

        if visitor_repartition_partitions > 0:
            hits = hits.repartition(visitor_repartition_partitions, "visitor_id")

        # ── Running last-touch attribution ──
        # For each hit, carry forward the most recent search referrer per visitor.
        # We use last() with ignorenulls over a window ordered by hit timestamp.
        visitor_timeline = Window.partitionBy("visitor_id").orderBy("hit_ts").rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )

        hits = hits.withColumn(
            "last_engine",
            F.last(F.col("search_info.engine_domain"), ignorenulls=True).over(visitor_timeline),
        ).withColumn(
            "last_keyword",
            F.last(F.col("search_info.keyword"), ignorenulls=True).over(visitor_timeline),
        )

        # ── Filter to purchase hits with search attribution ──
        purchases = (
            hits
            .filter(F.col("event_list").isNotNull())
            .withColumn("events", F.split(F.regexp_replace(F.col("event_list"), r"\s", ""), ","))
            .filter(F.array_contains(F.col("events"), "1"))
            .filter(F.col("product_list").isNotNull() & (F.col("product_list") != ""))
            .filter(F.col("last_engine").isNotNull())
            .withColumn("revenue", parse_product_revenue(F.col("product_list")).cast("double"))
            .filter(F.col("revenue") > 0)
        )

        # ── Aggregate by engine + keyword ──
        result = (
            purchases
            .groupBy(
                F.col("last_engine").alias("engine_domain"), F.col("last_keyword").alias("keyword")
            )
            .agg(F.sum("revenue").alias("revenue"))
            .orderBy(F.col("revenue").desc())
        )

        output = result.select(
            F.col("engine_domain").alias("Search Engine Domain"),
            F.col("keyword").alias("Search Keyword"),
            F.round(F.col("revenue"), 2).alias("Revenue"),
        )

        today = datetime.now(timezone.utc).date().isoformat()
        output_file = f"{args['output_path'].rstrip('/')}/{today}_SearchKeywordPerformance"

        # Small aggregates: one Parquet file (fewer tiny objects). Large keyword cardinality: raise
        # --curated_output_partitions to parallelize the write (e.g. 8–32).
        if curated_output_partitions <= 1:
            output_to_write = output.coalesce(1)
        else:
            output_to_write = output.repartition(curated_output_partitions)
        output_to_write.write.mode("overwrite").parquet(output_file)
    finally:
        if base_persisted:
            base.unpersist()

    if sync_db_sinks:
        rows = result.collect()
        if rows:
            if not (db_host and db_name and db_secret_arn):
                raise RuntimeError("DB sink enabled but db_host/db_name/db_secret_arn not configured.")

            import json
            import boto3
            import pg8000

            secrets = boto3.client("secretsmanager")
            secret_value = secrets.get_secret_value(SecretId=db_secret_arn)
            payload = json.loads(secret_value.get("SecretString", "{}"))
            user = payload.get("username")
            password = payload.get("password")
            if not user or not password:
                raise RuntimeError("DB secret missing username/password keys.")

            fact_table = _validate_pg_identifier(db_fact_table, "db_fact_table")
            ai_table = _validate_pg_identifier(db_ai_table, "db_ai_table")
            ai_uk = _validate_pg_identifier(f"uq_{ai_table}_engine_kw", "AI unique index name")

            run_date = datetime.now(timezone.utc).date().isoformat()
            conn = pg8000.connect(
                host=db_host,
                port=db_port,
                user=user,
                password=password,
                database=db_name,
                timeout=20,
            )
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {fact_table} (
                    event_date DATE,
                    search_engine_domain TEXT,
                    search_keyword TEXT,
                    total_revenue NUMERIC(18,2)
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ai_table} (
                    keyword_id SERIAL PRIMARY KEY,
                    search_engine_domain TEXT,
                    search_keyword TEXT,
                    revenue_impact_score DOUBLE PRECISION,
                    last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            # Dedupe legacy rows so UNIQUE INDEX creation cannot fail (duplicate engine+keyword).
            cur.execute(
                f"""
                DELETE FROM {ai_table} a
                USING {ai_table} b
                WHERE a.keyword_id < b.keyword_id
                  AND a.search_engine_domain = b.search_engine_domain
                  AND a.search_keyword = b.search_keyword
                """
            )
            cur.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {ai_uk}
                ON {ai_table} (search_engine_domain, search_keyword)
                """
            )
            cur.execute(f"DELETE FROM {fact_table} WHERE event_date = %s", (run_date,))
            for row in rows:
                cur.execute(
                    f"""
                    INSERT INTO {fact_table}
                    (event_date, search_engine_domain, search_keyword, total_revenue)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (run_date, row["engine_domain"], row["keyword"], float(row["revenue"])),
                )
                cur.execute(
                    f"""
                    INSERT INTO {ai_table}
                    (search_engine_domain, search_keyword, revenue_impact_score)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (search_engine_domain, search_keyword)
                    DO UPDATE SET
                      revenue_impact_score = EXCLUDED.revenue_impact_score,
                      last_processed_at = CURRENT_TIMESTAMP
                    """,
                    (row["engine_domain"], row["keyword"], float(row["revenue"])),
                )
            conn.commit()
            cur.close()
            conn.close()

    job.commit()


if __name__ == "__main__":
    main()
