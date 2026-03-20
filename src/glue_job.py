"""
AWS Glue ETL job -- processes hit-level data at scale using Spark.

Reads a tab-separated file from S3, attributes search-engine revenue
to keywords using running last-touch attribution, and writes the
aggregated results back to S3.

Attribution model (matches the core SearchKeywordAnalyzer):
    For each purchase, find the most recent search-engine referrer
    for that visitor *prior to or at* the purchase hit.  This correctly
    handles visitors who search multiple keywords across multiple purchases.

Glue job parameters:
    --input_path   s3://bucket/input/data.tsv
    --output_path  s3://bucket/output/
"""

import sys
from datetime import date
from urllib.parse import parse_qs, urlparse

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

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
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


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


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])
    sync_db_sinks = _optional_arg("sync_db_sinks", "false").lower() == "true"
    db_host = _optional_arg("db_host")
    db_port = int(_optional_arg("db_port", "5432"))
    db_name = _optional_arg("db_name")
    db_secret_arn = _optional_arg("db_secret_arn")
    db_fact_table = _optional_arg("db_fact_table", "fact_keyword_performance")
    db_ai_table = _optional_arg("db_ai_table", "ai_keyword_insights")

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    df = spark.read.option("header", "true").option("delimiter", "\t").csv(args["input_path"])

    hits = (
        df
        .withColumn("visitor_id", F.concat_ws("|", F.col("ip"), F.col("user_agent")))
        .withColumn(
            "hit_ts",
            # Ensure malformed/non-numeric timestamps don't become NULL,
            # which would make window ordering unstable.
            F.when(
                F.col("hit_time_gmt").rlike(r"^\d+$"),
                F.col("hit_time_gmt").cast("long"),
            ).otherwise(F.lit(0).cast("long")),
        )
        .withColumn("search_info", parse_search_referrer(F.col("referrer")))
    )

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
        .groupBy(F.col("last_engine").alias("engine_domain"), F.col("last_keyword").alias("keyword"))
        .agg(F.sum("revenue").alias("revenue"))
        .orderBy(F.col("revenue").desc())
    )

    output = result.select(
        F.col("engine_domain").alias("Search Engine Domain"),
        F.col("keyword").alias("Search Keyword"),
        F.round(F.col("revenue"), 2).alias("Revenue"),
    )

    today = date.today().isoformat()
    output_file = f"{args['output_path'].rstrip('/')}/{today}_SearchKeywordPerformance"

    # Write Parquet for fast columnar reads in BI tools.
    # Spark will create `output_file/part-*.parquet`.
    output.write.mode("overwrite").parquet(output_file)

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

            run_date = date.today().isoformat()
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
                CREATE TABLE IF NOT EXISTS {db_fact_table} (
                    event_date DATE,
                    search_engine_domain TEXT,
                    search_keyword TEXT,
                    total_revenue NUMERIC(18,2)
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {db_ai_table} (
                    keyword_id SERIAL PRIMARY KEY,
                    search_engine_domain TEXT,
                    search_keyword TEXT,
                    revenue_impact_score DOUBLE PRECISION,
                    last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(f"DELETE FROM {db_fact_table} WHERE event_date = %s", (run_date,))
            for row in rows:
                cur.execute(
                    f"""
                    INSERT INTO {db_fact_table}
                    (event_date, search_engine_domain, search_keyword, total_revenue)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (run_date, row["engine_domain"], row["keyword"], float(row["revenue"])),
                )
                cur.execute(
                    f"""
                    INSERT INTO {db_ai_table}
                    (search_engine_domain, search_keyword, revenue_impact_score)
                    VALUES (%s, %s, %s)
                    """,
                    (row["engine_domain"], row["keyword"], float(row["revenue"])),
                )
            conn.commit()
            cur.close()
            conn.close()

    job.commit()


if __name__ == "__main__":
    main()
