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

import boto3
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
    redshift_workgroup_name = _optional_arg("redshift_workgroup_name")
    redshift_database = _optional_arg("redshift_database")
    redshift_secret_arn = _optional_arg("redshift_secret_arn")
    redshift_fact_table = _optional_arg("redshift_fact_table", "fact_keyword_performance")
    aurora_cluster_arn = _optional_arg("aurora_cluster_arn")
    aurora_database = _optional_arg("aurora_database")
    aurora_secret_arn = _optional_arg("aurora_secret_arn")
    aurora_ai_table = _optional_arg("aurora_ai_table", "ai_keyword_insights")

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
            redshift_data = boto3.client("redshift-data")
            rds_data = boto3.client("rds-data")
            run_date = date.today().isoformat()

            if redshift_workgroup_name and redshift_database and redshift_secret_arn:
                redshift_data.execute_statement(
                    WorkgroupName=redshift_workgroup_name,
                    Database=redshift_database,
                    SecretArn=redshift_secret_arn,
                    Sql=f"""
                    CREATE TABLE IF NOT EXISTS {redshift_fact_table} (
                        event_date DATE,
                        search_engine_domain VARCHAR(100),
                        search_keyword VARCHAR(500),
                        total_revenue DECIMAL(18,2)
                    );
                    """,
                )
                redshift_data.execute_statement(
                    WorkgroupName=redshift_workgroup_name,
                    Database=redshift_database,
                    SecretArn=redshift_secret_arn,
                    Sql=f"DELETE FROM {redshift_fact_table} WHERE event_date = :d",
                    Parameters=[{"name": "d", "value": {"stringValue": run_date}}],
                )
                for row in rows:
                    redshift_data.execute_statement(
                        WorkgroupName=redshift_workgroup_name,
                        Database=redshift_database,
                        SecretArn=redshift_secret_arn,
                        Sql=f"""
                        INSERT INTO {redshift_fact_table}
                        (event_date, search_engine_domain, search_keyword, total_revenue)
                        VALUES (:d, :engine, :keyword, :revenue)
                        """,
                        Parameters=[
                            {"name": "d", "value": {"stringValue": run_date}},
                            {"name": "engine", "value": {"stringValue": row["engine_domain"]}},
                            {"name": "keyword", "value": {"stringValue": row["keyword"]}},
                            {"name": "revenue", "value": {"doubleValue": float(row["revenue"])}},
                        ],
                    )

            if aurora_cluster_arn and aurora_database and aurora_secret_arn:
                rds_data.execute_statement(
                    resourceArn=aurora_cluster_arn,
                    secretArn=aurora_secret_arn,
                    database=aurora_database,
                    sql=f"""
                    CREATE TABLE IF NOT EXISTS {aurora_ai_table} (
                        keyword_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        search_engine_domain TEXT,
                        search_keyword TEXT,
                        revenue_impact_score DOUBLE PRECISION,
                        last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """,
                )
                for row in rows:
                    rds_data.execute_statement(
                        resourceArn=aurora_cluster_arn,
                        secretArn=aurora_secret_arn,
                        database=aurora_database,
                        sql=f"""
                        INSERT INTO {aurora_ai_table}
                        (search_engine_domain, search_keyword, revenue_impact_score)
                        VALUES (:engine, :keyword, :revenue)
                        """,
                        parameters=[
                            {"name": "engine", "value": {"stringValue": row["engine_domain"]}},
                            {"name": "keyword", "value": {"stringValue": row["keyword"]}},
                            {"name": "revenue", "value": {"doubleValue": float(row["revenue"])}},
                        ],
                    )

    job.commit()


if __name__ == "__main__":
    main()
