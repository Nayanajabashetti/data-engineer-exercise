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

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    df = spark.read.option("header", "true").option("delimiter", "\t").csv(args["input_path"])

    hits = (
        df
        .withColumn("visitor_id", F.concat_ws("|", F.col("ip"), F.col("user_agent")))
        .withColumn("hit_ts", F.col("hit_time_gmt").cast("long"))
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
        F.format_number("revenue", 2).alias("Revenue"),
    )

    today = date.today().isoformat()
    output_file = f"{args['output_path'].rstrip('/')}/{today}_SearchKeywordPerformance"

    output.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", "\t").csv(output_file)

    job.commit()


if __name__ == "__main__":
    main()
