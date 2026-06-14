"""Standalone PySpark benchmark script for Dataproc.

Usage:
    gcloud dataproc jobs submit pyspark \\
        --cluster=$DATAPROC_CLUSTER \\
        --region=$DATAPROC_REGION \\
        --project=$GCP_PROJECT \\
        dataproc_benchmark.py \\
        -- gs://<bucket>/tbd_phase2/group_02/events.parquet \\
              gs://<bucket>/tbd_phase2/group_02/dimension.parquet \\
              gs://<bucket>/tbd_phase2/group_02/results.json

Outputs timing results as JSON to the provided GCS path.
"""

import sys
import json
import time
import gc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

N_REPS = 3


def run(fn, n=N_REPS):
    times = []
    for _ in range(n):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    times.sort()
    return times[len(times) // 2]  # median


def main(events_path, dimension_path, output_path):
    spark = (
        SparkSession.builder
        .appName("TBDPhase2DataprocBenchmark")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    results = {}

    # Q1 ──────────────────────────────────────────────────────────────────────
    def q1():
        df = spark.read.parquet(events_path)
        r = (
            df.filter(
                (F.col("event_date") >= F.lit("2026-01-01").cast(DateType())) &
                (F.col("event_date") <= F.lit("2026-01-31").cast(DateType())) &
                (F.col("country") == "PL") &
                F.col("revenue").isNotNull()
            )
            .groupBy("event_date")
            .agg(F.sum("revenue").alias("total_revenue"), F.avg("revenue").alias("avg_revenue"), F.count("*").alias("order_count"))
            .orderBy("event_date")
        )
        r.cache(); r.count(); r.unpersist()

    results["Q1_daily_revenue_filter"] = run(q1)
    print(f"Q1 median: {results['Q1_daily_revenue_filter']:.3f}s")

    # Q2 ──────────────────────────────────────────────────────────────────────
    def q2():
        orders = spark.read.parquet(events_path)
        products = spark.read.parquet(dimension_path)
        r = (
            orders.filter(F.col("revenue").isNotNull())
            .join(F.broadcast(products), on="product_id", how="inner")
            .groupBy("category")
            .agg(F.sum("revenue").alias("total_revenue"), F.avg("revenue").alias("avg_order_value"),
                 F.count("*").alias("order_count"), F.sum("quantity").alias("total_quantity"))
            .orderBy(F.desc("total_revenue"))
        )
        r.cache(); r.count(); r.unpersist()

    results["Q2_category_revenue_join"] = run(q2)
    print(f"Q2 median: {results['Q2_category_revenue_join']:.3f}s")

    # Q3 ──────────────────────────────────────────────────────────────────────
    def q3():
        df = spark.read.parquet(events_path)
        r = (
            df.filter(F.col("revenue").isNotNull())
            .groupBy("customer_id")
            .agg(F.sum("revenue").alias("total_revenue"), F.count("*").alias("order_count"), F.avg("quantity").alias("avg_quantity"))
            .orderBy(F.desc("total_revenue"))
            .limit(50)
        )
        r.cache(); r.count(); r.unpersist()

    results["Q3_top50_customers"] = run(q3)
    print(f"Q3 median: {results['Q3_top50_customers']:.3f}s")

    payload = json.dumps(results, indent=2)
    print("Results:", payload)

    # Write to GCS
    spark.sparkContext.parallelize([payload], 1).saveAsTextFile(output_path)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: dataproc_benchmark.py <events_path> <dimension_path> <output_gcs_path>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3])
