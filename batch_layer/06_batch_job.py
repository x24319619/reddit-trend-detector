"""
Batch Layer PySpark Job (Reddit Comments dataset)
--------------------------------------------------------------------------
Reads batch_master_data.csv (the 75% "historical" slice, uploaded to S3),
and computes per-subreddit historical statistics:

    - total_comments        : how many comments this subreddit received
                               across the whole historical period covered
    - avg_comments_per_hour  : the historical baseline the speed layer's
                               live comment rate gets compared against

Usage (submit as an EMR Step):

    spark-submit 06_batch_job.py \\
        --input s3://your-bucket-name/batch_master_data.csv \\
        --output s3://your-bucket-name/batch_output/

"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, min as spark_min, max as spark_max


def parse_args():
    p = argparse.ArgumentParser(description="Batch layer: compute historical comment-volume baseline per subreddit")
    p.add_argument("--input", required=True, help="S3 path to batch_master_data.csv")
    p.add_argument("--output", required=True, help="S3 path to write Parquet output")
    return p.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("edditTrendDetector-BatchLayer").getOrCreate()

    raw = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(args.input)
    )

    parsed = raw.select(
        col("subreddit"),
        col("created_utc").cast("long"),
    ).filter(col("subreddit").isNotNull())

    # Compute per-subreddit totals and the time span covered, so we can
    # derive a genuine "average comments per hour" baseline rather than
    # just a raw total (which wouldn't be comparable across subreddits
    # covering different time windows).
    subreddit_stats = (
        parsed.groupBy("subreddit")
        .agg(
            count("*").alias("total_comments"),
            spark_min("created_utc").alias("earliest_ts"),
            spark_max("created_utc").alias("latest_ts"),
        )
        .withColumn(
            "hours_covered",
            ((col("latest_ts") - col("earliest_ts")) / 3600.0)
        )
        .withColumn(
            "avg_comments_per_hour",
            # guard against divide-by-zero for subreddits with only one
            # timestamp (hours_covered would be 0)
            col("total_comments") / (col("hours_covered") + 0.0001)
        )
    )

    # Write output as Parquet, ready for Athena to query
    subreddit_stats.write.mode("overwrite").parquet(args.output)

    print(f"[batch-layer] wrote subreddit statistics to {args.output}")
    subreddit_stats.orderBy(col("total_comments").desc()).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()