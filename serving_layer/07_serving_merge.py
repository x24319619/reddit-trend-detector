"""
DOWNTIME_DETECTOR - Serving Layer Merge (Reddit Trend Detector)
-------------------------------------------------------------------

This script implements the Serving Layer of the Lambda Architecture.

It combines:

    • Historical subreddit activity from the Batch Layer
      (Amazon Athena querying Parquet data stored in Amazon S3)

    • Live subreddit activity from the Speed Layer
      (Amazon DynamoDB updated by the real-time processing pipeline)

For each subreddit, the script compares the current comment rate
(comments/hour) against its historical average. A subreddit is classified
as TRENDING when its current activity exceeds a configurable multiple of
its historical baseline.

Usage:
    python3 07_serving_merge.py

Optional arguments:

    --min-baseline-comments
        Ignore subreddits with very little historical activity.

    --trend-threshold
        Trending threshold expressed as a multiple of the historical
        average (default = 2.0).
"""
import json
import os
from datetime import datetime
import argparse
import time

import boto3

ATHENA_DATABASE = "downtime_detector"
ATHENA_TABLE = "subreddit_baseline"
ATHENA_OUTPUT_LOCATION = "s3://downtime-detector-batch-data/athena-results/"

DYNAMODB_TABLE = "subreddit-comment-counts"

WINDOW_SIZE_SECONDS = 300


def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge historical baseline with live subreddit activity."
    )

    parser.add_argument(
        "--min-baseline-comments",
        type=int,
        default=1000,
        help="Ignore subreddits with fewer historical comments than this."
    )

    parser.add_argument(
        "--trend-threshold",
        type=float,
        default=2.0,
        help="Trending threshold multiplier."
    )

    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS Region."
    )

    return parser.parse_args()


def run_athena_query(query, region):
    """
    Run an Athena query and return the results as a list of dictionaries.
    """

    athena = boto3.client("athena", region_name=region)

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": ATHENA_DATABASE
        },
        ResultConfiguration={
            "OutputLocation": ATHENA_OUTPUT_LOCATION
        }
    )

    query_id = response["QueryExecutionId"]

    while True:

        execution = athena.get_query_execution(
            QueryExecutionId=query_id
        )

        state = execution["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break

        if state in ("FAILED", "CANCELLED"):

            reason = execution["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "Unknown error"
            )

            raise RuntimeError(
                f"Athena query failed: {reason}"
            )

        time.sleep(1)

    results = []

    next_token = None
    header = None

    while True:

        if next_token:

            response = athena.get_query_results(
                QueryExecutionId=query_id,
                NextToken=next_token
            )

        else:

            response = athena.get_query_results(
                QueryExecutionId=query_id
            )

        rows = response["ResultSet"]["Rows"]

        if header is None:
            header = [
                col["VarCharValue"]
                for col in rows[0]["Data"]
            ]
            rows = rows[1:]

        for row in rows:

            values = [
                col.get("VarCharValue")
                for col in row["Data"]
            ]

            results.append(
                dict(zip(header, values))
            )

        next_token = response.get("NextToken")

        if not next_token:
            break

    return results


def get_baseline(min_baseline_comments, region):
    """
    Retrieve the historical baseline from Athena.
    """

    query = f"""
    SELECT
        subreddit,
        total_comments,
        avg_comments_per_hour
    FROM {ATHENA_TABLE}
    WHERE total_comments >= {min_baseline_comments}
    """

    rows = run_athena_query(query, region)

    baseline = {}

    for row in rows:

        baseline[row["subreddit"]] = {

            "total_comments": int(row["total_comments"]),

            "avg_comments_per_hour": float(
                row["avg_comments_per_hour"]
            )
        }

    return baseline


def get_live_counts(region):
    """
    Retrieve the latest live comment counts from DynamoDB.
    """

    dynamodb = boto3.resource(
        "dynamodb",
        region_name=region
    )

    table = dynamodb.Table(DYNAMODB_TABLE)

    current_window = (
        int(time.time() // WINDOW_SIZE_SECONDS)
        * WINDOW_SIZE_SECONDS
    )

    items = []

    response = table.scan(
        FilterExpression="window_start = :w",
        ExpressionAttributeValues={
            ":w": current_window
        }
    )

    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:

        response = table.scan(
            FilterExpression="window_start = :w",
            ExpressionAttributeValues={
                ":w": current_window
            },
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )

        items.extend(response.get("Items", []))

    live_counts = {}

    for item in items:

        live_counts[item["subreddit"]] = int(
            item["comment_count"]
        )

    return live_counts, current_window


def main():

    args = parse_args()

    print("\n[Serving Layer] Loading historical baseline from Athena...")

    baseline = get_baseline(
        args.min_baseline_comments,
        args.region
    )

    print(
        f"Loaded {len(baseline)} historical subreddits."
    )

    print("\n[Serving Layer] Loading live metrics from DynamoDB...")

    live_counts, current_window = get_live_counts(
        args.region
    )

    print(
        f"Loaded {len(live_counts)} live subreddits "
        f"(window {current_window})."
    )

    print("\n========== SERVING LAYER RESULTS ==========\n")

    print(
        f"{'Subreddit':<22}"
        f"{'Current/5m':>12}"
        f"{'Current/hr':>12}"
        f"{'Baseline/hr':>14}"
        f"{'Ratio':>10}"
        f"{'Status':>12}"
    )

    print("-" * 86)

    merged_results = []

    for subreddit, base in baseline.items():

        current_5min = live_counts.get(subreddit, 0)

        if current_5min == 0:
            continue

        current_per_hour = (
            current_5min * (3600 / WINDOW_SIZE_SECONDS)
        )

        baseline_per_hour = base["avg_comments_per_hour"]

        ratio = (
            current_per_hour / baseline_per_hour
            if baseline_per_hour > 0
            else 0
        )

        status = (
            "TRENDING"
            if ratio >= args.trend_threshold
            else "NORMAL"
        )

        merged_results.append({

            "subreddit": subreddit,

            "current_5min": current_5min,

            "current_per_hour": current_per_hour,

            "baseline_per_hour": baseline_per_hour,

            "ratio": ratio,

            "status": status
        })

    merged_results.sort(
        key=lambda x: x["ratio"],
        reverse=True
    )

    for row in merged_results:

        print(
            f"{row['subreddit']:<22}"
            f"{row['current_5min']:>12}"
            f"{row['current_per_hour']:>12.1f}"
            f"{row['baseline_per_hour']:>14.1f}"
            f"{row['ratio']:>10.2f}x"
            f"{row['status']:>12}"
        )

    trending = [
        row
        for row in merged_results
        if row["status"] == "TRENDING"
    ]

    normal = len(merged_results) - len(trending)

    print("\n========== SUMMARY ==========")

    print(f"Historical baseline loaded : {len(baseline)}")

    print(f"Live metrics processed     : {len(live_counts)}")

    print(f"Normal activity            : {normal}")

    print(f"Trending detected          : {len(trending)}")

    print("=" * 30)

    save_results(
        merged_results,
        baseline,
        live_counts,
        trending,
        args.trend_threshold
    )

def save_results(merged_results, baseline, live_counts, trending, threshold):
    """
    Save the merged results for the Streamlit dashboard.
    """

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "historical_subreddits": len(baseline),
        "live_subreddits": len(live_counts),
        "trending_count": len(trending),
        "threshold": threshold,
        "results": merged_results
    }

    os.makedirs("../dashboard/data", exist_ok=True)

    with open("../dashboard/data/latest_results.json", "w") as f:
        json.dump(output, f, indent=4)

    print("\nDashboard data updated.")

if __name__ == "__main__":
    main()

