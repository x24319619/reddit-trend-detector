"""
DOWNTIME_DETECTOR - Serving Layer Merge (Reddit Trend Detector)
-------------------------------------------------------------------
This is the "merge" step of the Lambda architecture: it joins the batch
layer's historical baseline (queried from Athena, backed by the Parquet
output in S3) with the speed layer's live comment counts (queried directly
from DynamoDB), and flags any subreddit whose current activity is
significantly above its historical norm as "trending".

Usage:
    pip install boto3 --user
    python3 07_serving_merge.py --min-baseline-comments 1000 --trend-threshold 2.0

    --min-baseline-comments  : ignore subreddits with too little historical
                                volume to have a meaningful baseline (same
                                reasoning as the "1-visit endpoint" issue
                                seen in the earlier server-log version)
    --trend-threshold        : how many multiples of the historical average
                                counts as "trending" (2.0 = current rate is
                                at least 2x the historical norm)
"""
import argparse
import time

import boto3

ATHENA_DATABASE = "downtime_detector"
ATHENA_TABLE = "subreddit_baseline"
ATHENA_OUTPUT_LOCATION = "s3://downtime-detector-batch-data/athena-results/"
DYNAMODB_TABLE = "subreddit-comment-counts"
WINDOW_SIZE_SECONDS = 300  # must match the speed layer Lambda's window size


def parse_args():
    p = argparse.ArgumentParser(description="Merge batch baseline with live counts to flag trending subreddits")
    p.add_argument("--min-baseline-comments", type=int, default=1000,
                   help="Ignore subreddits with fewer historical comments than this (avoids noisy low-volume baselines)")
    p.add_argument("--trend-threshold", type=float, default=2.0,
                   help="Flag as trending if current rate >= this many times the historical baseline")
    p.add_argument("--region", default="us-east-1")
    return p.parse_args()


def run_athena_query(query, region):
    """Running a query against Athena and return rows as a list of dicts."""
    athena = boto3.client("athena", region_name=region)

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    query_id = response["QueryExecutionId"]

    # Poll until the query finishes
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown error")
        raise RuntimeError(f"Athena query failed: {reason}")

    results = athena.get_query_results(QueryExecutionId=query_id)
    rows = results["ResultSet"]["Rows"]
    header = [col["VarCharValue"] for col in rows[0]["Data"]]
    data = []
    for row in rows[1:]:
        values = [col.get("VarCharValue") for col in row["Data"]]
        data.append(dict(zip(header, values)))
    return data


def get_baseline(min_baseline_comments, region):
    """Fetching the historical baseline (avg comments/hour) per subreddit from Athena."""
    query = f"""
        SELECT subreddit, total_comments, avg_comments_per_hour
        FROM {ATHENA_TABLE}
        WHERE total_comments >= {min_baseline_comments}
    """
    rows = run_athena_query(query, region)
    return {
        r["subreddit"]: {
            "total_comments": int(r["total_comments"]),
            "avg_comments_per_hour": float(r["avg_comments_per_hour"]),
        }
        for r in rows
    }


def get_live_counts(region):
    """Fetching the most recent window's live comment counts per subreddit from DynamoDB."""
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(DYNAMODB_TABLE)

    now = int(time.time())
    current_window = int(now // WINDOW_SIZE_SECONDS) * WINDOW_SIZE_SECONDS

    # DynamoDB doesn't support a simple "all items in this window" scan
    # efficiently at scale, but for this project's data volume a full scan
    # filtered by window_start is fine. For production, a GSI on
    # window_start would be the better pattern.
    response = table.scan(
        FilterExpression="window_start = :w",
        ExpressionAttributeValues={":w": current_window},
    )
    items = response.get("Items", [])

    live_counts = {}
    for item in items:
        subreddit = item["subreddit"]
        count = int(item["comment_count"])
        live_counts[subreddit] = count
    return live_counts, current_window


def main():
    args = parse_args()

    print("[serving-layer] fetching historical baseline from Athena...")
    baseline = get_baseline(args.min_baseline_comments, args.region)
    print(f"[serving-layer] loaded baseline for {len(baseline)} subreddits "
          f"(min {args.min_baseline_comments} historical comments)")

    print("[serving-layer] fetching live counts from DynamoDB...")
    live_counts, window_start = get_live_counts(args.region)
    print(f"[serving-layer] found live data for {len(live_counts)} subreddits "
          f"in window starting {window_start}")

    print("\n[serving-layer] TRENDING SUBREDDITS RIGHT NOW:\n")
    print(f"{'subreddit':<20} {'current/5min':>14} {'current/hr':>12} {'baseline/hr':>12} {'ratio':>8}")
    print("-" * 70)

    trending_found = False
    for subreddit, base in baseline.items():
        current_5min = live_counts.get(subreddit, 0)
        if current_5min == 0:
            continue
        current_per_hour = current_5min * (3600 / WINDOW_SIZE_SECONDS)
        ratio = current_per_hour / base["avg_comments_per_hour"] if base["avg_comments_per_hour"] > 0 else 0

        if ratio >= args.trend_threshold:
            trending_found = True
            print(f"{subreddit:<20} {current_5min:>14} {current_per_hour:>12.1f} "
                  f"{base['avg_comments_per_hour']:>12.1f} {ratio:>7.1f}x")

    if not trending_found:
        print("(no subreddits currently exceed the trending threshold)")


if __name__ == "__main__":
    main()