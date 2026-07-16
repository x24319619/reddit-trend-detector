"""
Reddit-Trend-Detector Kinesis Producer (Reddit Comments dataset)
-------------------------------------------------------------------
Reads reddit_sample.csv (a slice of the "May 2015 Reddit Comments" dataset,
pulled via SQL from the full 31GB Pushshift SQLite database) and streams
each comment into AWS Kinesis Data Streams as a JSON record, at a
configurable rate -- simulating a continuous live comment feed.

IMPORTANT: unlike the earlier Apache-log producer, this file is genuine CSV
with a header row, and some `body` fields contain embedded newlines (a
multi-line comment, still inside one quoted CSV field). We MUST use
Python's csv module (not naive line-splitting or regex) so multi-line
comments are parsed as a single record, not split into garbage extra rows.

Columns in reddit_sample.csv: subreddit, body, created_utc, score, author

Usage:
    pip install boto3 --user
    python3 02_producer.py --file reddit_sample.csv --stream server-pulse-stream --rate 20

    --rate 20       -> ~20 records/second (raise this to stress-test for benchmarking)
    --loop          -> keep replaying the file forever (good for long demo recordings)
    --jitter 0.3    -> +/-30% random variation in send timing, so it doesn't look
                       like a metronome in the demo
"""
import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime, timezone

import boto3

csv.field_size_limit(10_000_000)  # some comment bodies can be unusually long


def parse_args():
    p = argparse.ArgumentParser(description="Replay Reddit comments CSV into Kinesis at a controlled rate")
    p.add_argument("--file", required=True, help="Path to reddit_sample.csv")
    p.add_argument("--stream", default="server-pulse-stream", help="Kinesis stream name")
    p.add_argument("--region", default="us-east-1", help="AWS region")
    p.add_argument("--rate", type=float, default=20.0, help="Target records per second")
    p.add_argument("--loop", action="store_true", help="Loop over the file forever")
    p.add_argument("--jitter", type=float, default=0.3, help="Randomness in inter-record delay (0-1)")
    p.add_argument("--max-records", type=int, default=None, help="Stop after N records (for quick tests)")
    return p.parse_args()


def build_record(row):
    """Turn one CSV row (dict) into our canonical JSON schema."""
    subreddit = (row.get("subreddit") or "unknown").strip()
    body = row.get("body") or ""
    try:
        created_utc = int(float(row.get("created_utc") or 0))
    except (TypeError, ValueError):
        created_utc = None
    try:
        score = int(float(row.get("score") or 0))
    except (TypeError, ValueError):
        score = None
    author = row.get("author") or "unknown"

    record = {
        "subreddit": subreddit,
        "body": body[:500],  # trim very long comments to keep record size sane
        "created_utc": created_utc,   # original comment timestamp (May 2015)
        "score": score,
        "author": author,
    }

    # Stamp with the REAL send time -- this is what the speed layer windows
    # on, not the comment's original 2015 timestamp. Same principle as the
    # earlier log-file producer's ingested_at/ingested_epoch fields.
    now = datetime.now(timezone.utc)
    record["ingested_at"] = now.isoformat()
    record["ingested_epoch"] = int(now.timestamp())
    return record


def stream_records(filepath, stream_name, region, rate, loop, jitter, max_records):
    kinesis = boto3.client("kinesis", region_name=region)
    delay = 1.0 / rate if rate > 0 else 0
    sent = 0
    skipped = 0

    while True:
        with open(filepath, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)  # correctly handles embedded newlines in quoted fields
            for row in reader:
                if max_records and sent >= max_records:
                    print(f"[producer] reached --max-records={max_records}, stopping.", file=sys.stderr)
                    return

                try:
                    record = build_record(row)
                except Exception as e:
                    skipped += 1
                    if skipped <= 5:
                        print(f"[producer] skipped malformed row: {e}", file=sys.stderr)
                    continue

                # Kinesis partition keys must be <= 256 chars, and subreddit
                # names are always short, so no truncation risk here -- but
                # guard anyway for safety.
                partition_key = record["subreddit"][:256] or "default"

                try:
                    kinesis.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(record).encode("utf-8"),
                        PartitionKey=partition_key,
                    )
                    sent += 1
                    if sent % 50 == 0:
                        print(f"[producer] sent {sent} records... ({skipped} skipped so far)", file=sys.stderr)
                except Exception as e:
                    print(f"[producer] put_record failed: {e}", file=sys.stderr)

                sleep_time = delay * (1 + random.uniform(-jitter, jitter)) if delay else 0
                if sleep_time > 0:
                    time.sleep(max(0, sleep_time))
        if not loop:
            break

    print(f"[producer] done. sent={sent} skipped={skipped}", file=sys.stderr)


if __name__ == "__main__":
    args = parse_args()
    stream_records(
        filepath=args.file,
        stream_name=args.stream,
        region=args.region,
        rate=args.rate,
        loop=args.loop,
        jitter=args.jitter,
        max_records=args.max_records,
    )
