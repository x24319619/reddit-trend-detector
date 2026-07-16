"""
Reddit-Trend-Detector- Reddit CSV Splitter
-------------------------------------------
Splits reddit_sample.csv into two parts, per the lecturer's guidance:

  - First 75% of comment rows  -> batch_master_data.csv
      This becomes the batch layer's "master data" / historical baseline.
      uploading it to  S3  -- it's what
       EMR/PySpark job reads to compute historical average comment
      volume per subreddit.

  - Last 25% of comment rows   -> speed_layer_stream.csv
      This is the "new" data that simulates live incoming comments. ONLY
      this file gets fed through 02_producer.py into Kinesis -> Lambda ->
      DynamoDB (the speed layer).


Usage (run in CloudShell, wherever reddit_sample.csv is):
    python3 05_split_log_data.py --file reddit_sample.csv

Produces two files in the same folder:
    batch_master_data.csv
    speed_layer_stream.csv
"""

import argparse
import csv
import sys

csv.field_size_limit(10_000_000)  # some comment bodies can be unusually long


def parse_args():
    p = argparse.ArgumentParser(description="Split reddit_sample.csv into 75% batch / 25% speed-layer portions")
    p.add_argument("--file", required=True, help="Path to reddit_sample.csv")
    p.add_argument("--batch-ratio", type=float, default=0.75,
                   help="Fraction of rows to use as batch master data (default 0.75 = 75%%)")
    p.add_argument("--batch-out", default="batch_master_data.csv", help="Output filename for batch master data")
    p.add_argument("--speed-out", default="speed_layer_stream.csv", help="Output filename for speed-layer stream data")
    return p.parse_args()


def split_file(filepath, batch_ratio, batch_out, speed_out):
    # PASS 1: count total data rows without holding them all in memory.
    # This streams through the file once, discarding each row after counting it.
    with open(filepath, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        total = sum(1 for _ in reader)

    if total == 0:
        print("[split] ERROR: input file has no data rows.", file=sys.stderr)
        sys.exit(1)

    split_index = int(total * batch_ratio)

    # PASS 2: stream through again, writing each row straight to the correct
    # output file as we go -- never holding more than one row in memory.
    with open(filepath, newline="", encoding="utf-8", errors="replace") as f_in, \
         open(batch_out, "w", newline="", encoding="utf-8") as f_batch, \
         open(speed_out, "w", newline="", encoding="utf-8") as f_speed:

        reader = csv.reader(f_in)
        next(reader)  # skip header in the input, we write our own below

        batch_writer = csv.writer(f_batch)
        speed_writer = csv.writer(f_speed)
        batch_writer.writerow(header)
        speed_writer.writerow(header)

        batch_count = 0
        speed_count = 0
        for i, row in enumerate(reader):
            if i < split_index:
                batch_writer.writerow(row)
                batch_count += 1
            else:
                speed_writer.writerow(row)
                speed_count += 1

    print(f"[split] total data rows: {total}")
    print(f"[split] batch master data ({batch_ratio*100:.0f}%): {batch_count} rows -> {batch_out}")
    print(f"[split] speed layer stream ({(1-batch_ratio)*100:.0f}%): {speed_count} rows -> {speed_out}")
    print(f"[split] done. Next steps:")
    print(f"  1. Upload '{batch_out}' directly to S3 (this feeds the EMR/PySpark batch job)")
    print(f"  2. Run 02_producer.py against '{speed_out}' ONLY (this feeds Kinesis -> Lambda -> DynamoDB)")


if __name__ == "__main__":
    args = parse_args()
    split_file(args.file, args.batch_ratio, args.batch_out, args.speed_out)
