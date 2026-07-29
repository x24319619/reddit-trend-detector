import base64
import json
import time
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('subreddit-comment-counts')

WINDOW_SIZE_SECONDS = 300  # 5-minute sliding window
TTL_SECONDS = 3600         # keep records for 1 hour, then auto-expire


def get_window_start(timestamp_epoch):
    return int(timestamp_epoch // WINDOW_SIZE_SECONDS) * WINDOW_SIZE_SECONDS


def lambda_handler(event, context):
    processed = 0
    counted = 0

    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            log_entry = json.loads(payload)

            subreddit = log_entry.get('subreddit', 'unknown')
            event_time = log_entry.get('ingested_epoch', time.time())

            window_start = get_window_start(event_time)
            expiry_time = int(time.time()) + TTL_SECONDS

            update_comment_count(subreddit, window_start, expiry_time)
            counted += 1
            processed += 1

        except Exception as e:
            print(f"Failed to process record: {e}")
            continue

    print(f"Processed {processed} records, {counted} comments counted")
    return {
        'statusCode': 200,
        'processed': processed,
        'comments_counted': counted
    }


def update_comment_count(subreddit, window_start, expiry_time):
    table.update_item(
        Key={
            'subreddit': subreddit,
            'window_start': window_start
        },
        UpdateExpression='ADD comment_count :inc SET expiry_time = :exp',
        ExpressionAttributeValues={
            ':inc': 1,
            ':exp': expiry_time
        }
    )