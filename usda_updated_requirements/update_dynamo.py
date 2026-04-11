import csv
import sys
import os
import logging
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

TABLE_NAME = "food-nutrients"


def main():
    if len(sys.argv) != 2:
        print("Usage: python update_dynamo.py <csv_file>")
        print("  csv_file: path to CSV with columns fdcId, database, subCategory")
        sys.exit(1)

    csv_file = sys.argv[1]

    # Load AWS credentials from .env
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, ".env"))

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION", "ap-south-1")

    # Read CSV
    rows = []
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    logging.info(f"Loaded {len(rows)} entries from {csv_file}")

    # Connect to DynamoDB
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    table = dynamodb.Table(TABLE_NAME)

    success = 0
    failed = []

    for row in rows:
        fdc_id = int(row["fdcId"])
        database = row["database"]
        sub_category = row["subCategory"]

        try:
            table.update_item(
                Key={"fdcId": fdc_id},
                UpdateExpression="SET #db = :db, #sc = :sc",
                ExpressionAttributeNames={
                    "#db": "database",
                    "#sc": "subCategory",
                },
                ExpressionAttributeValues={
                    ":db": database,
                    ":sc": sub_category,
                },
            )
            success += 1
            if success % 100 == 0:
                logging.info(f"  updated {success}/{len(rows)}...")
        except ClientError as e:
            logging.error(f"Failed fdcId {fdc_id}: {e.response['Error']['Message']}")
            failed.append(fdc_id)

    logging.info(f"\n=== DONE ===")
    logging.info(f"  Total:     {len(rows)}")
    logging.info(f"  Updated:   {success}")
    logging.info(f"  Failed:    {len(failed)}")
    if failed:
        logging.info(f"  Failed IDs: {failed[:20]}{'...' if len(failed) > 20 else ''}")


if __name__ == "__main__":
    main()
