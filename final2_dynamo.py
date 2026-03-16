import os
import csv
import json
import logging
import decimal
import requests
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from dynamo_db_script import HIERARCHY, PARENT_LOOKUP, build_nutrient_lookup

# Mapping from nutrient_nbr (string like "255") -> nid (int), built from HIERARCHY.
# Used to resolve abridged API response format where nutrients use 'number' instead of 'id'.
_NBR_TO_NID = {nbr: nid for nid, name, unit, nbr, parent_id, level, group in HIERARCHY if nbr}

def build_nutrient_lookup_abridged(raw_food_nutrients: list) -> dict:
    """
    Build nutrient lookup from ABRIDGED API response format.
    Abridged nutrients: {"number": "255", "name": "Water", "amount": 93.6, ...}
    Returns the same shape as build_nutrient_lookup() output keyed by nid (int).
    """
    lookup = {}
    for fn in raw_food_nutrients:
        nbr = fn.get("number")
        if nbr and nbr in _NBR_TO_NID:
            nid = _NBR_TO_NID[nbr]
            if nid not in lookup:
                lookup[nid] = fn
    return lookup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def process_api_response(data):
    """
    Process the parsed JSON data directly from the USDA API response.
    Refines it using the exact logic provided in dynamo_db_script.py.
    """
    fdc_id = data.get("fdcId")
    description = data.get("description")
    if not description:
        description = "No description"
    
    out_data = {
        "fdcId": fdc_id,
        "description": description
    }
    
    raw_nutrients = data.get("foodNutrients", [])

    # Auto-detect response format:
    # Default: {"nutrient": {"id": 1051}, "amount": 93.6}
    # Abridged: {"number": "255", "name": "Water", "amount": 93.6}
    is_abridged = bool(raw_nutrients and "number" in raw_nutrients[0] and "nutrient" not in raw_nutrients[0])

    if is_abridged:
        nutrient_lookup = build_nutrient_lookup_abridged(raw_nutrients)
    else:
        nutrient_lookup = build_nutrient_lookup(raw_nutrients)

    # We evaluate everything present in HIERARCHY
    for entry in HIERARCHY:
        nid, name, unit, nbr, parent_id, level, group = entry
        
        # We also need to check if ANY of its children have an amount or data
        def has_data_or_children_with_data(current_nid):
            if current_nid in nutrient_lookup:
                return True
            for child_entry in HIERARCHY:
                if child_entry[4] == current_nid:
                    if has_data_or_children_with_data(child_entry[0]):
                        return True
            return False
            
        if not has_data_or_children_with_data(nid):
            continue
            
        fn_data = nutrient_lookup.get(nid)
        amount = None
        if fn_data is not None:
            amount = fn_data.get("amount")
            
        parent_info = PARENT_LOOKUP.get(nid, {})
        
        entry_key = name.lower()
        entry_dict = {
            "nutrient_id": nid,
            "name": name,
            "unit": unit
        }
        
        if amount is not None:
            entry_dict["value"] = amount
            
        pid = parent_info.get("parent_id")
        if pid is not None:
            entry_dict["parent_nutrient_id"] = pid
            entry_dict["parent_nutrient_name"] = parent_info.get("parent_name")
            
        out_data[entry_key] = entry_dict

    return out_data

def get_dynamodb_table():
    """
    Load AWS credentials from .env and return the boto3 DynamoDB table resource.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, '.env'))
    
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region_name = os.getenv('AWS_REGION', 'us-east-1')

    if not aws_access_key_id or not aws_secret_access_key:
        logging.warning("AWS credentials not found in environment variables. Assuming default credentials or IAM role.")

    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    return dynamodb.Table('food-nutrients')

def read_fdc_ids_from_csv(csv_file_path):
    """
    Read fdcId values from a given CSV file.
    Assumes a single column with header 'fdcId', or automatically attempts to fall back to the first column.
    """
    fdc_ids = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            
            # Check if there is an explicit generic column named 'fdcId' (case-insensitive)
            col_idx = 0
            if header:
                for idx, col in enumerate(header):
                    if 'fdcid' in col.strip().lower():
                        col_idx = idx
                        break
                
                # if header didn't strictly contain 'fdcid' but looks like a number
                if col_idx == 0 and header[0].strip().isdigit():
                    fdc_ids.append(header[0].strip())
            
            for row in reader:
                if row and len(row) > col_idx:
                    val = row[col_idx].strip()
                    if val:
                        fdc_ids.append(val)
    except Exception as e:
        logging.error(f"Failed to read CSV file '{csv_file_path}': {str(e)}")
        
    return fdc_ids

def main():
    import argparse
    import time
    from datetime import date
    
    parser = argparse.ArgumentParser(description="Process USDA FDC API and store in DynamoDB")
    parser.add_argument("csv_file", help="Name of the CSV file inside fdcIds/ folder")
    args = parser.parse_args()

    # Flexible path resolution:
    # 1. Try path as provided
    # 2. Try inside fdcIds/ folder relative to script
    csv_file_path = args.csv_file
    if not os.path.exists(csv_file_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        fdc_ids_dir = os.path.join(script_dir, "fdcIds")
        csv_file_path = os.path.join(fdc_ids_dir, args.csv_file)
    
    if not os.path.exists(csv_file_path):
        logging.error(f"CSV file '{args.csv_file}' not found (tested direct path and 'fdcIds/' folder).")
        return

    fdc_ids = read_fdc_ids_from_csv(csv_file_path)
    if not fdc_ids:
        logging.warning(f"No fdcId values found in '{csv_file_path}'. Exiting.")
        return
        
    logging.info(f"Found {len(fdc_ids)} fdcId(s) in the CSV.")

    table = get_dynamodb_table()
    api_key = "f8r37KRDp65vbKLqG0bN710NdhCnWc2aIG51jTVf"
    
    success_count = 0
    failed_ids = []  # track (fdc_id, reason) pairs
    
    for fdc_id in fdc_ids:
        url = f"https://api.nal.usda.gov/fdc/v1/food/{fdc_id}?format=abridged&api_key={api_key}"
        logging.info(f"Fetching data for fdcId: {fdc_id}...")
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    # Parse using strict decimal to avoid boto3 Float type errors
                    data = json.loads(response.text, parse_float=decimal.Decimal)
                    out_data = process_api_response(data)
                    
                    try:
                        table.put_item(Item=out_data)
                        logging.info(f"  ✓ Successfully processed and pushed fdcId: {fdc_id}")
                        success_count += 1
                        break # Break retry loop on success
                    except ClientError as e:
                        logging.error(f"  ✗ DynamoDB ClientError for fdcId {fdc_id}: {e.response['Error']['Message']}")
                        logging.error(f"  Failed Item Payload: {json.dumps(out_data, default=str)}")
                        failed_ids.append((fdc_id, "DynamoDB error"))
                        break # Don't retry API on DB error
                elif response.status_code == 429:
                    if attempt < max_retries - 1:
                        logging.warning(f"  ⚠ Rate limited (429) for fdcId: {fdc_id}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        logging.error(f"  ✗ API request failed after {max_retries} retries for fdcId: {fdc_id}. Status: {response.status_code}")
                        failed_ids.append((fdc_id, "rate limited"))
                elif response.status_code == 404:
                    logging.error(f"  ✗ API request failed: fdcId {fdc_id} not found (404).")
                    logging.warning(f"    Likely a subsample ID (valid in local JSON dumps but not queryable via /food/{{fdcId}} endpoint). Skipping.")
                    failed_ids.append((fdc_id, "404 not found - likely subsample ID"))
                    break # Don't retry 404
                else:
                    logging.error(f"  ✗ API request failed for fdcId: {fdc_id}. Status code: {response.status_code}")
                    failed_ids.append((fdc_id, f"HTTP {response.status_code}"))
                    break # Don't retry other errors by default
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                     logging.warning(f"  ⚠ Request error for fdcId: {fdc_id} ({e}). Retrying in {retry_delay} seconds...")
                     time.sleep(retry_delay)
                     retry_delay *= 2
                else:
                    logging.error(f"  ✗ Request failed after {max_retries} retries for fdcId: {fdc_id}: {e}")
            except Exception as e:
                logging.exception(f"  ✗ Unexpected error processing fdcId {fdc_id}: {e}")
                failed_ids.append((fdc_id, f"exception: {e}"))
                break

    # --- Final summary ---
    total = len(fdc_ids)
    logging.info(f"\n=== RUN SUMMARY ==================================")
    logging.info(f"  Total IDs processed : {total}")
    logging.info(f"  ✓ Succeeded         : {success_count}")
    logging.info(f"  ✗ Failed            : {len(failed_ids)}")
    logging.info(f"==================================================")

    if failed_ids:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_path = os.path.join(script_dir, f"failed_fdc_ids_{date.today()}.log")
        with open(log_path, 'w') as lf:
            lf.write(f"# Failed fdcIds — {date.today()}\n")
            lf.write(f"# These IDs could not be fetched from the live USDA API.\n")
            lf.write(f"# 404 errors = subsample IDs not accessible via /food/{{fdcId}} endpoint.\n\n")
            for fid, reason in failed_ids:
                lf.write(f"{fid}\t{reason}\n")
        logging.info(f"  Failed ID list written to: {log_path}")

if __name__ == "__main__":
    main()
