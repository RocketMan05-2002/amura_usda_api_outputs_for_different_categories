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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def process_api_response(data):
    """
    Process the parsed JSON data directly from the USDA API response.
    Refines it using the exact logic provided in dynamo_db_script.py.
    """
    fdc_id = data.get("fdcId")
    description = data.get("description", "")
    
    out_data = {
        "fdcId": fdc_id,
        "description": description
    }
    
    raw_nutrients = data.get("foodNutrients", [])
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
    load_dotenv()
    
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
    parser = argparse.ArgumentParser(description="Process USDA FDC API and store in DynamoDB")
    parser.add_argument("csv_file", help="Path to the CSV file containing fdcId column")
    args = parser.parse_args()

    csv_file_path = args.csv_file
    if not os.path.exists(csv_file_path):
        logging.error(f"CSV file '{csv_file_path}' does not exist.")
        return

    fdc_ids = read_fdc_ids_from_csv(csv_file_path)
    if not fdc_ids:
        logging.warning(f"No fdcId values found in '{csv_file_path}'. Exiting.")
        return
        
    logging.info(f"Found {len(fdc_ids)} fdcId(s) in the CSV.")

    table = get_dynamodb_table()
    api_key = "f8r37KRDp65vbKLqG0bN710NdhCnWc2aIG51jTVf"
    
    for fdc_id in fdc_ids:
        url = f"https://api.nal.usda.gov/fdc/v1/food/{fdc_id}?api_key={api_key}"
        logging.info(f"Fetching data for fdcId: {fdc_id}...")
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                # Parse using strict decimal to avoid boto3 Float type errors
                data = json.loads(response.text, parse_float=decimal.Decimal)
                out_data = process_api_response(data)
                
                table.put_item(Item=out_data)
                logging.info(f"  ✓ Successfully processed and pushed fdcId: {fdc_id}")
            else:
                logging.error(f"  ✗ API request failed for fdcId: {fdc_id}. Status code: {response.status_code}")
                
        except ClientError as e:
            logging.error(f"  ✗ DynamoDB ClientError for fdcId {fdc_id}: {e.response['Error']['Message']}")
        except Exception as e:
            logging.error(f"  ✗ Error processing or inserting fdcId {fdc_id}: {e}")

if __name__ == "__main__":
    main()
