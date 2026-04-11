import ijson
import csv
import sys
import os

# Database configs: top-level JSON key (for ijson prefix), database name, how to get subCategory
DB_CONFIGS = {
    "foundation": {
        "json_prefix": "FoundationFoods.item",
        "database": "foundation",
        "get_sub_category": lambda item: item.get("foodCategory", {}).get("description", ""),
    },
    "sr_legacy": {
        "json_prefix": "SRLegacyFoods.item",
        "database": "sr_legacy",
        "get_sub_category": lambda item: item.get("foodCategory", {}).get("description", ""),
    },
    "survey_fndds": {
        "json_prefix": "SurveyFoods.item",
        "database": "survey_fndds",
        "get_sub_category": lambda item: item.get("wweiaFoodCategory", {}).get("wweiaFoodCategoryDescription", ""),
    },
    "branded": {
        "json_prefix": "BrandedFoods.item",
        "database": "branded",
        "get_sub_category": lambda item: item.get("brandedFoodCategory", ""),
    },
}


def main():
    if len(sys.argv) != 4:
        print("Usage: python extract_csv2.py <database_type> <json_file> <fdc_ids_csv>")
        print(f"  database_type: {', '.join(DB_CONFIGS.keys())}")
        print("  json_file: path to the full database JSON file")
        print("  fdc_ids_csv: path to CSV with fdcId column")
        sys.exit(1)

    db_type = sys.argv[1]
    json_file = sys.argv[2]
    fdc_ids_file = sys.argv[3]

    if db_type not in DB_CONFIGS:
        print(f"Error: unknown database type '{db_type}'. Must be one of: {', '.join(DB_CONFIGS.keys())}")
        sys.exit(1)

    config = DB_CONFIGS[db_type]

    # Load fdcIds from CSV
    fdc_ids = set()
    with open(fdc_ids_file, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if row and row[0].strip():
                fdc_ids.add(int(row[0].strip()))
    print(f"Total fdcIds from CSV: {len(fdc_ids)}")

    # Stream JSON with ijson
    print(f"Streaming {json_file}...")
    results = []
    found_ids = set()
    total_scanned = 0

    with open(json_file, "rb") as f:
        for entry in ijson.items(f, config["json_prefix"]):
            total_scanned += 1
            fdc_id = entry.get("fdcId")
            if fdc_id in fdc_ids:
                sub_category = config["get_sub_category"](entry)
                results.append({
                    "fdcId": fdc_id,
                    "database": config["database"],
                    "subCategory": sub_category,
                })
                found_ids.add(fdc_id)

            if total_scanned % 10000 == 0:
                print(f"  scanned {total_scanned} entries, matched {len(found_ids)} so far...")

    print(f"Total scanned in JSON: {total_scanned}")
    print(f"Matched entries: {len(results)}")

    missing = fdc_ids - found_ids
    if missing:
        print(f"Warning: {len(missing)} fdcIds not found in JSON")

    # Sort and write
    results.sort(key=lambda x: x["fdcId"])
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "usda_updated_requirements")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{db_type}Items.csv")

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["fdcId", "database", "subCategory"])
        writer.writeheader()
        writer.writerows(results)

    print(f"CSV written to: {output_file}")


if __name__ == "__main__":
    main()
