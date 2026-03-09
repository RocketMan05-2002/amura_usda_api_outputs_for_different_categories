"""
extract_foundation_fdc_ids.py

Reads all 4 FoodData Central database JSON files and extracts the top-level
`fdcId` from every food entry. Writes one output JSON per database into:

    all_4_databases_fdcIds/
        branded_food_fdc_ids.json
        foundation_food_fdc_ids.json
        sr_legacy_food_fdc_ids.json
        survey_food_fdc_ids.json

Each output JSON is a flat key-value mapping:
    {
        "<fdcId>": <fdcId as integer>,
        ...
    }
"""

import json
import os

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(BASE_DIR, "z_entire_db_jsons")
OUTPUT_DIR = os.path.join(BASE_DIR, "all_4_databases_fdcIds")

# Each entry: (source filename, top-level JSON array key, output filename)
DATABASES = [
    (
        "FoodData_Central_branded_food_json_2025-12-18 2.json",
        "BrandedFoods",
        "branded_food_fdc_ids.json",
    ),
    (
        "FoodData_Central_foundation_food_json_2025-12-18.json",
        "FoundationFoods",
        "foundation_food_fdc_ids.json",
    ),
    (
        "FoodData_Central_sr_legacy_food_json_2018-04.json",
        "SRLegacyFoods",
        "sr_legacy_food_fdc_ids.json",
    ),
    (
        "surveyDownload.json",
        "SurveyFoods",
        "survey_food_fdc_ids.json",
    ),
]
# ──────────────────────────────────────────────────────────────────────────────


def extract_fdc_ids(input_path: str, array_key: str) -> dict:
    """
    Parse a FoodData Central JSON file and return a dict mapping
    str(fdcId) -> fdcId (int) for every top-level food entry.
    """
    print(f"  Reading: {os.path.basename(input_path)}")
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    foods   = data.get(array_key, [])
    missing = 0
    fdc_map = {}

    for food in foods:
        fdc_id = food.get("fdcId")
        if fdc_id is not None:
            fdc_map[str(fdc_id)] = fdc_id
        else:
            missing += 1

    print(f"  Entries : {len(foods):>7,}  |  fdcIds extracted: {len(fdc_map):>7,}  |  missing: {missing}")
    return fdc_map


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}\n")

    for src_file, array_key, out_file in DATABASES:
        input_path  = os.path.join(INPUT_DIR, src_file)
        output_path = os.path.join(OUTPUT_DIR, out_file)

        print(f"[{array_key}]")
        fdc_map = extract_fdc_ids(input_path, array_key)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(fdc_map, f, indent=2)

        print(f"  Saved  : {out_file}\n")

    print("Done. All 4 fdcId JSON files written to 'all_4_databases_fdcIds/'.")


if __name__ == "__main__":
    main()
