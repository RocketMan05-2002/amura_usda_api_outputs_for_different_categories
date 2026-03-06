import json
import os
from pathlib import Path

# ── Source and Output Folders ────────────────────────────────────────────────
SOURCE_FOLDERS = [
    "01_foundation_foods_curl_outputs",
    "02_sr_legacy_foods_curl_outputs",
    "03_survey_fndds_foods_curl_outputs",
    "04_branded_foods_curl_outputs",
]

def simplify_nutrient(node):
    """
    Recursively simplify a nutrient node to only include required fields.
    Required: nutrient_id, name, value (from amount), unit, nutrients (nested).
    """
    simplified = {
        "nutrient_id": node.get("nutrient_id"),
        "name": node.get("name"),
        "value": node.get("amount"),
        "unit": node.get("unit"),
    }
    
    # If there are nested nutrients, process them recursively
    child_nutrients = node.get("nutrients", [])
    if child_nutrients:
        simplified["nutrients"] = [simplify_nutrient(child) for child in child_nutrients]
    
    # Remove keys with None values (like value if it's a category header)
    return {k: v for k, v in simplified.items() if v is not None or k == "nutrients"}

def process_file(src_path, dst_folder):
    """Simplify a single corrected JSON file and save to the destination."""
    with open(src_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    fdc_id = data.get("fdcId")
    description = data.get("description")
    
    # Transform nutrients
    raw_nutrients = data.get("foodNutrients", [])
    simplified_nutrients = [simplify_nutrient(n) for n in raw_nutrients]
    
    # Final simplified object
    backend_response = {
        "fdcId": fdc_id,
        "description": description,
        "foodNutrients": simplified_nutrients
    }
    
    # Create filename: <original_name_without_ext>_<fdc_id>.json
    original_stem = src_path.stem
    dst_filename = f"{original_stem}_{fdc_id}.json"
    dst_path = dst_folder / dst_filename
    
    with open(dst_path, "w", encoding="utf-8") as f:
        json.dump(backend_response, f, indent=2, ensure_ascii=False)
    
    return dst_filename

def main():
    script_dir = Path(__file__).parent.resolve()
    print(f"Working directory: {script_dir}\n")

    for folder_name in SOURCE_FOLDERS:
        src_folder = script_dir / f"corrected_{folder_name}"
        dst_folder = script_dir / f"final_backend_response_{folder_name}"
        
        if not src_folder.exists():
            print(f"⚠  Corrected folder not found, skipping: {src_folder}")
            continue
            
        dst_folder.mkdir(parents=True, exist_ok=True)
        print(f"📂  Processing: {src_folder.name} → {dst_folder.name}")
        
        json_files = sorted(src_folder.glob("*.json"))
        for json_file in json_files:
            try:
                fname = process_file(json_file, dst_folder)
                print(f"  ✓  Created: {fname}")
            except Exception as e:
                print(f"  ✗  Error processing {json_file.name}: {e}")
        print()

    print("✅  Done. All simplified backend response files generated.")

if __name__ == "__main__":
    main()
