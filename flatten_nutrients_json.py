import os
import json

BASE_DIR = "/Users/rocketman/PC/amura/amura_usda_api_outputs_for_different_categories"
INPUT_FOLDERS = [
    "final_backend_response_01_foundation_foods_curl_outputs",
    "final_backend_response_02_sr_legacy_foods_curl_outputs",
    "final_backend_response_03_survey_fndds_foods_curl_outputs",
    "final_backend_response_04_branded_foods_curl_outputs"
]

def flatten_nutrients(nutrients_list, parent_id=None, parent_name=None, flat_dict=None):
    if flat_dict is None:
        flat_dict = {}
        
    for nutrient in nutrients_list:
        nutrient_name = nutrient.get("name")
        if not nutrient_name:
            continue
            
        entry = {
            "nutrient_id": nutrient.get("nutrient_id"),
            "name": nutrient_name,
            "unit": nutrient.get("unit")
        }
        
        # Include value if it exists (some categories like "Proximates" don't have a value)
        if "value" in nutrient:
            entry["value"] = nutrient["value"]
            
        # Attach the parent information if this is a nested nutrient
        if parent_id is not None:
            entry["parent_nutrient_id"] = parent_id
            entry["parent_nutrient_name"] = parent_name
            
        # Add to flattened dictionary using the nutrient name in lower case as key parameter 
        flat_dict[nutrient_name.lower()] = entry
        
        # Make the recursive call to process sub-nutrients
        if "nutrients" in nutrient:
            flatten_nutrients(nutrient["nutrients"], nutrient.get("nutrient_id"), nutrient_name, flat_dict)
            
    return flat_dict

def main():
    for in_folder in INPUT_FOLDERS:
        in_folder_path = os.path.join(BASE_DIR, in_folder)
        if not os.path.exists(in_folder_path):
            print(f"Directory not found, skipping: {in_folder_path}")
            continue
            
        # Create output folders namely resolved_backend_json_<folder_name>
        out_folder_name = f"resolved_backend_json_{in_folder}"
        out_folder_path = os.path.join(BASE_DIR, out_folder_name)
        os.makedirs(out_folder_path, exist_ok=True)
        
        count = 0
        for filename in os.listdir(in_folder_path):
            if not filename.endswith(".json"):
                continue
                
            in_file_path = os.path.join(in_folder_path, filename)
            out_file_path = os.path.join(out_folder_path, filename)
            
            with open(in_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            out_data = {
                "fdcId": data.get("fdcId")
            }
            if "description" in data:
                out_data["description"] = data.get("description")
                
            food_nutrients = data.get("foodNutrients", [])
            flat_nutrients = flatten_nutrients(food_nutrients)
            
            # Populate our flat output dictionary with the extracted nested nutrients
            for k, v in flat_nutrients.items():
                out_data[k] = v
                
            with open(out_file_path, "w", encoding="utf-8") as f:
                json.dump(out_data, f, indent=2)
                
            count += 1
            
        print(f"Processed {count} files from '{in_folder}' -> '{out_folder_name}'")

if __name__ == "__main__":
    main()
