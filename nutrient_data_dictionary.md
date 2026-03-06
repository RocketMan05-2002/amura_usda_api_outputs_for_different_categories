# Nutrient Data Dictionary

This document defines the variables and structure used in the nested nutrient JSON files.

## Top-Level Fields

| Field             | Type    | Description                                                              |
| ----------------- | ------- | ------------------------------------------------------------------------ |
| `fdcId`           | Integer | Unique identifier for the food in the FoodData Central (FDC) database.   |
| `description`     | String  | Commercial or common name of the food.                                   |
| `dataType`        | String  | Category of data (e.g., Foundation, SR Legacy, Survey (FNDDS), Branded). |
| `publicationDate` | String  | Date when the data was published on FDC.                                 |
| `foodNutrients`   | List    | Nested collection of nutrients, organized by hierarchy.                  |

## Nutrient Object Fields

Each entry in the `foodNutrients` list (including nested ones) contains:

| Field          | Type    | Description                                                             |
| -------------- | ------- | ----------------------------------------------------------------------- |
| `nutrient_id`  | Integer | Unique identifier for the specific nutrient.                            |
| `name`         | String  | Common name of the nutrient (e.g., Protein, Vitamin C).                 |
| `unit`         | String  | Unit of measurement (e.g., G, MG, UG, KCAL).                            |
| `nutrient_nbr` | String  | USDA nutrient number used for internal references.                      |
| `group`        | String  | Theoretical group the nutrient belongs to (e.g., Proximates, Minerals). |
| `amount`       | Float   | The quantity of the nutrient per 100g of food.                          |
| `nutrients`    | List    | (Optional) Nested list of child nutrients or sub-categories.            |

### Optional Precision Fields (Found in some source files)

- `min`: The minimum observed value.
- `max`: The maximum observed value.
- `median`: The median observed value.
- `dataPoints`: Number of samples used for the analysis.
- `derivation`: Information about how the value was obtained (e.g., Analytical, Calculated).
