"""
transform_nutrients.py
======================
Reads raw USDA FDC API JSON files from 4 food-type folders and outputs
new JSON files with nutrients restructured into the correct nested hierarchy.

Expected folder layout (relative to this script):
    01_foundation_foods_curl_outputs/       ← 10 JSON files
    sr_legacy/              ← 10 JSON files
    surveyfndds/            ← 10 JSON files
    branded_foods/          ← 10 JSON files

Output:
    corrected_foundation_foods/
    corrected_sr_legacy/
    corrected_surveyfndds/
    corrected_branded_foods/

Usage:
    python transform_nutrients.py

Requirements:
    Python 3.8+  (no third-party packages needed)
"""

import json
import os
import copy
from pathlib import Path

# ── Folder names to process ───────────────────────────────────────────────────
SOURCE_FOLDERS = [
    "01_foundation_foods_curl_outputs",
    "02_sr_legacy_foods_curl_outputs",
    "03_survey_fndds_foods_curl_outputs",
    "04_branded_foods_curl_outputs",
]

# ── Full nutrient hierarchy definition ───────────────────────────────────────
# Each entry: (nutrient_id, nutrient_name, unit, nutrient_nbr, parent_id, nesting_level, group)
# parent_id = None means this is a top-level section header
# nutrient_id is stored as int for matching against API response

HIERARCHY = [
    # ── PROXIMATES ──────────────────────────────────────────────────────────
    (2045,"Proximates","G","951",None,0,"Proximates"),
    (1051,"Water","G","255",2045,1,"Proximates"),
    (1001,"Solids","G","201",2045,1,"Proximates"),
    (1049,"Solids, non-fat","G","253",1001,2,"Proximates"),
    (1064,"Solids, soluble","G","271",1001,2,"Proximates"),
    (2047,"Energy (Atwater General Factors)","KCAL","957",2045,1,"Proximates"),
    (2048,"Energy (Atwater Specific Factors)","KCAL","958",2045,1,"Proximates"),
    (1008,"Energy","KCAL","208",2045,1,"Proximates"),
    (1062,"Energy","kJ","268",2045,1,"Proximates"),
    (1002,"Nitrogen","G","202",2045,1,"Proximates"),
    (1052,"Adjusted Nitrogen","G","256",1002,2,"Proximates"),
    (1003,"Protein","G","203",2045,1,"Proximates"),
    (1053,"Adjusted Protein","G","257",2045,1,"Proximates"),
    (1004,"Total lipid (fat)","G","204",2045,1,"Proximates"),
    (1085,"Total fat (NLEA)","G","298",2045,1,"Proximates"),
    (1007,"Ash","G","207",2045,1,"Proximates"),
    (2027,"Proximate","G","200",2045,1,"Proximates"),
    (1023,"pH","PH","226",2045,1,"Proximates"),
    (1024,"Specific Gravity","SP_GR","227",2045,1,"Proximates"),
    # Carbohydrates (child of Proximates)
    (2039,"Carbohydrates","G","956",2045,1,"Carbohydrates"),
    (1005,"Carbohydrate, by difference","G","205",2039,2,"Carbohydrates"),
    (1050,"Carbohydrate, by summation","G","205.2",2039,2,"Carbohydrates"),
    (1072,"Carbohydrate, other","G","284",2039,2,"Carbohydrates"),
    (1079,"Fiber, total dietary","G","291",2039,2,"Carbohydrates"),
    (1082,"Fiber, soluble","G","295",1079,3,"Carbohydrates"),
    (1084,"Fiber, insoluble","G","297",1079,3,"Carbohydrates"),
    (2033,"Total dietary fiber (AOAC 2011.25)","G","293",1079,3,"Carbohydrates"),
    (2038,"High Molecular Weight Dietary Fiber (HMWDF)","G","293.3",2033,4,"Carbohydrates"),
    (2065,"Low Molecular Weight Dietary Fiber (LMWDF)","G","293.4",2033,4,"Carbohydrates"),
    (2034,"Insoluble dietary fiber (IDF)","G","293.1",2033,4,"Carbohydrates"),
    (2035,"Soluble dietary fiber (SDFP+SDFS)","G","293.2",2033,4,"Carbohydrates"),
    (2036,"Soluble dietary fiber (SDFP)","G","954",2035,5,"Carbohydrates"),
    (2037,"Soluble dietary fiber (SDFS)","G","953",2035,5,"Carbohydrates"),
    (2058,"Beta-glucan","G","",1079,3,"Carbohydrates"),
    (1006,"Fiber, crude (DO NOT USE - Archived)","G","206",1079,3,"Carbohydrates"),
    (1066,"Fiber, neutral detergent (DO NOT USE - Archived)","G","273",1079,3,"Carbohydrates"),
    (1009,"Starch","G","209",2039,2,"Carbohydrates"),
    (1071,"Resistant starch","G","283",1009,3,"Carbohydrates"),
    (1065,"Glycogen","G","272",1009,3,"Carbohydrates"),
    (1015,"Amylose","G","218",1009,3,"Carbohydrates"),
    (1016,"Amylopectin","G","219",1009,3,"Carbohydrates"),
    (1403,"Inulin","G","806",2039,2,"Carbohydrates"),
    (1017,"Pectin","G","220",2039,2,"Carbohydrates"),
    (1070,"Nonstarch polysaccharides","G","282",2039,2,"Carbohydrates"),
    (1080,"Lignin","G","292",2039,2,"Carbohydrates"),
    (1063,"Sugars, Total","G","269.3",2039,2,"Carbohydrates"),
    (2000,"Total Sugars","G","269",2039,2,"Carbohydrates"),
    (1236,"Sugars, intrinsic","G","549",2000,3,"Carbohydrates"),
    (1235,"Sugars, added","G","539",2000,3,"Carbohydrates"),
    (1067,"Reducing sugars","G","274",2000,3,"Carbohydrates"),
    (1010,"Sucrose","G","210",2000,3,"Carbohydrates"),
    (1011,"Glucose","G","211",2000,3,"Carbohydrates"),
    (1012,"Fructose","G","212",2000,3,"Carbohydrates"),
    (1013,"Lactose","G","213",2000,3,"Carbohydrates"),
    (1014,"Maltose","G","214",2000,3,"Carbohydrates"),
    (1075,"Galactose","G","287",2000,3,"Carbohydrates"),
    (1399,"Mannose","G","801",2000,3,"Carbohydrates"),
    (1400,"Triose","G","803",2000,3,"Carbohydrates"),
    (1401,"Tetrose","G","804",2000,3,"Carbohydrates"),
    (1402,"Other Saccharides","G","805",2000,3,"Carbohydrates"),
    (2064,"Oligosaccharides","MG","",2039,2,"Carbohydrates"),
    (1076,"Raffinose","G","288",2064,3,"Carbohydrates"),
    (1077,"Stachyose","G","289",2064,3,"Carbohydrates"),
    (2063,"Verbascose","G","",2064,3,"Carbohydrates"),
    (1069,"Oligosaccharides","G","281",2064,3,"Carbohydrates"),
    (1086,"Total sugar alcohols","G","299",2039,2,"Carbohydrates"),
    (1055,"Mannitol","G","260",1086,3,"Carbohydrates"),
    (1056,"Sorbitol","G","261",1086,3,"Carbohydrates"),
    (1078,"Xylitol","G","290",1086,3,"Carbohydrates"),
    (1181,"Inositol","MG","422",1086,3,"Carbohydrates"),
    (1019,"Pentosan","G","222",2039,2,"Carbohydrates"),
    (1020,"Pentoses","G","223",2039,2,"Carbohydrates"),
    (1073,"Arabinose","G","285",1020,3,"Carbohydrates"),
    (1074,"Xylose","G","286",1020,3,"Carbohydrates"),
    (1081,"Ribose","G","294",1020,3,"Carbohydrates"),
    (1021,"Hemicellulose","G","224",2039,2,"Carbohydrates"),
    (1022,"Cellulose","G","225",2039,2,"Carbohydrates"),
    (1068,"Beta-glucans","G","276",2039,2,"Carbohydrates"),
    # Organic Acids (child of Proximates)
    (1025,"Organic acids","G","229",2045,1,"Organic Acids"),
    (1026,"Acetic acid","MG","230",1025,2,"Organic Acids"),
    (1027,"Aconitic acid","MG","231",1025,2,"Organic Acids"),
    (1028,"Benzoic acid","MG","232",1025,2,"Organic Acids"),
    (1029,"Chelidonic acid","MG","233",1025,2,"Organic Acids"),
    (1030,"Chlorogenic acid","MG","234",1025,2,"Organic Acids"),
    (1031,"Cinnamic acid","MG","235",1025,2,"Organic Acids"),
    (1032,"Citric acid","MG","236",1025,2,"Organic Acids"),
    (1033,"Fumaric acid","MG","237",1025,2,"Organic Acids"),
    (1034,"Galacturonic acid","MG","238",1025,2,"Organic Acids"),
    (1035,"Gallic acid","MG","239",1025,2,"Organic Acids"),
    (1036,"Glycolic acid","MG","240",1025,2,"Organic Acids"),
    (1037,"Isocitric acid","MG","241",1025,2,"Organic Acids"),
    (1038,"Lactic acid","MG","242",1025,2,"Organic Acids"),
    (1039,"Malic acid","MG","243",1025,2,"Organic Acids"),
    (1040,"Oxaloacetic acid","MG","244",1025,2,"Organic Acids"),
    (1041,"Oxalic acid","MG","245",1025,2,"Organic Acids"),
    (1042,"Phytic acid","MG","246",1025,2,"Organic Acids"),
    (1043,"Pyruvic acid","MG","247",1025,2,"Organic Acids"),
    (1044,"Quinic acid","MG","248",1025,2,"Organic Acids"),
    (1045,"Salicylic acid","MG","249",1025,2,"Organic Acids"),
    (1046,"Succinic acid","MG","250",1025,2,"Organic Acids"),
    (1047,"Tartaric acid","MG","251",1025,2,"Organic Acids"),
    (1048,"Ursolic acid","MG","252",1025,2,"Organic Acids"),
    # ── MINERALS ────────────────────────────────────────────────────────────
    (2043,"Minerals","G","300",None,0,"Minerals"),
    (1087,"Calcium, Ca","MG","301",2043,1,"Minerals"),
    (1239,"Calcium, intrinsic","MG","561",1087,2,"Minerals"),
    (1237,"Calcium, added","MG","551",1087,2,"Minerals"),
    (1089,"Iron, Fe","MG","303",2043,1,"Minerals"),
    (1141,"Iron, heme","MG","364",1089,2,"Minerals"),
    (1142,"Iron, non-heme","MG","365",1089,2,"Minerals"),
    (1240,"Iron, intrinsic","MG","563",1089,2,"Minerals"),
    (1238,"Iron, added","MG","553",1089,2,"Minerals"),
    (1090,"Magnesium, Mg","MG","304",2043,1,"Minerals"),
    (1091,"Phosphorus, P","MG","305",2043,1,"Minerals"),
    (1092,"Potassium, K","MG","306",2043,1,"Minerals"),
    (1093,"Sodium, Na","MG","307",2043,1,"Minerals"),
    (1149,"Salt, NaCl","MG","375",1093,2,"Minerals"),
    (1095,"Zinc, Zn","MG","309",2043,1,"Minerals"),
    (1098,"Copper, Cu","MG","312",2043,1,"Minerals"),
    (1101,"Manganese, Mn","MG","315",2043,1,"Minerals"),
    (1100,"Iodine, I","UG","314",2043,1,"Minerals"),
    (1103,"Selenium, Se","UG","317",2043,1,"Minerals"),
    (1099,"Fluoride, F","UG","313",2043,1,"Minerals"),
    (1148,"Fluoride - DO NOT USE; use 313","UG","374",1099,2,"Minerals"),
    (1094,"Sulfur, S","MG","308",2043,1,"Minerals"),
    (1146,"Nickel, Ni","UG","371",2043,1,"Minerals"),
    (1102,"Molybdenum, Mo","UG","316",2043,1,"Minerals"),
    (1097,"Cobalt, Co","UG","311",2043,1,"Minerals"),
    (1137,"Boron, B","UG","354",2043,1,"Minerals"),
    (1088,"Chlorine, Cl","MG","302",2043,1,"Minerals"),
    (1096,"Chromium, Cr","UG","310",2043,1,"Minerals"),
    (1132,"Aluminum, Al","UG","348",2043,1,"Minerals"),
    (1133,"Antimony, Sb","UG","349",2043,1,"Minerals"),
    (1134,"Arsenic, As","UG","350",2043,1,"Minerals"),
    (1135,"Barium, Ba","UG","351",2043,1,"Minerals"),
    (1136,"Beryllium, Be","UG","352",2043,1,"Minerals"),
    (1138,"Bromine, Br","UG","355",2043,1,"Minerals"),
    (1139,"Cadmium, Cd","UG","356",2043,1,"Minerals"),
    (1140,"Gold, Au","UG","363",2043,1,"Minerals"),
    (1143,"Lead, Pb","UG","367",2043,1,"Minerals"),
    (1144,"Lithium, Li","UG","368",2043,1,"Minerals"),
    (1145,"Mercury, Hg","UG","370",2043,1,"Minerals"),
    (1147,"Rubidium, Rb","UG","373",2043,1,"Minerals"),
    (1150,"Silicon, Si","UG","378",2043,1,"Minerals"),
    (1151,"Silver, Ag","UG","379",2043,1,"Minerals"),
    (1152,"Strontium, Sr","UG","380",2043,1,"Minerals"),
    (1153,"Tin, Sn","UG","385",2043,1,"Minerals"),
    (1154,"Titanium, Ti","UG","386",2043,1,"Minerals"),
    (1155,"Vanadium, V","UG","389",2043,1,"Minerals"),
    (1059,"Nitrates","MG","264",2043,1,"Minerals"),
    (1060,"Nitrites","MG","265",2043,1,"Minerals"),
    (1061,"Nitrosamine,total","MG","266",2043,1,"Minerals"),
    # ── VITAMINS ────────────────────────────────────────────────────────────
    (2046,"Vitamins and Other Components","G","952",None,0,"Vitamins"),
    (1162,"Vitamin C, total ascorbic acid","MG","401",2046,1,"Vitamins"),
    (1163,"Vitamin C, reduced ascorbic acid","MG","402",1162,2,"Vitamins"),
    (1164,"Vitamin C, dehydro ascorbic acid","MG","403",1162,2,"Vitamins"),
    (1247,"Vitamin C, intrinsic","MG","581",1162,2,"Vitamins"),
    (1241,"Vitamin C, added","MG","571",1162,2,"Vitamins"),
    (1165,"Thiamin","MG","404",2046,1,"Vitamins"),
    (1249,"Thiamin, intrinsic","MG","584",1165,2,"Vitamins"),
    (1243,"Thiamin, added","MG","574",1165,2,"Vitamins"),
    (1166,"Riboflavin","MG","405",2046,1,"Vitamins"),
    (1250,"Riboflavin, intrinsic","MG","585",1166,2,"Vitamins"),
    (1244,"Riboflavin, added","MG","575",1166,2,"Vitamins"),
    (1167,"Niacin","MG","406",2046,1,"Vitamins"),
    (1168,"Niacin from tryptophan, determined","MG","407",1167,2,"Vitamins"),
    (1169,"Niacin equivalent N406 +N407","MG","409",1167,2,"Vitamins"),
    (1251,"Niacin, intrinsic","MG","586",1167,2,"Vitamins"),
    (1245,"Niacin, added","MG","576",1167,2,"Vitamins"),
    (1170,"Pantothenic acid","MG","410",2046,1,"Vitamins"),
    (1175,"Vitamin B-6","MG","415",2046,1,"Vitamins"),
    (1171,"Vitamin B-6, pyridoxine, alcohol form","MG","411",1175,2,"Vitamins"),
    (1172,"Vitamin B-6, pyridoxal, aldehyde form","MG","412",1175,2,"Vitamins"),
    (1173,"Vitamin B-6, pyridoxamine, amine form","MG","413",1175,2,"Vitamins"),
    (1174,"Vitamin B-6, N411 + N412 +N413","MG","414",1175,2,"Vitamins"),
    (1176,"Biotin","UG","416",2046,1,"Vitamins"),
    (1177,"Folate, total","UG","417",2046,1,"Vitamins"),
    (1179,"Folate, free","UG","419",1177,2,"Vitamins"),
    (1186,"Folic acid","UG","431",1177,2,"Vitamins"),
    (1187,"Folate, food","UG","432",1177,2,"Vitamins"),
    (1190,"Folate, DFE","UG","435",1177,2,"Vitamins"),
    (1189,"Folate, not 5-MTHF","UG","434",1177,2,"Vitamins"),
    (1188,"5-methyl tetrahydrofolate (5-MTHF)","UG","433",1177,2,"Vitamins"),
    (1191,"10-Formyl folic acid (10HCOFA)","UG","436",1177,2,"Vitamins"),
    (1192,"5-Formyltetrahydrofolic acid (5-HCOH4","UG","437",1177,2,"Vitamins"),
    (1193,"Tetrahydrofolic acid (THF)","UG","438",1177,2,"Vitamins"),
    (1180,"Choline, total","MG","421",2046,1,"Vitamins"),
    (1194,"Choline, free","MG","450",1180,2,"Vitamins"),
    (1195,"Choline, from phosphocholine","MG","451",1180,2,"Vitamins"),
    (1196,"Choline, from phosphotidyl choline","MG","452",1180,2,"Vitamins"),
    (1197,"Choline, from glycerophosphocholine","MG","453",1180,2,"Vitamins"),
    (1199,"Choline, from sphingomyelin","MG","455",1180,2,"Vitamins"),
    (1182,"Inositol phosphate","MG","423",1180,2,"Vitamins"),
    (1198,"Betaine","MG","454",2046,1,"Vitamins"),
    (1178,"Vitamin B-12","UG","418",2046,1,"Vitamins"),
    (1252,"Vitamin B-12, intrinsic","UG","588",1178,2,"Vitamins"),
    (1246,"Vitamin B-12, added","UG","578",1178,2,"Vitamins"),
    (1106,"Vitamin A, RAE","UG","320",2046,1,"Vitamins"),
    (1105,"Retinol","UG","319",1106,2,"Vitamins"),
    (2067,"Vitamin A","UG","960",1106,2,"Vitamins"),
    (1107,"Carotene, beta","UG","321",1106,2,"Vitamins"),
    (1159,"cis-beta-Carotene","UG","321.1",1107,3,"Vitamins"),
    (2028,"trans-beta-Carotene","UG","321.2",1107,3,"Vitamins"),
    (1108,"Carotene, alpha","UG","322",1106,2,"Vitamins"),
    (1118,"Carotene, gamma","UG","332",1106,2,"Vitamins"),
    (1120,"Cryptoxanthin, beta","UG","334",1106,2,"Vitamins"),
    (2032,"Cryptoxanthin, alpha","UG","335",1106,2,"Vitamins"),
    (1104,"Vitamin A, IU","IU","318",2046,1,"Vitamins"),
    (1156,"Vitamin A, RE","MCG_RE","392",2046,1,"Vitamins"),
    (1157,"Carotene","MCG_RE","393",2046,1,"Vitamins"),
    (2040,"Other carotenoids","UG","955",2046,1,"Vitamins"),
    (1122,"Lycopene","UG","337",2040,2,"Vitamins"),
    (1160,"cis-Lycopene","UG","337.1",1122,3,"Vitamins"),
    (2029,"trans-Lycopene","UG","337.2",1122,3,"Vitamins"),
    (1123,"Lutein + zeaxanthin","UG","338",2040,2,"Vitamins"),
    (1161,"cis-Lutein/Zeaxanthin","UG","338.3",1123,3,"Vitamins"),
    (1121,"Lutein","UG","338.1",1123,3,"Vitamins"),
    (1119,"Zeaxanthin","UG","338.2",1123,3,"Vitamins"),
    (1116,"Phytoene","UG","330",2040,2,"Vitamins"),
    (1117,"Phytofluene","UG","331",2040,2,"Vitamins"),
    (2041,"Tocopherols and tocotrienols","MG","323.99",2046,1,"Vitamins"),
    (2055,"Total Tocopherols","MG","",2041,2,"Vitamins"),
    (1109,"Vitamin E (alpha-tocopherol)","MG","323",2055,3,"Vitamins"),
    (1242,"Vitamin E, added","MG","573",1109,4,"Vitamins"),
    (1248,"Vitamin E, intrinsic","MG","583",1109,4,"Vitamins"),
    (1125,"Tocopherol, beta","MG","341",2055,3,"Vitamins"),
    (1126,"Tocopherol, gamma","MG","342",2055,3,"Vitamins"),
    (1127,"Tocopherol, delta","MG","343",2055,3,"Vitamins"),
    (1158,"Vitamin E","MG_ATE","394",2041,2,"Vitamins"),
    (2068,"Vitamin E","MG","959",2041,2,"Vitamins"),
    (1124,"Vitamin E (label entry primarily)","IU","340",2041,2,"Vitamins"),
    (2054,"Total Tocotrienols","MG","",2041,2,"Vitamins"),
    (1128,"Tocotrienol, alpha","MG","344",2054,3,"Vitamins"),
    (1129,"Tocotrienol, beta","MG","345",2054,3,"Vitamins"),
    (1130,"Tocotrienol, gamma","MG","346",2054,3,"Vitamins"),
    (1131,"Tocotrienol, delta","MG","347",2054,3,"Vitamins"),
    (1110,"Vitamin D (D2 + D3), International Units","IU","324",2046,1,"Vitamins"),
    (1114,"Vitamin D (D2 + D3)","UG","328",2046,1,"Vitamins"),
    (1111,"Vitamin D2 (ergocalciferol)","UG","325",1114,2,"Vitamins"),
    (1112,"Vitamin D3 (cholecalciferol)","UG","326",1114,2,"Vitamins"),
    (1113,"25-hydroxycholecalciferol","UG","327",1114,2,"Vitamins"),
    (2059,"Vitamin D4","UG","",1114,2,"Vitamins"),
    (1115,"25-hydroxyergocalciferol","UG","329",1114,2,"Vitamins"),
    (1185,"Vitamin K (phylloquinone)","UG","430",2046,1,"Vitamins"),
    (1184,"Vitamin K (Dihydrophylloquinone)","UG","429",2046,1,"Vitamins"),
    (1183,"Vitamin K (Menaquinone-4)","UG","428",2046,1,"Vitamins"),
    (2069,"Glutathione","MG","961",2046,1,"Vitamins"),
    (2057,"Ergothioneine","MG","",2046,1,"Vitamins"),
    (1054,"Piperine","G","259",2046,1,"Vitamins"),
    (1083,"Theophylline","MG","296",2046,1,"Vitamins"),
    # ── LIPIDS ──────────────────────────────────────────────────────────────
    (2044,"Lipids","G","950",None,0,"Lipids"),
    (1258,"Fatty acids, total saturated","G","606",2044,1,"Lipids"),
    (1326,"Fatty acids, total sat., NLEA","G","690",1258,2,"Lipids"),
    (1318,"Fatty acids, saturated, other","G","677",1258,2,"Lipids"),
    (1259,"SFA 4:0","G","607",1258,2,"Lipids"),
    (2003,"SFA 5:0","G","632",1258,2,"Lipids"),
    (1260,"SFA 6:0","G","608",1258,2,"Lipids"),
    (2004,"SFA 7:0","G","633",1258,2,"Lipids"),
    (1261,"SFA 8:0","G","609",1258,2,"Lipids"),
    (2005,"SFA 9:0","G","634",1258,2,"Lipids"),
    (1262,"SFA 10:0","G","610",1258,2,"Lipids"),
    (1335,"SFA 11:0","G","699",1258,2,"Lipids"),
    (1263,"SFA 12:0","G","611",1258,2,"Lipids"),
    (1332,"SFA 13:0","G","696",1258,2,"Lipids"),
    (1264,"SFA 14:0","G","612",1258,2,"Lipids"),
    (1299,"SFA 15:0","G","652",1258,2,"Lipids"),
    (1265,"SFA 16:0","G","613",1258,2,"Lipids"),
    (1300,"SFA 17:0","G","653",1258,2,"Lipids"),
    (1266,"SFA 18:0","G","614",1258,2,"Lipids"),
    (1322,"SFA 19:0","G","686",1258,2,"Lipids"),
    (1267,"SFA 20:0","G","615",1258,2,"Lipids"),
    (2006,"SFA 21:0","G","681",1258,2,"Lipids"),
    (1273,"SFA 22:0","G","624",1258,2,"Lipids"),
    (2007,"SFA 23:0","G","682",1258,2,"Lipids"),
    (1301,"SFA 24:0","G","654",1258,2,"Lipids"),
    (1292,"Fatty acids, total monounsaturated","G","645",2044,1,"Lipids"),
    (1327,"Fatty acids, total monounsat., NLEA","G","691",1292,2,"Lipids"),
    (1319,"Fatty acids, monounsat., other","G","678",1292,2,"Lipids"),
    (2008,"MUFA 12:1","G","635",1292,2,"Lipids"),
    (1274,"MUFA 14:1","G","625",1292,2,"Lipids"),
    (2009,"MUFA 14:1 c","G","822",1274,3,"Lipids"),
    (1333,"MUFA 15:1","G","697",1292,2,"Lipids"),
    (1275,"MUFA 16:1","G","626",1292,2,"Lipids"),
    (1314,"MUFA 16:1 c","G","673",1275,3,"Lipids"),
    (1323,"MUFA 17:1","G","687",1292,2,"Lipids"),
    (2010,"MUFA 17:1 c","G","825",1323,3,"Lipids"),
    (1268,"MUFA 18:1","G","617",1292,2,"Lipids"),
    (1315,"MUFA 18:1 c","G","674",1268,3,"Lipids"),
    (1413,"MUFA 18:1-11 c (18:1c n-7)","G","860",1268,3,"Lipids"),
    (1412,"MUFA 18:1-11 t (18:1t n-7)","G","859",1268,3,"Lipids"),
    (1277,"MUFA 20:1","G","628",1292,2,"Lipids"),
    (2012,"MUFA 20:1 c","G","829",1277,3,"Lipids"),
    (1279,"MUFA 22:1","G","630",1292,2,"Lipids"),
    (1317,"MUFA 22:1 c","G","676",1279,3,"Lipids"),
    (2014,"MUFA 22:1 n-9","G","676.1",1279,3,"Lipids"),
    (2015,"MUFA 22:1 n-11","G","676.2",1279,3,"Lipids"),
    (1312,"MUFA 24:1 c","G","671",1292,2,"Lipids"),
    (1293,"Fatty acids, total polyunsaturated","G","646",2044,1,"Lipids"),
    (1328,"Fatty acids, total polyunsat., NLEA","G","692",1293,2,"Lipids"),
    (1320,"Fatty acids, polyunsat., other","G","679",1293,2,"Lipids"),
    (1291,"Fatty acids, other than 607-615, 617-621, 624-632, 652-654, 686-689)","G","644",1293,2,"Lipids"),
    (1324,"PUFA 16:2","G","688",1293,2,"Lipids"),
    (1269,"PUFA 18:2","G","618",1293,2,"Lipids"),
    (2016,"PUFA 18:2 c","G","831",1269,3,"Lipids"),
    (1316,"PUFA 18:2 n-6 c,c","G","675",1269,3,"Lipids"),
    (1311,"PUFA 18:2 CLAs","G","670",1269,3,"Lipids"),
    (1307,"PUFA 18:2 i","G","666",1269,3,"Lipids"),
    (1309,"PUFA 18:2 c,t","G","668",1269,3,"Lipids"),
    (1308,"PUFA 18:2 t,c","G","667",1269,3,"Lipids"),
    (1270,"PUFA 18:3","G","619",1293,2,"Lipids"),
    (2018,"PUFA 18:3 c","G","833",1270,3,"Lipids"),
    (1404,"PUFA 18:3 n-3 c,c,c (ALA)","G","851",1270,3,"Lipids"),
    (1321,"PUFA 18:3 n-6 c,c,c","G","685",1270,3,"Lipids"),
    (1409,"PUFA 18:3i","G","856",1270,3,"Lipids"),
    (1276,"PUFA 18:4","G","627",1293,2,"Lipids"),
    (2026,"PUFA 20:2 c","G","840",1293,2,"Lipids"),
    (1313,"PUFA 20:2 n-6 c,c","G","672",1293,2,"Lipids"),
    (1325,"PUFA 20:3","G","689",1293,2,"Lipids"),
    (2020,"PUFA 20:3 c","G","835",1325,3,"Lipids"),
    (1405,"PUFA 20:3 n-3","G","852",1325,3,"Lipids"),
    (1406,"PUFA 20:3 n-6","G","853",1325,3,"Lipids"),
    (1414,"PUFA 20:3 n-9","G","861",1325,3,"Lipids"),
    (2021,"PUFA 22:3","G","683",1293,2,"Lipids"),
    (1271,"PUFA 20:4","G","620",1293,2,"Lipids"),
    (2022,"PUFA 20:4c","G","836",1271,3,"Lipids"),
    (1407,"PUFA 20:4 n-3","G","854",1271,3,"Lipids"),
    (1408,"PUFA 20:4 n-6","G","855",1271,3,"Lipids"),
    (2023,"PUFA 20:5c","G","837",1293,2,"Lipids"),
    (1278,"PUFA 20:5 n-3 (EPA)","G","629",1293,2,"Lipids"),
    (1334,"PUFA 22:2","G","698",1293,2,"Lipids"),
    (1410,"PUFA 21:5","G","857",1293,2,"Lipids"),
    (2024,"PUFA 22:5 c","G","838",1293,2,"Lipids"),
    (1411,"PUFA 22:4","G","858",1293,2,"Lipids"),
    (1280,"PUFA 22:5 n-3 (DPA)","G","631",1293,2,"Lipids"),
    (2025,"PUFA 22:6 c","G","839",1293,2,"Lipids"),
    (1272,"PUFA 22:6 n-3 (DHA)","G","621",1293,2,"Lipids"),
    (1257,"Fatty acids, total trans","G","605",2044,1,"Lipids"),
    (1329,"Fatty acids, total trans-monoenoic","G","693",1257,2,"Lipids"),
    (1281,"TFA 14:1 t","G","821",1329,3,"Lipids"),
    (1303,"TFA 16:1 t","G","662",1329,3,"Lipids"),
    (1304,"TFA 18:1 t","G","663",1329,3,"Lipids"),
    (2011,"TFA 17:1 t","G","826",1329,3,"Lipids"),
    (2013,"TFA 20:1 t","G","830",1329,3,"Lipids"),
    (1305,"TFA 22:1 t","G","664",1329,3,"Lipids"),
    (1330,"Fatty acids, total trans-dienoic","G","694",1257,2,"Lipids"),
    (1306,"TFA 18:2 t not further defined","G","665",1330,3,"Lipids"),
    (2017,"TFA 18:2 t","G","832",1330,3,"Lipids"),
    (1310,"TFA 18:2 t,t","G","669",1330,3,"Lipids"),
    (1331,"Fatty acids, total trans-polyenoic","G","695",1257,2,"Lipids"),
    (2019,"TFA 18:3 t","G","834",1331,3,"Lipids"),
    (1253,"Cholesterol","MG","601",2044,1,"Lipids"),
    (1283,"Phytosterols","MG","636",2044,1,"Lipids"),
    (2053,"Stigmastadiene","MG","",1283,2,"Lipids"),
    (1285,"Stigmasterol","MG","638",1283,2,"Lipids"),
    (1286,"Campesterol","MG","639",1283,2,"Lipids"),
    (1287,"Brassicasterol","MG","640",1283,2,"Lipids"),
    (1288,"Beta-sitosterol","MG","641",1283,2,"Lipids"),
    (2060,"Ergosta-7-enol","MG","",1283,2,"Lipids"),
    (2061,"Ergosta-7,22-dienol","MG","",1283,2,"Lipids"),
    (2062,"Ergosta-5,7-dienol","MG","",1283,2,"Lipids"),
    (1284,"Ergosterol","MG","637",1283,2,"Lipids"),
    (1289,"Campestanol","MG","642",1283,2,"Lipids"),
    (1294,"Beta-sitostanol","MG","647",1283,2,"Lipids"),
    (1295,"Delta-7-avenasterol","MG","648",1283,2,"Lipids"),
    (1296,"Delta-5-avenasterol","MG","649",1283,2,"Lipids"),
    (1297,"Alpha-spinasterol","MG","650",1283,2,"Lipids"),
    (2052,"Delta-7-Stigmastenol","MG","",1283,2,"Lipids"),
    (1298,"Phytosterols, other","MG","651",1283,2,"Lipids"),
    (1254,"Glycerides","G","602",2044,1,"Lipids"),
    (1255,"Phospholipids","G","603",2044,1,"Lipids"),
    (1256,"Glycolipids","G","604",2044,1,"Lipids"),
    (1290,"Unsaponifiable matter (lipids)","G","643",2044,1,"Lipids"),
    (1302,"Wax Esters(Total Wax)","G","661",2044,1,"Lipids"),
    # ── AMINO ACIDS ─────────────────────────────────────────────────────────
    (2042,"Amino acids","G","500",None,0,"Amino Acids"),
    (1210,"Tryptophan","G","501",2042,1,"Amino Acids"),
    (1211,"Threonine","G","502",2042,1,"Amino Acids"),
    (1212,"Isoleucine","G","503",2042,1,"Amino Acids"),
    (1213,"Leucine","G","504",2042,1,"Amino Acids"),
    (1214,"Lysine","G","505",2042,1,"Amino Acids"),
    (1215,"Methionine","G","506",2042,1,"Amino Acids"),
    (1216,"Cystine","G","507",2042,1,"Amino Acids"),
    (1232,"Cysteine","G","526",2042,1,"Amino Acids"),
    (1229,"Cysteine and methionine(sulfer containig AA)","G","522",2042,1,"Amino Acids"),
    (1230,"Phenylalanine and tyrosine (aromatic  AA)","G","523",2042,1,"Amino Acids"),
    (1217,"Phenylalanine","G","508",1230,2,"Amino Acids"),
    (1218,"Tyrosine","G","509",1230,2,"Amino Acids"),
    (1219,"Valine","G","510",2042,1,"Amino Acids"),
    (1220,"Arginine","G","511",2042,1,"Amino Acids"),
    (1221,"Histidine","G","512",2042,1,"Amino Acids"),
    (1222,"Alanine","G","513",2042,1,"Amino Acids"),
    (1223,"Aspartic acid","G","514",2042,1,"Amino Acids"),
    (1231,"Asparagine","G","525",1223,2,"Amino Acids"),
    (1224,"Glutamic acid","G","515",2042,1,"Amino Acids"),
    (1233,"Glutamine","G","528",1224,2,"Amino Acids"),
    (1225,"Glycine","G","516",2042,1,"Amino Acids"),
    (1226,"Proline","G","517",2042,1,"Amino Acids"),
    (1227,"Serine","G","518",2042,1,"Amino Acids"),
    (1228,"Hydroxyproline","G","521",2042,1,"Amino Acids"),
    (1234,"Taurine","G","529",2042,1,"Amino Acids"),
    # ── OTHER ───────────────────────────────────────────────────────────────
    (1018,"Alcohol, ethyl","G","221",None,0,"Other"),
    (1057,"Caffeine","MG","262",None,0,"Other"),
    (1058,"Theobromine","MG","263",None,0,"Other"),
    (2057,"Ergothioneine","MG","",None,0,"Other"),
    (1336,"ORAC, Hydrophyllic","UMOL_TE","706",None,0,"Other"),
    (1337,"ORAC, Lipophillic","UMOL_TE","707",None,0,"Other"),
    (1338,"ORAC, Total","UMOL_TE","708",None,0,"Other"),
    (1339,"Total Phenolics","MG_GAE","709",None,0,"Other"),
    # ── ISOFLAVONES ─────────────────────────────────────────────────────────
    (1343,"Isoflavones","MG","713",None,0,"Isoflavones"),
    (1340,"Daidzein","MG","710",1343,1,"Isoflavones"),
    (1341,"Genistein","MG","711",1343,1,"Isoflavones"),
    (1342,"Glycitein","MG","712",1343,1,"Isoflavones"),
    (2049,"Daidzin","MG","717",1343,1,"Isoflavones"),
    (2050,"Genistin","MG","718",1343,1,"Isoflavones"),
    (2051,"Glycitin","MG","719",1343,1,"Isoflavones"),
    (1344,"Biochanin A","MG","714",1343,1,"Isoflavones"),
    (1345,"Formononetin","MG","715",1343,1,"Isoflavones"),
    (1346,"Coumestrol","MG","716",1343,1,"Isoflavones"),
    # ── FLAVONOIDS ──────────────────────────────────────────────────────────
    (1347,"Flavonoids, total","MG","729",None,0,"Flavonoids"),
    (1348,"Anthocyanidins","MG","730",1347,1,"Flavonoids"),
    (1349,"Cyanidin","MG","731",1348,2,"Flavonoids"),
    (1350,"Proanthocyanidin (dimer-A linkage)","MG","732",1348,2,"Flavonoids"),
    (1351,"Proanthocyanidin monomers","MG","733",1348,2,"Flavonoids"),
    (1352,"Proanthocyanidin dimers","MG","734",1348,2,"Flavonoids"),
    (1353,"Proanthocyanidin trimers","MG","735",1348,2,"Flavonoids"),
    (1354,"Proanthocyanidin 4-6mers","MG","736",1348,2,"Flavonoids"),
    (1355,"Proanthocyanidin 7-10mers","MG","737",1348,2,"Flavonoids"),
    (1356,"Proanthocyanidin polymers (>10mers)","MG","738",1348,2,"Flavonoids"),
    (1357,"Delphinidin","MG","741",1348,2,"Flavonoids"),
    (1358,"Malvidin","MG","742",1348,2,"Flavonoids"),
    (1359,"Pelargonidin","MG","743",1348,2,"Flavonoids"),
    (1360,"Peonidin","MG","745",1348,2,"Flavonoids"),
    (1361,"Petunidin","MG","746",1348,2,"Flavonoids"),
    (1362,"Flavans, total","MG","747",1347,1,"Flavonoids"),
    (1363,"Catechins, total","MG","748",1362,2,"Flavonoids"),
    (1364,"Catechin","MG","749",1363,3,"Flavonoids"),
    (1365,"Epigallocatechin","MG","750",1363,3,"Flavonoids"),
    (1366,"Epicatechin","MG","751",1363,3,"Flavonoids"),
    (1367,"Epicatechin-3-gallate","MG","752",1363,3,"Flavonoids"),
    (1368,"Epigallocatechin-3-gallate","MG","753",1363,3,"Flavonoids"),
    (1369,"Procyanidins, total","MG","754",1362,2,"Flavonoids"),
    (1370,"Theaflavins","MG","755",1362,2,"Flavonoids"),
    (1393,"Theaflavin -3,3' -digallate","MG","791",1370,3,"Flavonoids"),
    (1394,"Theaflavin -3' -gallate","MG","792",1370,3,"Flavonoids"),
    (1395,"Theaflavin -3 -gallate","MG","793",1370,3,"Flavonoids"),
    (1371,"Thearubigins","MG","756",1362,2,"Flavonoids"),
    (1392,"Theogallin","MG","790",1362,2,"Flavonoids"),
    (1396,"(+) -Gallo catechin","MG","794",1363,3,"Flavonoids"),
    (1397,"(+)-Catechin 3-gallate","MG","795",1363,3,"Flavonoids"),
    (1398,"(+)-Gallocatechin 3-gallate","MG","796",1363,3,"Flavonoids"),
    (1372,"Flavanones, total","MG","757",1347,1,"Flavonoids"),
    (1373,"Eriodictyol","MG","758",1372,2,"Flavonoids"),
    (1374,"Hesperetin","MG","759",1372,2,"Flavonoids"),
    (1375,"Isosakuranetin","MG","760",1372,2,"Flavonoids"),
    (1376,"Liquiritigenin","MG","761",1372,2,"Flavonoids"),
    (1377,"Naringenin","MG","762",1372,2,"Flavonoids"),
    (1378,"Flavones, total","MG","768",1347,1,"Flavonoids"),
    (1379,"Apigenin","MG","770",1378,2,"Flavonoids"),
    (1380,"Chrysoeriol","MG","771",1378,2,"Flavonoids"),
    (1381,"Diosmetin","MG","772",1378,2,"Flavonoids"),
    (1382,"Luteolin","MG","773",1378,2,"Flavonoids"),
    (1383,"Nobiletin","MG","781",1378,2,"Flavonoids"),
    (1384,"Sinensetin","MG","782",1378,2,"Flavonoids"),
    (1385,"Tangeretin","MG","783",1378,2,"Flavonoids"),
    (1386,"Flavonols, total","MG","784",1347,1,"Flavonoids"),
    (1387,"Isorhamnetin","MG","785",1386,2,"Flavonoids"),
    (1388,"Kaempferol","MG","786",1386,2,"Flavonoids"),
    (1389,"Limocitrin","MG","787",1386,2,"Flavonoids"),
    (1390,"Myricetin","MG","788",1386,2,"Flavonoids"),
    (1391,"Quercetin","MG","789",1386,2,"Flavonoids"),
    # ── PHENOLIC ACIDS ───────────────────────────────────────────────────────
    (1208,"Phenolic acids, total","MG","469",None,0,"Phenolic Acids"),
    (1209,"Polyphenols, total","MG","470",1208,1,"Phenolic Acids"),
    (1200,"p-Hydroxy benzoic acid","MG","460",1208,1,"Phenolic Acids"),
    (1201,"Caffeic acid","MG","461",1208,1,"Phenolic Acids"),
    (1202,"p-Coumaric acid","MG","462",1208,1,"Phenolic Acids"),
    (1203,"Ellagic acid","MG","463",1208,1,"Phenolic Acids"),
    (1204,"Ferrulic acid","MG","464",1208,1,"Phenolic Acids"),
    (1205,"Gentisic acid","MG","465",1208,1,"Phenolic Acids"),
    (1206,"Tyrosol","MG","466",1208,1,"Phenolic Acids"),
    (1207,"Vanillic acid","MG","467",1208,1,"Phenolic Acids"),
]


def build_nutrient_lookup(raw_food_nutrients: list) -> dict:
    """
    Build a dict: nutrient_id (int) -> full foodNutrient object from raw API JSON.
    Keeps first occurrence only (API sometimes repeats with different amounts).
    """
    lookup = {}
    for fn in raw_food_nutrients:
        nutrient = fn.get("nutrient", {})
        nid = nutrient.get("id")
        if nid is not None and int(nid) not in lookup:
            lookup[int(nid)] = fn
    return lookup


def make_nutrient_node(h_entry: tuple, fn_data) -> dict:
    """
    Build a single nutrient node for the output JSON.
    h_entry: one row of HIERARCHY
    fn_data: matching foodNutrient object from API (or None)
    """
    nid, name, unit, nbr, parent_id, level, group = h_entry

    node = {
        "nutrient_id": nid,
        "name": name,
        "unit": unit,
        "nutrient_nbr": nbr,
        "group": group,
    }

    if fn_data is not None:
        # Pull all fields from the raw foodNutrient entry
        node["amount"] = fn_data.get("amount")
        node["dataPoints"] = fn_data.get("dataPoints")
        node["min"] = fn_data.get("min")
        node["max"] = fn_data.get("max")
        node["median"] = fn_data.get("median")
        node["footnote"] = fn_data.get("footnote")
        node["percentDailyValue"] = fn_data.get("percentDailyValue")

        # Derivation info
        deriv = fn_data.get("foodNutrientDerivation") or fn_data.get("derivation")
        if deriv:
            node["derivation"] = {
                "id": deriv.get("id"),
                "code": deriv.get("code"),
                "description": deriv.get("description"),
            }

        # Source samples (Foundation foods have these)
        if "nutrientAnalysisFactor" in fn_data:
            node["nutrientAnalysisFactor"] = fn_data["nutrientAnalysisFactor"]

        # Remove None values to keep JSON clean
        node = {k: v for k, v in node.items() if v is not None}

    return node


def build_nested_nutrients(raw_food_nutrients: list) -> list:
    """
    Transform flat foodNutrients list into nested hierarchy.
    Returns a list of top-level section objects, each with a 'nutrients' list
    that may recursively contain 'nutrients' for children.
    """
    lookup = build_nutrient_lookup(raw_food_nutrients)

    # Index hierarchy by nutrient_id for quick child lookup
    # children_of[parent_id] = [list of h_entries]
    children_of: dict = {}
    top_level = []  # h_entries with parent_id = None

    for entry in HIERARCHY:
        nid, name, unit, nbr, parent_id, level, group = entry
        if parent_id is None:
            top_level.append(entry)
        else:
            children_of.setdefault(parent_id, []).append(entry)

    def build_node(entry: tuple):
        nid = entry[0]
        fn_data = lookup.get(nid)
        children = children_of.get(nid, [])

        # Build child nodes recursively
        child_nodes = []
        for child_entry in children:
            child_node = build_node(child_entry)
            if child_node is not None:
                child_nodes.append(child_node)

        # Only include this node if it has data OR has children with data
        has_data = fn_data is not None
        has_children = len(child_nodes) > 0

        if not has_data and not has_children:
            return None  # omit entirely — no data at all

        node = make_nutrient_node(entry, fn_data)
        if child_nodes:
            node["nutrients"] = child_nodes
        return node

    # Build top-level sections
    result = []
    for entry in top_level:
        node = build_node(entry)
        if node is not None:
            result.append(node)

    return result


def transform_food_json(raw: dict) -> dict:
    """
    Take a raw USDA FDC API response dict and return a transformed version
    with nested nutrients. All other top-level fields (description, dataType,
    foodCategory, labelNutrients, ingredients, etc.) are preserved as-is.
    """
    out = {}

    # ── Preserve all non-nutrient top-level fields exactly as-is ─────────────
    skip_keys = {"foodNutrients"}
    for k, v in raw.items():
        if k not in skip_keys:
            out[k] = copy.deepcopy(v)

    # ── Replace foodNutrients with nested structure ───────────────────────────
    raw_nutrients = raw.get("foodNutrients", [])
    out["foodNutrients"] = build_nested_nutrients(raw_nutrients)

    return out


def process_folder(src_folder: Path, dst_folder: Path):
    """Process all JSON files in src_folder, write transformed output to dst_folder."""
    dst_folder.mkdir(parents=True, exist_ok=True)

    json_files = sorted(src_folder.glob("*.json"))
    if not json_files:
        print(f"  ⚠  No JSON files found in {src_folder}")
        return

    for json_file in json_files:
        try:
            with open(json_file, encoding="utf-8") as f:
                raw = json.load(f)

            transformed = transform_food_json(raw)

            out_path = dst_folder / json_file.name
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(transformed, f, indent=2, ensure_ascii=False)

            fdc_id = raw.get("fdcId", "?")
            desc = raw.get("description", "")[:60]
            n_raw = len(raw.get("foodNutrients", []))
            n_sections = len(transformed.get("foodNutrients", []))
            print(f"  ✓  {json_file.name}  [{fdc_id}] {desc}")
            print(f"       {n_raw} raw nutrients → {n_sections} top-level sections")

        except Exception as e:
            print(f"  ✗  {json_file.name} — ERROR: {e}")


def main():
    script_dir = Path(__file__).parent.resolve()
    print(f"Working directory: {script_dir}\n")

    summary = []

    for folder_name in SOURCE_FOLDERS:
        src = script_dir / folder_name
        dst = script_dir / f"corrected_{folder_name}"

        if not src.exists():
            print(f"⚠  Folder not found, skipping: {src}")
            continue

        print(f"📂  Processing: {folder_name}  →  {dst.name}")
        
        # Count files for summary
        json_files = list(src.glob("*.json"))
        process_folder(src, dst)
        
        summary.append({
            "folder": folder_name,
            "count": len(json_files),
            "output": dst.name
        })
        print()

    print("" + "="*50)
    print("      TRANSFORMATION SUMMARY")
    print("="*50)
    for item in summary:
        print(f" {item['folder']:<40} | {item['count']} files -> {item['output']}")
    print("="*50)
    print("✅  Done. All corrected JSON files written.")


if __name__ == "__main__":
    main()
