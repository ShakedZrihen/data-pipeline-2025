import os, sys
from utils.convert_json_format import convert_json_to_target_prices_format
from utils.convert_json_format import convert_json_to_target_promos_format

# נתיב לתיקיית ה-PRICES עם הקבצים המקוריים
prices_dir = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\origin_files\PRICES"
promos_dir = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\origin_files\PROMOS"

# יצירת תיקייה פנימית בשם 'converted' אם היא לא קיימת
converted_dir = os.path.join(prices_dir, "converted")
converted_dir_promos = os.path.join(promos_dir, "converted")
os.makedirs(converted_dir, exist_ok=True)

# מעבר על כל קובצי .json בתיקייה
for filename in os.listdir(prices_dir):
    if filename.endswith(".json"):
        input_path = os.path.join(prices_dir, filename)
        name, ext = os.path.splitext(filename)
        new_filename = f"{name}_converted{ext}"
        output_path = os.path.join(converted_dir, new_filename)

        convert_json_to_target_prices_format(input_path, output_path)
        print(f"Converted: {filename}")

for filename in os.listdir(promos_dir):
    if filename.endswith(".json"):
        input_path = os.path.join(promos_dir, filename)
        name, ext = os.path.splitext(filename)
        new_filename = f"{name}_converted{ext}"
        output_path = os.path.join(converted_dir_promos, new_filename)

        convert_json_to_target_promos_format(converted_dir_promos, output_path)
        print(f"Converted: {filename}")
