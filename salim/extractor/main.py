import os
from utils.file_utils import extract_and_delete_gz, convert_xml_to_json

# 📁 כל הנתיבים
paths = [
    # Prices
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\carrefour\PriceFull7290055700007-0006-202508070510.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\hazi-hinam\PriceFull7290700100008-000-103-20250807-010629.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\osherad\PriceFull7290103152017-001-202508070800.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\ramilevy\PriceFull7290058140886-001-202508070010.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\tivtaam\PriceFull7290873255550-002-202508070010.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\yohananof\PriceFull7290803800003-001-202508070010.gz",

    # # Promos
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\carrefour\PromoFull7290055700007-0006-202508070511.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\hazi-hinam\PromoFull7290700100008-000-103-20250807-010712.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\osherad\PromoFull7290103152017-001-202508070800.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\ramilevy\PromoFull7290058140886-001-202508070010.gz",
    # r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\tivtaam\PromoFull7290873255550-002-202508070010.gz",
    r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\yohananof\PromoFull7290803800003-095-202508070011.gz"
]

# 📁 בסיס לתיקיות יעד
base_output_dir = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\converted_files"

for path in paths:
    try:
        # שלב 1: חילוץ
        extracted_path = extract_and_delete_gz(path)

        # שלב 2: המרה ל־JSON
        output_json_path = convert_xml_to_json(extracted_path)

        # שלב 3: שם הסופר מתוך הנתיב
        supermarket = os.path.basename(os.path.dirname(path))

        # שלב 4: קובץ מקור
        original_name = os.path.basename(output_json_path)

        # שלב 5: תיקיית יעד לפי סוג קובץ
        if "PriceFull" in path:
            subfolder = "PRICES"
        elif "PromoFull" in path:
            subfolder = "PROMOS"
        else:
            continue

        final_dir = os.path.join(base_output_dir, subfolder)
        os.makedirs(final_dir, exist_ok=True)

        # שלב 6: שם חדש לקובץ
        new_name = f"{supermarket}_{original_name}"
        final_path = os.path.join(final_dir, new_name)

        os.replace(output_json_path, final_path)
        print(f"✅ {new_name} saved to {subfolder}")

    except Exception as e:
        print(f"❌ Error processing {path}: {e}")

# from utils.file_utils import process_all_gz_in_folder
# from utils.file_utils import extract_and_delete_gz, convert_xml_to_json

# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\carrefour\PriceFull7290055700007-0006-202508070510.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\hazi-hinam\PriceFull7290700100008-000-103-20250807-010629.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\osherad\PriceFull7290103152017-001-202508070800.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\ramilevy\PriceFull7290058140886-001-202508070010.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\tivtaam\PriceFull7290873255550-002-202508070010.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\yohananof\PriceFull7290803800003-001-202508070010.gz

# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\carrefour\PromoFull7290055700007-0006-202508070511.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\hazi-hinam\PromoFull7290700100008-000-103-20250807-010712.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\osherad\PromoFull7290103152017-001-202508070800.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\ramilevy\PromoFull7290058140886-001-202508070010.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\tivtaam\PromoFull7290873255550-002-202508070010.gz
# C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\yohananof\PromoFull0000000000000-00-1-202407291011.gz

# outputPath = extract_and_delete_gz(path)

# print (outputPath)

# outputXml = convert_xml_to_json(outputPath)

# print (outputXml)

# path = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\osherad"

# process_all_gz_in_folder(path)

