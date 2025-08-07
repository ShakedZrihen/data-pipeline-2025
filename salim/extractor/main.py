from utils.file_utils import extract_and_delete_gz, convert_xml_to_json
# from utils.file_utils import process_all_gz_in_folder

# path = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\osherad\PromoFull7290103152017-001-202508070800.gz"
# path = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\carrefour\PromoFull7290055700007-0006-202508070511.gz"
# path = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\yohananof\PriceFull7290803800003-001-202508070010.gz"
path = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\hazi-hinam\PriceFull7290700100008-000-103-20250807-040701.gz"
outputPath = extract_and_delete_gz(path)

print (outputPath)

outputXml = convert_xml_to_json(outputPath)

print (outputXml)

# path = r"C:\Users\mayab\Documents\Software Engineering\ThirdYear\SemesterK\Systems Development In Python\finalProject\data-pipeline-2025\salim\crawlers\local_files\osherad"

# process_all_gz_in_folder(path)

