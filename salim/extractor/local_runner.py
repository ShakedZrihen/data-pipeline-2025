import json
from pathlib import Path
from lambda_function import lambda_handler as extractor_handler
from consumer_lambda import lambda_handler as consumer_handler
from src.xml_processor import XMLProcessor   # ✅ updated import

# ------------------------------------------------------
# CONFIG – pick which file and type to run locally
# ------------------------------------------------------
INPUT_FILE = "input/victory_branch1_promo.xml"
PROVIDER = "victory"
FILE_TYPE = "promoFull"   # change to "pricesFull" if testing price files

# ------------------------------------------------------
# Step 1: Run extractor locally
# ------------------------------------------------------
with open(INPUT_FILE, "rb") as f:
    xml_content = f.read()

processor = XMLProcessor()
parsed = processor.parse(xml_content, provider=PROVIDER, file_type=FILE_TYPE)

# Save extractor output
Path("output").mkdir(exist_ok=True)
with open("output/extracted.json", "w", encoding="utf-8") as f:
    json.dump(parsed, f, indent=2, ensure_ascii=False)

print("============================================================")
print("Extractor output saved to output/extracted.json")
print("============================================================")

# ------------------------------------------------------
# Step 2: Simulate SQS message and run consumer
# ------------------------------------------------------
sqs_message = {
    "Records": [
        {
            "body": json.dumps(parsed)
        }
    ]
}

print("============================================================")
print("SENDING MESSAGE TO CONSUMER")
print("============================================================")

consumer_handler(sqs_message, None)

print("============================================================")
print("Consumer finished. Check output/extracted.json for results.")
print("============================================================")
