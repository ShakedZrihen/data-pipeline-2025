import os
import sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_LOADER_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "postgresql"))
if JSON_LOADER_DIR not in sys.path:
    sys.path.append(JSON_LOADER_DIR)


WORK_DIR               = os.getenv("WORK_DIR", os.getcwd())

SQS_QUEUE_NAME         = os.getenv("SQS_QUEUE_NAME", "test-queue")
SQS_ENDPOINT_URL       = os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION             = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID      = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

SQS_WAIT_TIME          = int(os.getenv("SQS_WAIT_TIME", "5"))
SQS_MAX_PER_POLL       = int(os.getenv("SQS_MAX_PER_POLL", "10"))

STRICT_PRICE_ONLY      = os.getenv("STRICT_PRICE_ONLY", "1").lower() in ("1","true","yes","y")
PRICE_FIRST            = os.getenv("PRICE_FIRST", "1").lower() in ("1","true","yes","y")

STORES_DIR                 = os.getenv("STORES_DIR", os.path.join(WORK_DIR, "stores"))
ENRICH_OVERWRITE           = os.getenv("ENRICH_OVERWRITE", "0").lower() in ("1","true","yes","y")
ENRICH_NORMALIZE_BRANCH    = os.getenv("ENRICH_NORMALIZE_BRANCH", "1").lower() in ("1","true","yes","y")

PROMO_HINT_KEYS = {
    "start","start_at","startdate",
    "end","end_at","enddate",
    "minqty","min_qty",
    "discount","discountrate","discount_rate","discountedprice","discount_price",
    "products","items"
}
ID_KEYS_STRICT = {"promotionid","promotion_id"}
ID_KEY_GENERIC = "id"

UNITS = r"(?:ml|mL|l|L|gr|g|kg|mg|oz|מ\"?ל|מ׳׳ל|גרם|גר|מ\"?ג|מג|ק\"?ג|ק׳׳ג|ליטר|ל(?:׳|')?|ג(?:׳|')?)"
PACK_WORDS = r"(?:pack|מארז(?:ים)?|אריזה(?:ות)?|חבילה(?:ות)?)"
MARKETING = r"(?:מבצע|חדש!?+|בונוס|מתנה|SALE|Promo)"
CODE_WORDS = r"(?:SKU|EAN|UPC|PLU|מק[\"׳״']?ט|ברקוד|קטלוג)"
