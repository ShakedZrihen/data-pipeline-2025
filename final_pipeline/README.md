# 🧮 מטלת סיום — Data Pipeline (Full Docker Setup)

הפרויקט הזה מממש צינור נתונים מלא לפי הדרישות שלך:
- **Crawler** — רץ כל שעה (ובאתחול פעם אחת), מייצר קבצי מחירים גולמיים ומעלה ל‑S3 (MinIO).
- **Extractor** — מאזין לקבצים חדשים ב‑S3, מוציא/מנטרל ומעביר את הרשומות ל‑Queue (RabbitMQ), ושומר חותמת זמן אחרונה ב‑MongoDB.
- **Enricher** — צורך מהתור, משלים פרטים חסרים (ברירת מחדל בהיוריסטיקה / OpenAI אם יש מפתח), ושומר ל‑SQL (Postgres).
- **API** — FastAPI עם Swagger מלא, כולל מסלולים: `/supermarkets`, `/supermarkets/{id}`, `/supermarkets/{id}/products`, `/products`, `/products/barcode/{barcode}`.

## 🚀 איך מריצים
1. צרי `.env` מהקובץ לדוגמה:
   ```bash
   cp .env.example .env
   ```
2. הרימי את כל הסביבה:
   ```bash
   docker compose up --build
   ```
   זה ירים: Postgres, MongoDB, RabbitMQ, MinIO, תהליך init לתשתיות, Crawler, Extractor, Enricher, ו‑API.

3. גשי ל‑Swagger:
   - http://localhost:${API_PORT}/docs  (ברירת מחדל: http://localhost:8000/docs)

## 📦 מה בפנים
- `crawler/` — מייצר Batch לדוגמה כל שעה ומעלה קובץ `ndjson.gz` ל‑S3 (התממשקות מלאה ל‑MinIO).
- `extractor/` — מוצא קבצים חדשים, שולח כל רשומה כ‑JSON ל‑RabbitMQ, ושומר State ב‑MongoDB.
- `enricher/` — צורך מהתור, עושה העשרה (עם OpenAI אם קיים `OPENAI_API_KEY` ב‑env; אחרת היוריסטיקה), ושומר את התוצאות ל‑Postgres.
- `api/` — FastAPI שמוציא את הדאטה לפי המפרט (כולל השוואת מחירים לפי ברקוד).
- `infra/` — Init לתשתיות: יצירת דלי ב‑MinIO ו‑Queue ב‑RabbitMQ.
- `shared/` — קוד משותף (Config, S3, MQ).

## 🧪 בדיקה מהירה (Happy Path)
- ברגע שה‑stack עולה, ה‑Crawler יעלה Batch ראשון ו‑Extractor יעבד אותו, Enricher יכתוב ל‑DB.
- היכנסי ל‑`/docs` ונסי:
  - `GET /supermarkets`
  - `GET /products?q=חלב`
  - קחי ברקוד מאחד הפריטים והפעילי `GET /products/barcode/{barcode}`

## 🔁 Crawler כל שעה
ה‑Crawler מכיל Scheduler (APScheduler) שמעלה קובץ בכל שעה עגולה, וכן ריצה חד‑פעמית ב‑startup כדי שיהיה מיד דאטה לדמו.

## 🧰 אינטגרציה עם הקוד שלך
- אם יש לך כבר **Crawler אמיתי** שמוריד קבצים מרשתות (Goodpharm/Carrefour וכו׳), אפשר לשים אותו במקומו (או להריץ במקביל) **ובלבד** שיפיק קבצים בפורמט `NDJSON` דחוס `GZIP` לתיקיית S3 `prices/`.
- השדות הנדרשים לכל רשומה:  
  `barcode, canonical_name, brand?, category?, size_value?, size_unit?, price, currency, promo_price?, promo_text?, in_stock, supermarket_id, collected_at`  
  (שדות חסרים יושלמו ע״י `enricher` במידת האפשר).

## 🗃️ סכמת DB
- טבלה `supermarkets` ו‑`products` (ORM: SQLAlchemy). אפשר להרחיב שדות/טבלאות בקלות.
- `UniqueConstraint` מבטיח Snapshot גרסאות לפי `supermarket_id+barcode+collected_at`.

## 🔒 OpenAI (אופציונלי)
- הוסיפי ל‑`.env`:
  ```env
  OPENAI_API_KEY=sk-...
  ```
- אם לא מוגדר — יופעל fallback היוריסטי.

## 📚 מסלולי API (תמצית)
- `GET /supermarkets` — כל הסופרים.
- `GET /supermarkets/{id}` — סופרמרקט ב‑ID.
- `GET /supermarkets/{id}/products?search=...` — מוצרים של סופר מסוים.
- `GET /products?q=...&promo=...&min_price=...&max_price=...&supermarket_id=...` — חיפוש גמיש.
- `GET /products/barcode/{barcode}` — כל המקומות שמוכרים את אותו ברקוד + `savings` (פער מהמחיר הזול ביותר).

## 🛠️ הערות יישום
- **S3**: MinIO עם כתובת פנימית `http://minio:9000` ו‑Bucket `raw-prices` (נוצר אוטומטית).
- **Queue**: RabbitMQ עם Queue `prices_queue` (נוצר אוטומטית).
- **MongoDB**: נשמר State של ה‑Extractor (`extractor_state` / `last_processed_key`).
- **Postgres**: נשמרת הסכמה אוטומטית ע״י ה‑Enricher (ORM).

## 📄 שינויים עתידיים קלים
- הוספת טבלאות `promotions`, `stores` — אפשרית ע״י הרחבת המודל ב‑`enricher/models.py` וה‑endpoints ב‑API.
- מעבר ל‑SQS/DynamoDB בסביבת ענן — החלפת drivers ב‑`shared`/Services.

---
> אם תרצי, אוכל גם **להלביש על זה את הקוד הקיים שלך** (Supabase / קבצי Providers שכתבת) ולהתאים הכול ל‑compose הזה — פשוט תצרפי את הסקריפטים שיש לך ואשלב אותם ישירות כ‑service/ים באותו ה‑stack.


ברבים מהמקרים זה מספיק כדי שבקוד boto3 קיים יעבוד מול MinIO **ללא שינוי**.


## 🧺 העלאה ידנית ל‑S3 (חלק ה‑Upload של המטלה)
אפשרות 1 — **MinIO Console** (GUI):
1. גשו לכתובת הקונסול (ברירת מחדל: `http://localhost:9001`), התחברו עם `minioadmin/minioadmin`.
2. כנסו ל‑Bucket `raw-prices` → `Upload` → העלו קבצי `*.ndjson.gz` אל תחתית ה‑prefix `prices/`.
3. ה‑Extractor יקלוט אותם אוטומטית (מאזין כל דקה).

אפשרות 2 — **AWS CLI** (עם MinIO כ‑endpoint):
```bash
aws --endpoint-url http://localhost:9000   s3 cp ./my_prices.ndjson.gz s3://raw-prices/prices/my_prices.ndjson.gz   --no-verify-ssl
```
> חשוב: ברירת המחדל של ה‑Extractor היא שפורמט הקבצים יהיה `NDJSON.gz`. אם תרצי להעלות קבצי ספקים בפורמט מקורי (gz/xml/zip), מימשתי שלד ב‑`extractor/provider_parsers.py` – רק להוסיף Parser לפי שם הקובץ והוא יומר ל‑JSON.


## 🐍 Crawler — משתמשים רק ב‑Crawler שלך
- השירות `crawler` ב־Docker מריץ את `user_crawler/src/run_crawler.py` (הקוד שלך) כפי שהוא.
- ה־wrapper מריץ `main()` מיידית ואז כל שעה (APScheduler).
- ה־crawler שלך יכול לבחור:
  - להעלות ל‑S3 קבצי `NDJSON.gz` (הכי פשוט) — ה‑Extractor ייקלוט אותם מיד.
  - או להעלות קבצים "מקוריים" ולממש Parser ב‑`extractor/provider_parsers.py`.
- חיבור ל‑MinIO נעשה אוטומטית דרך משתני סביבה של boto3: `AWS_ENDPOINT_URL_S3=http://minio:9000` ועוד.

### הרצה
```bash
docker compose up --build
```
