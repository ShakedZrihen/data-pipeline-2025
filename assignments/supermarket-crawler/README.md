1) Create & activate a virtual environment:

python3 -m venv venv (preferrably python3.11 with: py -3.11 -m venv venv)
source venv/bin/activate

2) Install Dependencies

pip install -r requirements.txt

3) Configure AWS/S3 — create a .env in the project root:

AWS_ACCESS_KEY_ID=YOUR_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_SECRET
AWS_DEFAULT_REGION=il-central-1
S3_BUCKET=bucker name

4) With your venv active and .env set:

python crawler_final.py <your-s3-bucket> --only yohananof,victory,carrefour
(Use the --only flag)

