import os

import psycopg2

uri = os.environ["POSTGRES_URI"]
conn = psycopg2.connect(uri)
