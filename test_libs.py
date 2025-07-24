try:
    import requests
    import bs4
    import pandas as pd
    import sqlalchemy
    import psycopg2
    import fastapi
    import uvicorn
    import telegram
    import openai

    print("✅ All imports succeeded!")

    # Minimal usage checks (optional)
    print("requests version:", requests.__version__)
    print("pandas DataFrame:", pd.DataFrame({'a': [1, 2]}))
    print("sqlalchemy version:", sqlalchemy.__version__)
    print("psycopg2 version:", psycopg2.__version__)
    print("fastapi version:", fastapi.__version__)
    print("uvicorn version:", uvicorn.__version__)
    print("telegram version:", telegram.__version__)
    print("openai version:", openai.__version__)

except ImportError as e:
    print("❌ Import failed:", e)
except Exception as e:
    print("⚠️ Something else went wrong:", e)
