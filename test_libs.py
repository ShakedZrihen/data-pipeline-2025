import sys

print("🐍 Python version:", sys.version)

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

    print("\n✅ All imports succeeded!")

    # 🔍 Version checks
    print("\n📦 Versions:")
    print("requests:", requests.__version__)
    print("bs4 (BeautifulSoup):", bs4.__version__ if hasattr(bs4, '__version__') else "no __version__ attribute")
    print("pandas:", pd.__version__)
    print("sqlalchemy:", sqlalchemy.__version__)
    print("psycopg2:", psycopg2.__version__)
    print("fastapi:", fastapi.__version__)
    print("uvicorn:", uvicorn.__version__)
    print("telegram:", telegram.__version__)
    print("openai:", openai.__version__)

    # 🧪 Minimal functionality checks
    print("\n🧪 Functionality test:")
    df = pd.DataFrame({'a': [1, 2]})
    print("pandas DataFrame:\n", df)

except ImportError as e:
    print("❌ Import failed:", e)
except Exception as e:
    print("⚠️ Something else went wrong:", e)
