import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# קריאת כתובת מסד הנתונים ממשתנה סביבה (שמוגדר ב-docker-compose.yml)
SQLALCHEMY_DATABASE_URL = os.environ.get("DATABASE_URL")

# יצירת "מנוע" החיבור הראשי למסד הנתונים
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# הגדרת "תבנית" ליצירת שיחות (sessions) עם מסד הנתונים
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# יצירת קלאס "בסיס" שעליו כל המודלים שלנו (כמו Product ו-Price) ייבנו
Base = declarative_base()