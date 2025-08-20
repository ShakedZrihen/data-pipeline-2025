FROM python:3.11-slim

WORKDIR /app

# 1) התקנת תעודות CA כדי ש-requests יעבוד עם HTTPS
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 2) להעתיק את כל קוד האפליקציה, כולל crawlers וכל מה שמחוץ ל-app
#    אם utils/ או קבצים נוספים יושבים בשורש—נעתיק את הכל.
COPY . .

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
