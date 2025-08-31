FROM python:3.12-slim

WORKDIR /app
COPY citymarket.py .
COPY goodpharm.py .
COPY yohananof.py .

RUN pip install selenium
CMD ["python3","goodpharm.py","&&","python3","citymarket.py","&&","python3","yohananof.py"]
