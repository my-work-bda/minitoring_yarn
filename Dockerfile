FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# CMD ["python", "main.py"]

# docker build -t kevinity310/yarn-scraping:v2 .
# docker run 
