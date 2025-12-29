FROM python:3.12-slim
WORKDIR /app

# Remember we use the browse
RUN  apt-get update \
 && apt-get install -y chromium \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "scraper.py"]
