FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copier requirements API (contient celery + redis)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Installer Scrapy pour que le worker puisse lancer le spider
RUN pip install scrapy==2.11.2 itemadapter==0.9.0 psycopg2-binary==2.9.9

COPY . .

# Le worker démarre Celery
CMD ["celery", "-A", "app.tasks", "worker", "--loglevel=info", "--concurrency=2"]
