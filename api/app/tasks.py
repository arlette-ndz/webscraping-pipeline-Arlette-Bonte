import os
import subprocess
import sys
from celery import Celery

BROKER = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery = Celery("flight_tasks", broker=BROKER, backend=BACKEND)

celery.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Africa/Abidjan",
    enable_utc=True,
    beat_schedule={
        "scrape-flights-every-6h": {
            "task": "app.tasks.run_scraping_task",
            "schedule": 21600.0,
        },
    },
)

@celery.task(bind=True, name="app.tasks.run_scraping_task", max_retries=2, default_retry_delay=120)
def run_scraping_task(self):
    env = {**os.environ}
    try:
        result = subprocess.run(
            [sys.executable, "-m", "scrapy", "crawl", "skyscanner"],
            cwd="/scraper",
            capture_output=True,
            text=True,
            timeout=900,
            env=env,
        )
        status = "success" if result.returncode == 0 else "error"
        return {
            "status": status,
            "returncode": result.returncode,
            "stdout_tail": result.stdout[-3000:],
            "stderr_tail": result.stderr[-1000:],
        }
    except subprocess.TimeoutExpired:
        raise self.retry(exc=Exception("Timeout"))
    except Exception as exc:
        raise self.retry(exc=exc)
