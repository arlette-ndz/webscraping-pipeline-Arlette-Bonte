"""
Tâches Celery — Scraping asynchrone et planifié
Celery Beat : scraping automatique tous les jours à 6h00 (Abidjan)
"""

import subprocess
import os
import logging
from celery import Celery
from celery.schedules import crontab

logger   = logging.getLogger(__name__)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

celery = Celery("flight_tasks", broker=REDIS_URL, backend=REDIS_URL)

celery.conf.update(
    task_serializer   = "json",
    accept_content    = ["json"],
    result_serializer = "json",
    timezone          = "Africa/Abidjan",
    enable_utc        = True,
    task_track_started = True,
    beat_schedule     = {
        "scraping-quotidien-matin": {
            "task":     "celery_app.tasks.lancer_scraping",
            "schedule": crontab(hour=6, minute=0),
        },
        "scraping-quotidien-soir": {
            "task":     "celery_app.tasks.lancer_scraping",
            "schedule": crontab(hour=18, minute=0),
        },
    },
)


@celery.task(bind=True, name="celery_app.tasks.lancer_scraping", max_retries=2)
def lancer_scraping(self):
    """Lance le spider Scrapy Skyscanner et logue le résultat."""
    task_id = self.request.id
    logger.info(f"[{task_id}] Démarrage scraping vols Abidjan...")
    self.update_state(state="PROGRESS", meta={"status": "Spider en cours..."})

    # Enregistrer le début dans la DB
    _log_debut(task_id)

    try:
        result = subprocess.run(
            ["scrapy", "crawl", "skyscanner_vols"],
            cwd="/app/flight_scraper",
            capture_output=True,
            text=True,
            timeout=600,
            env={**os.environ},
        )

        if result.returncode == 0:
            # Extraire le nb d'items depuis les logs Scrapy
            items = _extraire_items(result.stdout + result.stderr)
            _log_fin(task_id, "success", items, 0, "Scraping terminé avec succès")
            logger.info(f"[{task_id}] Terminé : {items} vols collectés")
            return {
                "status":  "SUCCESS",
                "task_id": task_id,
                "items":   items,
                "message": f"{items} vols collectés et mis à jour en base",
            }
        else:
            err = result.stderr[-500:] if result.stderr else "Erreur inconnue"
            _log_fin(task_id, "erreur", 0, 1, err)
            raise Exception(f"Spider returncode={result.returncode}: {err}")

    except subprocess.TimeoutExpired:
        _log_fin(task_id, "timeout", 0, 1, "Timeout > 10 min")
        raise Exception("Timeout : scraping > 10 min")
    except Exception as e:
        logger.error(f"[{task_id}] Erreur : {e}")
        raise self.retry(exc=e, countdown=60)


def _extraire_items(log_text):
    """Extrait le nombre d'items depuis les logs Scrapy."""
    import re
    match = re.search(r"item_scraped_count['\"]?\s*[:=]\s*(\d+)", log_text)
    if match:
        return int(match.group(1))
    match = re.search(r"Scraped (\d+) items", log_text)
    if match:
        return int(match.group(1))
    return 0


def _log_debut(task_id):
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "db"),
            dbname=os.getenv("DB_NAME", "flights_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
        )
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO scraping_logs (task_id, statut) VALUES (%s, 'en_cours')",
            (str(task_id),)
        )
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"Log début impossible : {e}")


def _log_fin(task_id, statut, vols_collectes, erreurs, message):
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "db"),
            dbname=os.getenv("DB_NAME", "flights_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
        )
        cur = conn.cursor()
        cur.execute("""
            UPDATE scraping_logs
            SET fin=CURRENT_TIMESTAMP, statut=%s,
                vols_collectes=%s, erreurs=%s, message=%s
            WHERE task_id=%s
        """, (statut, vols_collectes, erreurs, message[:500], str(task_id)))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"Log fin impossible : {e}")
