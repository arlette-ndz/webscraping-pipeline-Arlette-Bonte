import os

BOT_NAME         = "flight_scraper"
SPIDER_MODULES   = ["flight_scraper.spiders"]
NEWSPIDER_MODULE = "flight_scraper.spiders"

# Identification éthique ENSEA
USER_AGENT = "ENSEA Educational Project — Surveillance Vols Abidjan (konatengolo@ufhb.edu.ci)"

ROBOTSTXT_OBEY            = False  # API REST, pas de site web
DOWNLOAD_DELAY            = 1.5
RANDOMIZE_DOWNLOAD_DELAY  = True
CONCURRENT_REQUESTS       = 2
CONCURRENT_REQUESTS_PER_DOMAIN = 1

DEFAULT_REQUEST_HEADERS = {
    "Accept":          "application/json",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

RETRY_TIMES      = 3
RETRY_HTTP_CODES = [429, 500, 502, 503, 504]

# Pipelines (ordre d'exécution)
ITEM_PIPELINES = {
    "flight_scraper.pipelines.CleaningPipeline":    200,
    "flight_scraper.pipelines.PostgreSQLPipeline":  300,
    "flight_scraper.pipelines.JsonPipeline":        400,
}

# Connexion PostgreSQL (lue depuis variables d'environnement)
DATABASE = {
    "host":     os.getenv("DB_HOST",     "db"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME",     "flights_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

# Logs
LOG_LEVEL = "INFO"
os.makedirs("logs", exist_ok=True)
LOG_FILE = "logs/scraper.log"
