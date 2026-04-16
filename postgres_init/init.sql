-- Ce script s'exécute automatiquement au premier démarrage du conteneur postgres
-- Il crée la base de données si elle n'existe pas déjà

SELECT 'CREATE DATABASE flights_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'flights_db')\gexec

-- Connecter à flights_db et créer la table (sera aussi créée par SQLAlchemy/Scrapy au démarrage)
\c flights_db

CREATE TABLE IF NOT EXISTS flights (
    id                SERIAL PRIMARY KEY,
    scraped_at        TIMESTAMP,
    query_type        VARCHAR(50),
    origin_sky_id     VARCHAR(10),
    origin_name       VARCHAR(100),
    destination_sky_id VARCHAR(10),
    destination_name  VARCHAR(100),
    zone              VARCHAR(100),
    departure_date    DATE,
    return_date       DATE,
    price             NUMERIC(10,2),
    currency          VARCHAR(5)    DEFAULT 'EUR',
    cabin_class       VARCHAR(50),
    stops             INTEGER,
    stop_details      TEXT,
    stop_summary      VARCHAR(400),
    airline           VARCHAR(200),
    flight_number     VARCHAR(100),
    duration_minutes  INTEGER,
    is_direct         BOOLEAN,
    score             NUMERIC(10,4),
    tags              TEXT,
    created_at        TIMESTAMP DEFAULT NOW(),
    updated_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE(origin_sky_id, destination_sky_id, departure_date, price, airline, cabin_class)
);

CREATE INDEX IF NOT EXISTS idx_dest  ON flights(destination_name);
CREATE INDEX IF NOT EXISTS idx_date  ON flights(departure_date);
CREATE INDEX IF NOT EXISTS idx_price ON flights(price);
CREATE INDEX IF NOT EXISTS idx_zone  ON flights(zone);
CREATE INDEX IF NOT EXISTS idx_direct ON flights(is_direct);

-- Vue pratique pour pgAdmin
CREATE OR REPLACE VIEW v_flights_summary AS
SELECT
    destination_name,
    zone,
    COUNT(*)                    AS total_vols,
    MIN(price)                  AS prix_min,
    ROUND(AVG(price)::NUMERIC, 2) AS prix_moyen,
    MAX(price)                  AS prix_max,
    SUM(CASE WHEN is_direct THEN 1 ELSE 0 END) AS vols_directs,
    MAX(scraped_at)             AS dernier_scraping
FROM flights
WHERE price IS NOT NULL
GROUP BY destination_name, zone
ORDER BY prix_moyen ASC;
