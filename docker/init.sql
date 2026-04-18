-- ================================================================
-- ENSEA — AS Data Science | Pipeline Web Scraping
-- Schéma PostgreSQL — Surveillance vols Abidjan
-- ================================================================

-- Table principale des vols
CREATE TABLE IF NOT EXISTS vols (
    id               SERIAL PRIMARY KEY,
    vol_id           VARCHAR(255) UNIQUE,          -- identifiant unique Skyscanner
    origine          VARCHAR(10)  NOT NULL,
    destination      VARCHAR(10)  NOT NULL,
    ville_origine    VARCHAR(100) DEFAULT 'Abidjan',
    ville_destination VARCHAR(100),
    pays_destination VARCHAR(100),
    continent        VARCHAR(50),

    -- Dates & horaires (calculés automatiquement à chaque scrap)
    date_depart      TIMESTAMP,
    date_arrivee     TIMESTAMP,
    duree_minutes    INTEGER DEFAULT 0,
    heure_depart     TIME,
    heure_arrivee    TIME,

    -- Prix
    prix             NUMERIC(10,2),
    devise           VARCHAR(5)   DEFAULT 'USD',
    prix_xof         INTEGER,                      -- converti en FCFA

    -- Infos vol
    compagnie        VARCHAR(255),
    classe_cabine    VARCHAR(50),
    escales          INTEGER      DEFAULT 0,
    villes_escale    VARCHAR(255),                 -- ex: "Casablanca, Paris"
    type_vol         VARCHAR(10)  DEFAULT 'oneway',-- oneway / return

    -- Collecte
    date_collecte    DATE         DEFAULT CURRENT_DATE,
    heure_collecte   TIME         DEFAULT CURRENT_TIME,
    source_endpoint  VARCHAR(100),
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- Table statistiques par destination (mise à jour à chaque scrap)
CREATE TABLE IF NOT EXISTS destinations (
    id           SERIAL PRIMARY KEY,
    code_iata    VARCHAR(10)  UNIQUE,
    ville        VARCHAR(100),
    pays         VARCHAR(100),
    continent    VARCHAR(50),
    prix_min     NUMERIC(10,2),
    prix_moyen   NUMERIC(10,2),
    prix_max     NUMERIC(10,2),
    nb_vols      INTEGER DEFAULT 0,
    derniere_maj DATE
);

-- Table logs des scrapings
CREATE TABLE IF NOT EXISTS scraping_logs (
    id             SERIAL PRIMARY KEY,
    task_id        VARCHAR(100),
    demarrage      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fin            TIMESTAMP,
    statut         VARCHAR(20) DEFAULT 'en_cours',
    vols_collectes INTEGER DEFAULT 0,
    vols_inseres   INTEGER DEFAULT 0,
    erreurs        INTEGER DEFAULT 0,
    message        TEXT
);

-- Index pour performances
CREATE INDEX IF NOT EXISTS idx_vols_destination  ON vols(destination);
CREATE INDEX IF NOT EXISTS idx_vols_date_depart  ON vols(date_depart);
CREATE INDEX IF NOT EXISTS idx_vols_prix         ON vols(prix);
CREATE INDEX IF NOT EXISTS idx_vols_date_coll    ON vols(date_collecte);
CREATE INDEX IF NOT EXISTS idx_vols_continent    ON vols(continent);

-- Trigger : updated_at automatique
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = CURRENT_TIMESTAMP; RETURN NEW; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_vols_updated_at ON vols;
CREATE TRIGGER trg_vols_updated_at
    BEFORE UPDATE ON vols
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Vue : meilleurs prix par destination
CREATE OR REPLACE VIEW v_meilleurs_prix AS
SELECT
    destination,
    ville_destination,
    pays_destination,
    continent,
    type_vol,
    MIN(prix)                         AS prix_min_usd,
    MIN(prix_xof)                     AS prix_min_xof,
    ROUND(AVG(prix)::numeric, 2)      AS prix_moyen_usd,
    MAX(prix)                         AS prix_max_usd,
    MIN(escales)                      AS escales_min,
    COUNT(*)                          AS nb_vols,
    MAX(date_collecte)                AS derniere_collecte
FROM vols
GROUP BY destination, ville_destination, pays_destination, continent, type_vol
ORDER BY prix_min_usd ASC;

-- Vue : évolution des prix par date de collecte
CREATE OR REPLACE VIEW v_evolution_prix AS
SELECT
    destination,
    ville_destination,
    date_collecte,
    ROUND(MIN(prix)::numeric, 2)  AS prix_min,
    ROUND(AVG(prix)::numeric, 2)  AS prix_moyen,
    COUNT(*)                      AS nb_vols
FROM vols
GROUP BY destination, ville_destination, date_collecte
ORDER BY destination, date_collecte;

-- Vue : stats par compagnie
CREATE OR REPLACE VIEW v_stats_compagnies AS
SELECT
    compagnie,
    COUNT(*)                      AS nb_vols,
    ROUND(MIN(prix)::numeric, 2)  AS prix_min,
    ROUND(AVG(prix)::numeric, 2)  AS prix_moyen,
    COUNT(DISTINCT destination)   AS nb_destinations
FROM vols
WHERE compagnie IS NOT NULL AND compagnie != ''
GROUP BY compagnie
ORDER BY nb_vols DESC;

DO $$ BEGIN RAISE NOTICE 'Base flights_db initialisée avec succès.'; END $$;
