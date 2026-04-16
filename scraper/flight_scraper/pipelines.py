import json
import logging
import psycopg2
import os
from datetime import datetime
from itemadapter import ItemAdapter

logger = logging.getLogger(__name__)


class CleaningPipeline:
    """Nettoie et valide chaque item avant stockage."""

    def process_item(self, item, spider):
        a = ItemAdapter(item)

        # Prix
        price = a.get("price")
        if price is not None:
            try:
                a["price"] = round(float(price), 2)
            except (ValueError, TypeError):
                a["price"] = None

        # Dates → YYYY-MM-DD
        for f in ("departure_date", "return_date"):
            v = a.get(f)
            if v:
                a[f] = str(v)[:10]

        # scraped_at → timestamp
        sa = a.get("scraped_at")
        if sa:
            a["scraped_at"] = str(sa)[:19].replace("T", " ")

        # Durée
        d = a.get("duration_minutes")
        if d is not None:
            try:
                a["duration_minutes"] = int(d)
            except Exception:
                a["duration_minutes"] = None

        # Compagnie
        airline = a.get("airline")
        if airline:
            a["airline"] = str(airline).strip()[:200]

        # Classe cabine
        cc = a.get("cabin_class") or "auto"
        a["cabin_class"] = str(cc).lower().strip()[:50]

        # Stops
        stops = a.get("stops")
        if stops is not None:
            try:
                a["stops"] = int(stops)
            except Exception:
                a["stops"] = None

        # is_direct cohérence
        if a.get("stops") == 0:
            a["is_direct"] = True
        elif isinstance(a.get("stops"), int) and a["stops"] > 0:
            a["is_direct"] = False

        # stop_details → string JSON
        sd = a.get("stop_details")
        if sd and not isinstance(sd, str):
            a["stop_details"] = json.dumps(sd)
        elif not sd:
            a["stop_details"] = "[]"

        # Zone
        if not a.get("zone"):
            a["zone"] = "Autre"

        return item


class PostgresPipeline:
    """
    Insère dans PostgreSQL avec UPSERT :
    — si le vol existe déjà (même origine/destination/date/prix/compagnie/classe),
      on met à jour les champs qui peuvent changer (escales, durée, score, scraped_at).
    — sinon on insère.
    Résultat : la base est toujours fraîche à chaque scraping.
    """

    def open_spider(self, spider):
        self.conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", 5432)),
            dbname=os.environ.get("POSTGRES_DB", "flights_db"),
            user=os.environ.get("POSTGRES_USER", "flights_user"),
            password=os.environ.get("POSTGRES_PASSWORD", "flights_pass"),
        )
        self.cur = self.conn.cursor()
        self._ensure_schema()
        self.buffer = []
        self.BATCH = 50
        self.inserted = 0
        self.updated = 0
        self.errors = 0

    def _ensure_schema(self):
        self.cur.execute("""
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
                currency          VARCHAR(5)  DEFAULT 'EUR',
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
            )
        """)
        # Index pour les requêtes dashboard
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_dest ON flights(destination_name)")
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_date ON flights(departure_date)")
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_price ON flights(price)")
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_direct ON flights(is_direct)")
        self.conn.commit()

    def process_item(self, item, spider):
        a = ItemAdapter(item)
        dep = str(a.get("departure_date") or "")[:10] or None
        ret = str(a.get("return_date") or "")[:10] or None
        sa = a.get("scraped_at") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        self.buffer.append((
            sa,
            a.get("query_type"),
            a.get("origin_sky_id"),
            a.get("origin_name"),
            a.get("destination_sky_id"),
            a.get("destination_name"),
            a.get("zone"),
            dep, ret,
            a.get("price"),
            a.get("currency", "EUR"),
            a.get("cabin_class", "auto"),
            a.get("stops"),
            a.get("stop_details", "[]"),
            a.get("stop_summary"),
            a.get("airline"),
            a.get("flight_number"),
            a.get("duration_minutes"),
            a.get("is_direct"),
            a.get("score"),
            a.get("tags", "[]"),
        ))
        if len(self.buffer) >= self.BATCH:
            self._flush()
        return item

    def _flush(self):
        if not self.buffer:
            return
        sql = """
            INSERT INTO flights (
                scraped_at, query_type,
                origin_sky_id, origin_name,
                destination_sky_id, destination_name, zone,
                departure_date, return_date,
                price, currency, cabin_class,
                stops, stop_details, stop_summary,
                airline, flight_number, duration_minutes,
                is_direct, score, tags
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            ON CONFLICT (origin_sky_id, destination_sky_id, departure_date, price, airline, cabin_class)
            DO UPDATE SET
                scraped_at       = EXCLUDED.scraped_at,
                stops            = EXCLUDED.stops,
                stop_details     = EXCLUDED.stop_details,
                stop_summary     = EXCLUDED.stop_summary,
                duration_minutes = EXCLUDED.duration_minutes,
                cabin_class      = EXCLUDED.cabin_class,
                score            = EXCLUDED.score,
                tags             = EXCLUDED.tags,
                zone             = EXCLUDED.zone,
                updated_at       = NOW()
        """
        try:
            self.cur.executemany(sql, self.buffer)
            self.conn.commit()
            self.inserted += len(self.buffer)
            logger.info(f"[DB] Flush {len(self.buffer)} rows (total: {self.inserted})")
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"[DB] Erreur flush: {e}")
            self.errors += len(self.buffer)
        finally:
            self.buffer = []

    def close_spider(self, spider):
        self._flush()
        logger.info(f"[DB] Terminé — inserted/updated: {self.inserted}, errors: {self.errors}")
        self.cur.close()
        self.conn.close()
