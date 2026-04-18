"""
Pipelines Scrapy
1. CleaningPipeline    : valide et normalise
2. PostgreSQLPipeline  : upsert (INSERT … ON CONFLICT DO UPDATE)
3. JsonPipeline        : export JSON brut
"""

import json
import os
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from itemadapter import ItemAdapter

logger = logging.getLogger(__name__)

USD_TO_XOF = 620


# ── 1. Nettoyage ─────────────────────────────────────────────────────────────

class CleaningPipeline:

    def process_item(self, item, spider):
        if item is None:
            return None
        a = ItemAdapter(item)

        # Prix obligatoire > 0
        prix = a.get("prix")
        try:
            prix = float(prix)
        except (TypeError, ValueError):
            return None
        if prix <= 0 or prix > 50000:
            return None
        a["prix"]    = round(prix, 2)
        a["prix_xof"] = round(prix * USD_TO_XOF)

        # Destination obligatoire
        if not a.get("destination"):
            return None

        # Nettoyage chaînes
        for f in ["destination", "origine"]:
            v = a.get(f)
            if v:
                a[f] = str(v).strip().upper()

        for f in ["ville_destination", "pays_destination", "continent",
                  "compagnie", "classe_cabine", "villes_escale"]:
            v = a.get(f)
            a[f] = str(v).strip() if v else ""

        # Escales : entier >= 0
        try:
            a["escales"] = max(0, int(float(a.get("escales") or 0)))
        except (TypeError, ValueError):
            a["escales"] = 0

        # Durée : entier >= 0
        try:
            a["duree_minutes"] = max(0, int(float(a.get("duree_minutes") or 0)))
        except (TypeError, ValueError):
            a["duree_minutes"] = 0

        # type_vol
        tv = str(a.get("type_vol") or "oneway").lower()
        a["type_vol"] = "return" if "ret" in tv else "oneway"

        # Dates
        for f in ["date_depart", "date_arrivee"]:
            v = a.get(f)
            if v and str(v) not in ("None", "nan", ""):
                a[f] = str(v)[:10]
            else:
                a[f] = None

        # Collecte
        if not a.get("date_collecte"):
            a["date_collecte"] = datetime.now().strftime("%Y-%m-%d")
        if not a.get("heure_collecte"):
            a["heure_collecte"] = datetime.now().strftime("%H:%M:%S")

        return item


# ── 2. PostgreSQL ─────────────────────────────────────────────────────────────

class PostgreSQLPipeline:

    def __init__(self, db_settings):
        self.db_settings = db_settings
        self.conn        = None
        self.cursor      = None
        self.inserted    = 0
        self.updated     = 0

    @classmethod
    def from_crawler(cls, crawler):
        return cls(db_settings=crawler.settings.get("DATABASE"))

    def open_spider(self, spider):
        try:
            self.conn   = psycopg2.connect(**self.db_settings)
            self.cursor = self.conn.cursor()
            logger.info("Connexion PostgreSQL établie")
        except Exception as e:
            logger.error(f"Erreur connexion PostgreSQL : {e}")
            self.conn = None

    def process_item(self, item, spider):
        if not self.conn or item is None:
            return item
        a = ItemAdapter(item)
        try:
            # UPSERT : met à jour le prix si le vol existe déjà
            self.cursor.execute("""
                INSERT INTO vols (
                    vol_id, origine, destination,
                    ville_origine, ville_destination, pays_destination, continent,
                    date_depart, date_arrivee, duree_minutes,
                    heure_depart, heure_arrivee,
                    prix, devise, prix_xof,
                    compagnie, classe_cabine,
                    escales, villes_escale, type_vol,
                    date_collecte, heure_collecte, source_endpoint
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s
                )
                ON CONFLICT (vol_id) DO UPDATE SET
                    prix          = EXCLUDED.prix,
                    prix_xof      = EXCLUDED.prix_xof,
                    compagnie     = EXCLUDED.compagnie,
                    classe_cabine = EXCLUDED.classe_cabine,
                    escales       = EXCLUDED.escales,
                    villes_escale = EXCLUDED.villes_escale,
                    duree_minutes = EXCLUDED.duree_minutes,
                    heure_depart  = EXCLUDED.heure_depart,
                    heure_arrivee = EXCLUDED.heure_arrivee,
                    date_collecte = EXCLUDED.date_collecte,
                    heure_collecte= EXCLUDED.heure_collecte,
                    updated_at    = CURRENT_TIMESTAMP
            """, (
                a.get("vol_id"),
                a.get("origine"),
                a.get("destination"),
                a.get("ville_origine"),
                a.get("ville_destination"),
                a.get("pays_destination"),
                a.get("continent"),
                a.get("date_depart"),
                a.get("date_arrivee"),
                a.get("duree_minutes"),
                a.get("heure_depart"),
                a.get("heure_arrivee"),
                a.get("prix"),
                a.get("devise", "USD"),
                a.get("prix_xof"),
                a.get("compagnie"),
                a.get("classe_cabine"),
                a.get("escales"),
                a.get("villes_escale"),
                a.get("type_vol"),
                a.get("date_collecte"),
                a.get("heure_collecte"),
                a.get("source_endpoint"),
            ))
            self.conn.commit()

            # Mettre à jour la table destinations
            self.cursor.execute("""
                INSERT INTO destinations
                    (code_iata, ville, pays, continent, prix_min, prix_moyen, prix_max, nb_vols, derniere_maj)
                VALUES (%s,%s,%s,%s,%s,%s,%s,1,%s)
                ON CONFLICT (code_iata) DO UPDATE SET
                    prix_min    = LEAST(destinations.prix_min, EXCLUDED.prix_min),
                    prix_max    = GREATEST(destinations.prix_max, EXCLUDED.prix_max),
                    prix_moyen  = ROUND(
                        ((destinations.prix_moyen * destinations.nb_vols) + EXCLUDED.prix_min)
                        / (destinations.nb_vols + 1)::numeric, 2
                    ),
                    nb_vols     = destinations.nb_vols + 1,
                    derniere_maj= EXCLUDED.derniere_maj
            """, (
                a.get("destination"),
                a.get("ville_destination"),
                a.get("pays_destination"),
                a.get("continent"),
                a.get("prix"),
                a.get("prix"),
                a.get("prix"),
                a.get("date_collecte"),
            ))
            self.conn.commit()
            self.inserted += 1

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Erreur insertion vol {a.get('vol_id', '?')} : {e}")
        return item

    def close_spider(self, spider):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info(f"PostgreSQL : {self.inserted} vols insérés/mis à jour")


# ── 3. JSON ───────────────────────────────────────────────────────────────────

class JsonPipeline:

    def __init__(self):
        os.makedirs("data", exist_ok=True)
        self.vols = []

    def process_item(self, item, spider):
        if item is None:
            return item
        self.vols.append(dict(ItemAdapter(item)))
        return item

    def close_spider(self, spider):
        now = datetime.now().isoformat()
        output = {
            "meta": {
                "total":      len(self.vols),
                "generated":  now,
                "source":     "Skyscanner via RapidAPI",
                "origine":    "Abidjan (ABJ)",
            },
            "vols": self.vols,
        }
        with open("data/raw_data.json", "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"JSON sauvegardé : {len(self.vols)} vols → data/raw_data.json")
