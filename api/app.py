"""
API REST Flask — Surveillance Vols Abidjan
ENSEA — AS Data Science | Dr N'golo Konate
Architecture Niveau ARGENT
"""

from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
import sys
from datetime import datetime

# Ajout du chemin racine pour les imports Celery
_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)

app = Flask(__name__, template_folder="templates")
CORS(app)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "db"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME",     "flights_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def rows(cur):
    return [dict(r) for r in cur.fetchall()]


def err(msg, code=500):
    return jsonify({"success": False, "error": str(msg)}), code


# ── Pages HTML ────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/analyse")
def analyse():
    return render_template("analyse.html")


# ── Health ────────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM vols")
        n = cur.fetchone()[0]
        cur.close(); conn.close()
        db_ok = True
    except Exception as e:
        n = 0; db_ok = False
    return jsonify({
        "status":    "ok",
        "db":        "connectée" if db_ok else "erreur",
        "vols_en_base": n,
        "timestamp": datetime.now().isoformat(),
    })


# ── Vols — liste paginée avec filtres ─────────────────────────────────────────

@app.route("/api/vols")
def get_vols():
    page        = max(1, request.args.get("page",   1,   type=int))
    limit       = min(100, max(1, request.args.get("limit", 20,  type=int)))
    destination = request.args.get("destination", "")
    continent   = request.args.get("continent",   "")
    type_vol    = request.args.get("type_vol",    "")
    escales     = request.args.get("escales",     None, type=int)
    compagnie   = request.args.get("compagnie",   "")
    sort        = request.args.get("sort",        "prix")
    order       = "DESC" if request.args.get("order", "asc") == "desc" else "ASC"
    offset      = (page - 1) * limit

    allowed_sort = {"prix", "date_depart", "duree_minutes", "date_collecte", "escales"}
    if sort not in allowed_sort:
        sort = "prix"

    conditions = ["1=1"]
    params     = []

    if destination:
        conditions.append("(destination ILIKE %s OR ville_destination ILIKE %s OR pays_destination ILIKE %s)")
        params += [f"%{destination}%"] * 3
    if continent:
        conditions.append("continent ILIKE %s")
        params.append(f"%{continent}%")
    if type_vol:
        conditions.append("type_vol = %s")
        params.append(type_vol)
    if escales is not None:
        conditions.append("escales = %s")
        params.append(escales)
    if compagnie:
        conditions.append("compagnie ILIKE %s")
        params.append(f"%{compagnie}%")

    where = "WHERE " + " AND ".join(conditions)

    try:
        conn  = get_db()
        cur   = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SELECT COUNT(*) AS n FROM vols {where}", params)
        total = cur.fetchone()["n"]
        cur.execute(
            f"SELECT * FROM vols {where} ORDER BY {sort} {order} LIMIT %s OFFSET %s",
            params + [limit, offset]
        )
        vols = rows(cur)
        cur.close(); conn.close()

        # Sérialiser les dates
        for v in vols:
            for k, val in v.items():
                if hasattr(val, "isoformat"):
                    v[k] = val.isoformat()

        return jsonify({
            "success": True,
            "pagination": {
                "page":   page,
                "limit":  limit,
                "total":  total,
                "pages":  max(1, (total + limit - 1) // limit),
            },
            "data": vols,
        })
    except Exception as e:
        return err(e)


# ── Vol par ID ────────────────────────────────────────────────────────────────

@app.route("/api/vols/<int:vol_id>")
def get_vol(vol_id):
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM vols WHERE id = %s", (vol_id,))
        v = cur.fetchone()
        cur.close(); conn.close()
        if not v:
            return err("Vol introuvable", 404)
        d = dict(v)
        for k, val in d.items():
            if hasattr(val, "isoformat"):
                d[k] = val.isoformat()
        return jsonify({"success": True, "data": d})
    except Exception as e:
        return err(e)


# ── Recherche ─────────────────────────────────────────────────────────────────

@app.route("/api/vols/search")
def search_vols():
    q = request.args.get("query", "").strip()
    if not q:
        return err("Paramètre 'query' requis", 400)
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM vols
            WHERE ville_destination ILIKE %s
               OR destination        ILIKE %s
               OR pays_destination   ILIKE %s
               OR compagnie          ILIKE %s
               OR continent          ILIKE %s
            ORDER BY prix ASC LIMIT 50
        """, [f"%{q}%"] * 5)
        vols = rows(cur)
        cur.close(); conn.close()
        for v in vols:
            for k, val in v.items():
                if hasattr(val, "isoformat"):
                    v[k] = val.isoformat()
        return jsonify({"success": True, "count": len(vols), "data": vols})
    except Exception as e:
        return err(e)


# ── Destinations ──────────────────────────────────────────────────────────────

@app.route("/api/destinations")
def get_destinations():
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT code_iata, ville, pays, continent,
                   prix_min, prix_moyen, prix_max, nb_vols,
                   derniere_maj::text
            FROM destinations
            ORDER BY nb_vols DESC
        """)
        dests = rows(cur)
        cur.close(); conn.close()
        return jsonify({"success": True, "count": len(dests), "data": dests})
    except Exception as e:
        return err(e)


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.route("/api/stats")
def get_stats():
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        stats = {}

        cur.execute("SELECT COUNT(*) AS n FROM vols")
        stats["total_vols"] = cur.fetchone()["n"]

        cur.execute("SELECT COUNT(DISTINCT destination) AS n FROM vols")
        stats["total_destinations"] = cur.fetchone()["n"]

        cur.execute("SELECT COUNT(DISTINCT compagnie) AS n FROM vols WHERE compagnie != ''")
        stats["total_compagnies"] = cur.fetchone()["n"]

        cur.execute("SELECT ROUND(AVG(prix)::numeric,2) AS m FROM vols WHERE prix > 0")
        r = cur.fetchone()
        stats["prix_moyen"] = float(r["m"]) if r["m"] else 0

        cur.execute("SELECT MIN(prix) AS m FROM vols WHERE prix > 0")
        r = cur.fetchone()
        stats["prix_min"] = float(r["m"]) if r["m"] else 0

        # Par continent
        cur.execute("""
            SELECT continent, COUNT(*) AS nb_vols,
                   ROUND(MIN(prix)::numeric,2) AS prix_min,
                   ROUND(AVG(prix)::numeric,2) AS prix_moyen
            FROM vols WHERE continent != ''
            GROUP BY continent ORDER BY nb_vols DESC
        """)
        stats["par_continent"] = rows(cur)

        # Par destination (top 20 par nb_vols)
        cur.execute("""
            SELECT destination, ville_destination, pays_destination, continent,
                   COUNT(*)                        AS nb_vols,
                   ROUND(MIN(prix)::numeric,2)     AS prix_min,
                   ROUND(AVG(prix)::numeric,2)     AS prix_moyen,
                   MIN(prix_xof)                   AS prix_min_xof
            FROM vols WHERE ville_destination != ''
            GROUP BY destination, ville_destination, pays_destination, continent
            ORDER BY prix_min ASC
        """)
        stats["par_destination"] = rows(cur)

        # Par compagnie (top 15)
        cur.execute("""
            SELECT compagnie, COUNT(*) AS nb_vols,
                   ROUND(MIN(prix)::numeric,2) AS prix_min,
                   ROUND(AVG(prix)::numeric,2) AS prix_moyen
            FROM vols WHERE compagnie != '' AND compagnie IS NOT NULL
            GROUP BY compagnie ORDER BY nb_vols DESC LIMIT 15
        """)
        stats["par_compagnie"] = rows(cur)

        # Distribution escales
        cur.execute("""
            SELECT escales, COUNT(*) AS nb_vols,
                   ROUND(AVG(prix)::numeric,2) AS prix_moyen
            FROM vols GROUP BY escales ORDER BY escales
        """)
        stats["par_escales"] = rows(cur)

        # Évolution des prix par date de collecte
        cur.execute("""
            SELECT date_collecte::text       AS date,
                   ROUND(MIN(prix)::numeric,2) AS prix_min,
                   ROUND(AVG(prix)::numeric,2) AS prix_moyen,
                   COUNT(*)                   AS nb_vols
            FROM vols
            GROUP BY date_collecte
            ORDER BY date_collecte DESC
            LIMIT 30
        """)
        stats["evolution"] = rows(cur)

        # Derniers logs de scraping
        cur.execute("""
            SELECT task_id, demarrage::text, fin::text,
                   statut, vols_collectes, message
            FROM scraping_logs
            ORDER BY demarrage DESC LIMIT 10
        """)
        stats["scraping_logs"] = rows(cur)

        # Prix min par continent (pour carte)
        cur.execute("""
            SELECT continent,
                   ROUND(MIN(prix)::numeric,2) AS prix_min,
                   COUNT(DISTINCT destination)  AS nb_dest
            FROM vols WHERE continent != ''
            GROUP BY continent
        """)
        stats["carte_continents"] = rows(cur)

        cur.close(); conn.close()
        return jsonify({"success": True, "data": stats})
    except Exception as e:
        return err(e)


# ── Meilleurs prix par destination ────────────────────────────────────────────

@app.route("/api/meilleurs-prix")
def meilleurs_prix():
    type_vol = request.args.get("type_vol", "oneway")
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT destination, ville_destination, pays_destination, continent,
                   type_vol,
                   MIN(prix)                        AS prix_min_usd,
                   MIN(prix_xof)                    AS prix_min_xof,
                   ROUND(AVG(prix)::numeric, 2)     AS prix_moyen_usd,
                   MAX(prix)                        AS prix_max_usd,
                   MIN(escales)                     AS escales_min,
                   COUNT(*)                         AS nb_vols,
                   MAX(date_collecte::text)         AS derniere_collecte
            FROM vols
            WHERE type_vol = %s
            GROUP BY destination, ville_destination, pays_destination, continent, type_vol
            ORDER BY prix_min_usd ASC
        """, (type_vol,))
        data = rows(cur)
        cur.close(); conn.close()
        return jsonify({"success": True, "type_vol": type_vol, "data": data})
    except Exception as e:
        return err(e)


# ── Évolution prix d'une destination ─────────────────────────────────────────

@app.route("/api/evolution/<destination>")
def evolution_destination(destination):
    type_vol = request.args.get("type_vol", "oneway")
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT date_collecte::text           AS date,
                   ROUND(MIN(prix)::numeric,2)   AS prix_min,
                   ROUND(AVG(prix)::numeric,2)   AS prix_moyen,
                   MIN(prix_xof)                 AS prix_min_xof,
                   COUNT(*)                      AS nb_vols
            FROM vols
            WHERE destination = %s AND type_vol = %s
            GROUP BY date_collecte
            ORDER BY date_collecte ASC
        """, (destination.upper(), type_vol))
        data = rows(cur)
        cur.close(); conn.close()
        return jsonify({
            "success":     True,
            "destination": destination.upper(),
            "type_vol":    type_vol,
            "data":        data,
        })
    except Exception as e:
        return err(e)


# ── Scraping — lancer ─────────────────────────────────────────────────────────

@app.route("/api/scrape", methods=["POST"])
def lancer_scraping():
    try:
        from celery_app.tasks import lancer_scraping as task_fn
        task = task_fn.delay()
        return jsonify({
            "success": True,
            "message": "Scraping lancé en arrière-plan",
            "task_id": task.id,
        }), 202
    except Exception as e:
        return err(e)


@app.route("/api/scrape/status/<task_id>")
def statut_scraping(task_id):
    try:
        from celery_app.tasks import celery
        task   = celery.AsyncResult(task_id)
        result = None
        if task.ready():
            try:    result = task.result
            except: result = str(task.result)
        return jsonify({
            "success": True,
            "task_id": task_id,
            "status":  task.state,
            "result":  result,
        })
    except Exception as e:
        return err(e)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=os.getenv("FLASK_ENV") == "development")
