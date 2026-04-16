from flask import Blueprint, jsonify, request
from .models import Flight
from .extensions import db
from sqlalchemy import func, desc, asc
import json

flights_bp = Blueprint("flights", __name__)
scrape_bp = Blueprint("scrape", __name__)
stats_bp = Blueprint("stats", __name__)


# ======================================================================
# FLIGHTS ENDPOINTS
# ======================================================================

@flights_bp.route("/data", methods=["GET"])
def get_all_flights():
    """
    GET /api/data
    Paramètres : page, limit, destination, origin, is_direct, min_price, max_price, sort_by, order
    """
    page = request.args.get("page", 1, type=int)
    limit = request.args.get("limit", 20, type=int)
    limit = min(limit, 100)

    q = Flight.query

    # Filtres
    destination = request.args.get("destination")
    if destination:
        q = q.filter(
            (Flight.destination_name.ilike(f"%{destination}%")) |
            (Flight.destination_sky_id.ilike(f"%{destination}%"))
        )

    origin = request.args.get("origin")
    if origin:
        q = q.filter(
            (Flight.origin_name.ilike(f"%{origin}%")) |
            (Flight.origin_sky_id.ilike(f"%{origin}%"))
        )

    is_direct = request.args.get("is_direct")
    if is_direct is not None:
        q = q.filter(Flight.is_direct == (is_direct.lower() == "true"))

    min_price = request.args.get("min_price", type=float)
    if min_price is not None:
        q = q.filter(Flight.price >= min_price)

    max_price = request.args.get("max_price", type=float)
    if max_price is not None:
        q = q.filter(Flight.price <= max_price)

    cabin_class = request.args.get("cabin_class")
    if cabin_class:
        q = q.filter(Flight.cabin_class.ilike(f"%{cabin_class}%"))

    date_from = request.args.get("date_from")
    if date_from:
        q = q.filter(Flight.departure_date >= date_from)

    date_to = request.args.get("date_to")
    if date_to:
        q = q.filter(Flight.departure_date <= date_to)

    # Tri
    sort_by = request.args.get("sort_by", "price")
    order = request.args.get("order", "asc")
    sort_col = getattr(Flight, sort_by, Flight.price)
    q = q.order_by(asc(sort_col) if order == "asc" else desc(sort_col))

    # Pagination
    paginated = q.paginate(page=page, per_page=limit, error_out=False)

    return jsonify({
        "status": "success",
        "data": [f.to_dict() for f in paginated.items],
        "pagination": {
            "page": page,
            "limit": limit,
            "total": paginated.total,
            "pages": paginated.pages,
            "has_next": paginated.has_next,
            "has_prev": paginated.has_prev,
        }
    })


@flights_bp.route("/data/<int:flight_id>", methods=["GET"])
def get_flight(flight_id):
    """GET /api/data/<id>"""
    flight = Flight.query.get_or_404(flight_id)
    return jsonify({"status": "success", "data": flight.to_dict()})


@flights_bp.route("/data/search", methods=["GET"])
def search_flights():
    """GET /api/data/search?query=Paris&page=1&limit=10"""
    query = request.args.get("query", "")
    page = request.args.get("page", 1, type=int)
    limit = request.args.get("limit", 20, type=int)
    limit = min(limit, 100)

    if not query:
        return jsonify({"status": "error", "message": "Paramètre 'query' requis"}), 400

    q = Flight.query.filter(
        (Flight.destination_name.ilike(f"%{query}%")) |
        (Flight.origin_name.ilike(f"%{query}%")) |
        (Flight.airline.ilike(f"%{query}%")) |
        (Flight.destination_sky_id.ilike(f"%{query}%"))
    )

    paginated = q.order_by(Flight.price.asc()).paginate(
        page=page, per_page=limit, error_out=False
    )

    return jsonify({
        "status": "success",
        "query": query,
        "data": [f.to_dict() for f in paginated.items],
        "pagination": {
            "page": page,
            "limit": limit,
            "total": paginated.total,
            "pages": paginated.pages,
        }
    })


# ======================================================================
# STATS ENDPOINTS (pour le dashboard)
# ======================================================================

@stats_bp.route("/stats/summary", methods=["GET"])
def stats_summary():
    """Statistiques globales."""
    total = Flight.query.count()
    direct = Flight.query.filter_by(is_direct=True).count()
    with_stops = Flight.query.filter(Flight.stops > 0).count()

    price_stats = db.session.query(
        func.min(Flight.price),
        func.max(Flight.price),
        func.avg(Flight.price),
    ).first()

    return jsonify({
        "status": "success",
        "data": {
            "total_flights": total,
            "direct_flights": direct,
            "flights_with_stops": with_stops,
            "min_price": float(price_stats[0]) if price_stats[0] else None,
            "max_price": float(price_stats[1]) if price_stats[1] else None,
            "avg_price": round(float(price_stats[2]), 2) if price_stats[2] else None,
        }
    })


@stats_bp.route("/stats/by-destination", methods=["GET"])
def stats_by_destination():
    """Prix moyen, min, max par destination."""
    rows = db.session.query(
        Flight.destination_name,
        Flight.destination_sky_id,
        func.count(Flight.id).label("count"),
        func.min(Flight.price).label("min_price"),
        func.max(Flight.price).label("max_price"),
        func.avg(Flight.price).label("avg_price"),
        func.sum(db.cast(Flight.is_direct, db.Integer)).label("direct_count"),
    ).group_by(
        Flight.destination_name, Flight.destination_sky_id
    ).order_by(func.avg(Flight.price).asc()).all()

    return jsonify({
        "status": "success",
        "data": [
            {
                "destination": r.destination_name,
                "sky_id": r.destination_sky_id,
                "count": r.count,
                "min_price": float(r.min_price) if r.min_price else None,
                "max_price": float(r.max_price) if r.max_price else None,
                "avg_price": round(float(r.avg_price), 2) if r.avg_price else None,
                "direct_count": int(r.direct_count) if r.direct_count else 0,
            }
            for r in rows
        ]
    })


@stats_bp.route("/stats/price-evolution", methods=["GET"])
def price_evolution():
    """Évolution des prix par date de départ pour une destination."""
    destination = request.args.get("destination", "Paris")
    rows = db.session.query(
        Flight.departure_date,
        func.min(Flight.price).label("min_price"),
        func.avg(Flight.price).label("avg_price"),
        func.count(Flight.id).label("count"),
    ).filter(
        Flight.destination_name.ilike(f"%{destination}%"),
        Flight.price.isnot(None),
    ).group_by(
        Flight.departure_date
    ).order_by(Flight.departure_date.asc()).all()

    return jsonify({
        "status": "success",
        "destination": destination,
        "data": [
            {
                "date": str(r.departure_date),
                "min_price": float(r.min_price) if r.min_price else None,
                "avg_price": round(float(r.avg_price), 2) if r.avg_price else None,
                "count": r.count,
            }
            for r in rows
        ]
    })


@stats_bp.route("/stats/stops-distribution", methods=["GET"])
def stops_distribution():
    """Répartition des vols directs vs avec escale(s)."""
    rows = db.session.query(
        Flight.stops,
        func.count(Flight.id).label("count"),
        func.avg(Flight.price).label("avg_price"),
    ).filter(Flight.stops.isnot(None)).group_by(Flight.stops).order_by(Flight.stops.asc()).all()

    return jsonify({
        "status": "success",
        "data": [
            {
                "stops": r.stops,
                "label": "Direct" if r.stops == 0 else f"{r.stops} escale{'s' if r.stops > 1 else ''}",
                "count": r.count,
                "avg_price": round(float(r.avg_price), 2) if r.avg_price else None,
            }
            for r in rows
        ]
    })


@stats_bp.route("/stats/top-airlines", methods=["GET"])
def top_airlines():
    """Top compagnies aériennes par nombre de vols."""
    rows = db.session.query(
        Flight.airline,
        func.count(Flight.id).label("count"),
        func.avg(Flight.price).label("avg_price"),
        func.min(Flight.price).label("min_price"),
    ).filter(
        Flight.airline.isnot(None),
        Flight.airline != "",
    ).group_by(
        Flight.airline
    ).order_by(func.count(Flight.id).desc()).limit(10).all()

    return jsonify({
        "status": "success",
        "data": [
            {
                "airline": r.airline,
                "count": r.count,
                "avg_price": round(float(r.avg_price), 2) if r.avg_price else None,
                "min_price": float(r.min_price) if r.min_price else None,
            }
            for r in rows
        ]
    })


# ======================================================================
# SCRAPE ENDPOINTS
# ======================================================================

@scrape_bp.route("/scrape", methods=["POST"])
def scrape_sync():
    """POST /api/scrape — Lance le scraping (synchrone, pour le niveau Bronze)."""
    import subprocess
    import os
    try:
        result = subprocess.run(
            ["scrapy", "crawl", "skyscanner"],
            cwd="/scraper",
            capture_output=True,
            text=True,
            timeout=300,
            env={**os.environ},
        )
        return jsonify({
            "status": "success" if result.returncode == 0 else "error",
            "message": "Scraping terminé" if result.returncode == 0 else "Erreur scraping",
            "stdout": result.stdout[-2000:],
            "stderr": result.stderr[-1000:],
        })
    except subprocess.TimeoutExpired:
        return jsonify({"status": "timeout", "message": "Scraping trop long, utiliser /scrape/async"}), 408
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@scrape_bp.route("/scrape/async", methods=["POST"])
def scrape_async():
    """POST /api/scrape/async — Lance le scraping via Celery."""
    from .tasks import run_scraping_task
    body = request.get_json(silent=True) or {}
    task = run_scraping_task.delay()
    return jsonify({
        "status": "queued",
        "task_id": task.id,
        "message": "Scraping lancé en arrière-plan",
        "status_url": f"/api/scrape/status/{task.id}",
    }), 202


@scrape_bp.route("/scrape/status/<task_id>", methods=["GET"])
def scrape_status(task_id):
    """GET /api/scrape/status/<task_id> — Statut d'une tâche Celery."""
    from .tasks import celery
    task = celery.AsyncResult(task_id)
    return jsonify({
        "task_id": task_id,
        "status": task.status,
        "result": task.result if task.ready() else None,
    })


@scrape_bp.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "flight-pipeline-api"})
