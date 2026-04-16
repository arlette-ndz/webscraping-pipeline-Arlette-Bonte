from .extensions import db
from datetime import datetime


class Flight(db.Model):
    __tablename__ = "flights"

    id = db.Column(db.Integer, primary_key=True)
    scraped_at = db.Column(db.DateTime)
    query_type = db.Column(db.String(50))
    origin_sky_id = db.Column(db.String(10))
    origin_name = db.Column(db.String(100))
    destination_sky_id = db.Column(db.String(10))
    destination_name = db.Column(db.String(100))
    departure_date = db.Column(db.Date)
    return_date = db.Column(db.Date)
    price = db.Column(db.Numeric(10, 2))
    currency = db.Column(db.String(5))
    cabin_class = db.Column(db.String(50))
    stops = db.Column(db.Integer)
    stop_details = db.Column(db.Text)
    stop_summary = db.Column(db.String(300))
    airline = db.Column(db.String(200))
    flight_number = db.Column(db.String(100))
    duration_minutes = db.Column(db.Integer)
    is_direct = db.Column(db.Boolean)
    score = db.Column(db.Numeric(10, 4))
    tags = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        import json
        stop_details = []
        if self.stop_details:
            try:
                stop_details = json.loads(self.stop_details)
            except Exception:
                stop_details = []

        tags = []
        if self.tags:
            try:
                tags = json.loads(self.tags)
            except Exception:
                tags = []

        # Déterminer le résumé escales
        if self.is_direct:
            stop_summary = "Vol direct"
        elif self.stops == 1:
            city = stop_details[0].get("city", "N/A") if stop_details else "N/A"
            stop_summary = f"1 escale ({city})"
        elif self.stops and self.stops >= 2:
            cities = ", ".join([s.get("city", "") for s in stop_details[:2] if s.get("city")])
            stop_summary = f"{self.stops} escales ({cities})"
        else:
            stop_summary = self.stop_summary or "N/A"

        return {
            "id": self.id,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
            "query_type": self.query_type,
            "origin": {
                "sky_id": self.origin_sky_id,
                "name": self.origin_name,
            },
            "destination": {
                "sky_id": self.destination_sky_id,
                "name": self.destination_name,
            },
            "departure_date": str(self.departure_date) if self.departure_date else None,
            "return_date": str(self.return_date) if self.return_date else None,
            "price": float(self.price) if self.price else None,
            "currency": self.currency,
            "cabin_class": self.cabin_class,
            "stops": self.stops,
            "stop_details": stop_details,
            "stop_summary": stop_summary,
            "airline": self.airline,
            "flight_number": self.flight_number,
            "duration_minutes": self.duration_minutes,
            "is_direct": self.is_direct,
            "score": float(self.score) if self.score else None,
            "tags": tags,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
