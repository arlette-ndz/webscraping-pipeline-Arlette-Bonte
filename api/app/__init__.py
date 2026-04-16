from flask import Flask
from flask_cors import CORS
from .extensions import db
from .routes import flights_bp, scrape_bp, stats_bp


def create_app():
    app = Flask(__name__)
    CORS(app)

    import os
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        f"postgresql://{os.environ.get('POSTGRES_USER', 'flights_user')}"
        f":{os.environ.get('POSTGRES_PASSWORD', 'flights_pass')}"
        f"@{os.environ.get('POSTGRES_HOST', 'postgres')}"
        f":{os.environ.get('POSTGRES_PORT', '5432')}"
        f"/{os.environ.get('POSTGRES_DB', 'flights_db')}"
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["CELERY_BROKER_URL"] = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
    app.config["CELERY_RESULT_BACKEND"] = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

    db.init_app(app)

    with app.app_context():
        db.create_all()

    app.register_blueprint(flights_bp, url_prefix="/api")
    app.register_blueprint(scrape_bp, url_prefix="/api")
    app.register_blueprint(stats_bp, url_prefix="/api")

    return app
