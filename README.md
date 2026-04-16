# Pipeline de Scraping de Vols depuis Abidjan

> **Projet de Web Scraping — ENSEA AS Data Science**  
> Niveau visé : 🥈 **ARGENT** + Dashboard bonus

---

## 🎯 Description

Pipeline complet de collecte, nettoyage et stockage des prix de billets d'avion depuis **Abidjan (ABJ)** vers **les destinations populaires** à travers le monde. Les données sont collectées via l'API **Skyscanner (RapidAPI)** avec le spider Scrapy, stockées dans PostgreSQL, exposées via une API Flask, et visualisées dans un dashboard HTML à 2 pages.

---

## 🏗 Architecture 

```
┌─────────────────────────────────────────────────────────────┐
│                        DOCKER COMPOSE                       │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌───────────────────────┐ │
│  │  Scrapy  │───▶│PostgreSQL│◀───│     Flask API         │ │
│  │  Spider  │    │flights_db│    │  /api/data            │ │
│  └──────────┘    └──────────┘    │  /api/stats/*         │ │
│        ▲                         │  /api/scrape/async    │ │
│        │         ┌──────────┐    └───────────────────────┘ │
│  ┌─────┴──────┐  │  Redis   │              │               │
│  │   Celery   │◀─│  Broker  │    ┌─────────▼─────────┐    │
│  │   Worker   │  └──────────┘    │   Nginx + Dashboard│    │
│  └────────────┘                  │   :8080            │    │
│  ┌─────────────┐                 └───────────────────┘    │
│  │ Celery Beat │ (scraping auto toutes les 6h)            │
│  └─────────────┘                                          │
│  ┌─────────────┐                                          │
│  │   pgAdmin   │ :5050                                    │
│  └─────────────┘                                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Structure du Projet

```
flight-pipeline/
├── scraper/                    # Spider Scrapy
│   ├── flight_scraper/
│   │   ├── spiders/
│   │   │   └── skyscanner_spider.py   # Spider principal
│   │   ├── pipelines.py               # Nettoyage + PostgreSQL (UPSERT)
│   │   ├── items.py
│   │   └── settings.py
│   ├── scrapy.cfg
│   ├── requirements.txt
│   └── Dockerfile
├── api/                        # API Flask
│   ├── app/
│   │   ├── __init__.py
│   │   ├── models.py          # Modèle SQLAlchemy
│   │   ├── routes.py          # Endpoints REST
│   │   ├── tasks.py           # Tâches Celery
│   │   └── extensions.py
│   ├── run.py
│   ├── requirements.txt
│   └── Dockerfile
├── dashboard/                  # Dashboard HTML 2 pages
│   ├── index.html             # Vue d'ensemble + tableau de bord
│   └── analysis.html          # Analyse des prix + évolution
├── nginx/
│   └── nginx.conf             # Reverse proxy
├── postgres_init/
│   └── init.sql               # Schéma DB + vue pgAdmin
├── docker-compose.yml
├── .env                       # ⚠ Variables d'environnement (à remplir)
└── README.md
```

---

## ⚡ Démarrage Rapide

### 1. Prérequis

```bash
# Docker + Docker Compose installés
docker --version       # >= 20.x
docker-compose --version  # >= 2.x
```

### 2. Configurer la clé API

Édite le fichier `.env` :

```bash
# Ouvre .env et remplace REMPLACE_PAR_TA_CLE_RAPIDAPI
RAPIDAPI_KEY=ta_vraie_cle_ici
```

> Ta clé RapidAPI Skyscanner se trouve sur : https://rapidapi.com/

### 3. Lancer tous les services

```bash
# Depuis le dossier flight-pipeline/
docker-compose up -d --build
```

### 4. Vérifier que tout tourne

```bash
docker-compose ps
# Tous les services doivent être "running" ou "healthy"
```

### 5. Lancer le premier scraping

```bash
# Option A : via le profil Docker (scraping direct)
docker-compose --profile scrape up scraper

# Option B : via l'API (asynchrone Celery)
curl -X POST http://localhost:8080/api/scrape/async
```

---

## 🌐 Accès aux Services

| Service | URL | Identifiants |
|---------|-----|-------------|
| **Dashboard** | http://localhost:8080 | — |
| **Analyse des prix** | http://localhost:8080/analysis.html | — |
| **API REST** | http://localhost:8080/api/ | — |
| **pgAdmin** | http://localhost:5050 | admin@ensea.ci / admin123 |
| **PostgreSQL** | localhost:**5433** | flights_user / flights_pass |

---

## Si tu as déjà pgAdmin avec flights_db

Si ta base `flights_db` existe déjà dans ton pgAdmin local, modifie `.env` :

```env
# Remplace "postgres" par l'IP de ta machine hôte
# Sur Linux : 172.17.0.1
# Sur Mac/Windows : host.docker.internal
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432   # port de ton postgres local
```

Et commente le service `postgres` dans `docker-compose.yml` :

```yaml
# postgres:           ← commenter tout le bloc
#   image: ...
```

Puis dans pgAdmin, connecte un nouveau serveur :
- **Host** : localhost
- **Port** : 5433 (si tu utilises le postgres Docker)
- **Database** : flights_db
- **Username** : flights_user
- **Password** : flights_pass

---

##  Endpoints API

### Données

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/api/data` | Tous les vols (pagination, filtres) |
| GET | `/api/data/<id>` | Un vol par ID |
| GET | `/api/data/search?query=Paris` | Recherche |

**Paramètres de `/api/data` :**
- `page`, `limit` (défaut 20, max 100)
- `destination` : nom ou skyId
- `is_direct` : true/false
- `min_price`, `max_price`
- `cabin_class`
- `date_from`, `date_to`
- `sort_by` : price, departure_date, destination_name
- `order` : asc/desc

### Statistiques (dashboard)

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/api/stats/summary` | KPIs globaux |
| GET | `/api/stats/by-destination` | Prix par destination |
| GET | `/api/stats/price-evolution?destination=Paris` | Évolution temporelle |
| GET | `/api/stats/stops-distribution` | Directs vs escales |
| GET | `/api/stats/top-airlines` | Top compagnies |

### Scraping

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| POST | `/api/scrape` | Scraping synchrone |
| POST | `/api/scrape/async` | Scraping asynchrone (Celery) |
| GET | `/api/scrape/status/<task_id>` | Statut tâche |
| GET | `/api/health` | Santé de l'API |

---

## 🌍 Destinations Scrapées 

| Zone | Destinations |
|------|-------------|
| Afrique Ouest | Dakar, Accra, Lomé, Bamako, Ouagadougou, Lagos, Cotonou, Conakry |
| Afrique Centre | Douala, Yaoundé, Libreville |
| Afrique Est | Nairobi, Addis-Abeba, Niamey |
| Afrique Sud | Johannesburg, Cape Town |
| Afrique Nord | Casablanca, Marrakech, Le Caire |
| Europe | Paris, Bruxelles, Istanbul, Londres, Amsterdam, Francfort, Madrid, Lille |
| Moyen-Orient | Doha, Dubaï, Beyrouth |
| Amériques | New York, Montréal, Washington, Chicago, Miami, Los Angeles, Québec, Ottawa |
| Asie | Guangzhou, Pékin, Mumbai |

---

## 🔄 Mise à Jour des Données

- **Automatique** : Celery Beat relance le scraping toutes les **6 heures**
- **Manuelle** : `POST /api/scrape/async` depuis le dashboard
- **UPSERT** : chaque scraping met à jour les données existantes sans doublon

---

##  Commandes Utiles

```bash
# Voir les logs en temps réel
docker-compose logs -f api
docker-compose logs -f celery_worker
docker-compose logs -f scraper

# Relancer seulement le scraper
docker-compose --profile scrape up scraper

# Arrêter tous les services
docker-compose down

# Supprimer aussi les volumes (reset complet)
docker-compose down -v

# Accéder à la DB directement
docker-compose exec postgres psql -U flights_user -d flights_db

# Voir les tâches Celery
docker-compose exec celery_worker celery -A app.tasks inspect active
```

---

##  Nettoyage des Données

Le pipeline effectue automatiquement :
- Déduplication via contrainte UNIQUE PostgreSQL
- Normalisation des prix en float (EUR)
- Normalisation des dates en `YYYY-MM-DD`
- Détection automatique de la classe cabine depuis les données API
- Construction du résumé d'escales : `"1 escale (Istanbul)"`, `"Vol direct"`, etc.
- UPSERT : mise à jour si le vol existe déjà, insertion sinon

---


