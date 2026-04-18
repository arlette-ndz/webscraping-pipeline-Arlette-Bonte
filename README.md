# ✈️ Vol Tracker Abidjan — Pipeline Web Scraping

> Surveillance automatisée des prix de billets d'avion **Abidjan (ABJ) **  
---

## 🎓 Informations académiques

| Champ | Valeur |
|-------|--------|
| **Projet** | Projet de Web Scraping |
| **Institution** | ENSEA — AS Data Science |

---

## 🌍 Destinations surveillées 

| Zone | Villes |
|------|--------|
| Afrique Ouest | Dakar, Accra, Lagos, Lomé, Bamako, Ouagadougou, Cotonou, Conakry |
| Afrique Centre | Douala, Yaoundé, Libreville |
| Afrique Est | Nairobi, Addis-Abeba, Niamey |
| Afrique Sud | Johannesburg, Cape Town |
| Afrique Nord | Casablanca, Marrakech, Le Caire |
| Europe | Paris, Bruxelles, Istanbul, Londres, Amsterdam, Francfort, Madrid, Lille |
| Moyen-Orient | Doha, Dubaï, Beyrouth |
| Amériques | New York, Montréal, Washington, Chicago, Miami, Los Angeles, Québec, Ottawa |
| Asie | Guangzhou, Pékin, Mumbai |

---

## 🏗️ Architecture — Niveau ARGENT

```
docker-compose.yml
├── db          (PostgreSQL 15)       → :5432
├── pgadmin     (PgAdmin 4)           → :5050
├── redis       (Redis 7)             → :6379
├── api         (Flask)               → :5000
├── celery_worker (Celery worker)
└── celery_beat   (Celery Beat — planification)
```

### Flux de données
```
Skyscanner API (RapidAPI)
    ↓  [3 endpoints par destination]
Scrapy Spider (skyscanner_vols)
    ↓
Pipeline Scrapy :
  CleaningPipeline   → validation, normalisation
  PostgreSQLPipeline → UPSERT (mise à jour à chaque scrap)
  JsonPipeline       → export data/raw_data.json
    ↓
PostgreSQL : tables vols, destinations, scraping_logs
    ↓
Flask API REST (11 endpoints)
    ↓
Dashboard HTML 2 pages (Chart.js)
```

---

## ⚙️ Installation — 3 étapes

### 1. Cloner et configurer

```bash
git clone https://github.com/[groupe]/webscraping-pipeline-[groupe].git
cd webscraping-pipeline-[groupe]

# Configurer les identifiants
cp .env.example .env
# Ouvrir .env et renseigner :
#   RAPIDAPI_KEY=votre_clé_ici
#   POSTGRES_PASSWORD=votre_mot_de_passe
#   PGADMIN_DEFAULT_EMAIL=votre@email.com
#   PGADMIN_DEFAULT_PASSWORD=votre_mot_de_passe
```

### 2. Lancer

```bash
docker-compose up -d
```

### 3. Accéder

| Service | URL | Description |
|---------|-----|-------------|
| Dashboard | http://localhost:5000 | Vue d'ensemble + tableau |
| Analyse | http://localhost:5000/analyse | Heatmap + évolution prix |
| API | http://localhost:5000/api/health | Vérification |
| PgAdmin | http://localhost:5050 | Interface base de données |

> **PgAdmin** : la base `flights_db` est préconfigurée automatiquement (serveur "Flights DB — ENSEA")

---

## 🔌 API REST — Endpoints

| Méthode | URL | Description |
|---------|-----|-------------|
| GET | `/api/health` | État de l'API et de la base |
| GET | `/api/vols` | Liste des vols (filtres + pagination) |
| GET | `/api/vols/<id>` | Vol par ID |
| GET | `/api/vols/search?query=Paris` | Recherche |
| GET | `/api/destinations` | Stats par destination |
| GET | `/api/stats` | Statistiques complètes |
| GET | `/api/meilleurs-prix?type_vol=oneway` | Meilleurs prix par destination |
| GET | `/api/evolution/<DEST>` | Évolution des prix d'une destination |
| POST | `/api/scrape` | Lancer le scraping (async Celery) |
| GET | `/api/scrape/status/<task_id>` | Statut d'une tâche |

### Paramètres `/api/vols`

```
page          → numéro de page (défaut: 1)
limit         → items par page (max 100)
destination   → filtre par ville/code/pays
continent     → filtre par continent
type_vol      → oneway | return
escales       → 0, 1, 2...
sort          → prix | date_depart | duree_minutes
order         → asc | desc
```

---

## 📅 Dates automatiques

Le spider calcule **automatiquement** les dates à chaque exécution :
- **Aller simple** : J+7 → J+37 (30 jours glissants)
- **Aller-retour** : départ J+7, retour J+21

Aucune date fixe dans le code — toujours à jour.

---

## 🔄 Mise à jour de la base

À chaque scraping, les données existantes sont **mises à jour** (UPSERT) :
```sql
ON CONFLICT (vol_id) DO UPDATE SET
    prix          = EXCLUDED.prix,
    prix_xof      = EXCLUDED.prix_xof,
    compagnie     = EXCLUDED.compagnie,
    escales       = EXCLUDED.escales,
    date_collecte = EXCLUDED.date_collecte,
    updated_at    = CURRENT_TIMESTAMP
```

---

## 📊 Dashboard

### Page 1 — Vue d'ensemble (`/`)
- KPIs : vols collectés, destinations, prix minimum, compagnies
- Graphique prix minimum par destination
- Donut : répartition par continent
- Évolution des collectes dans le temps
- Distribution des escales
- Tableau complet avec filtres (continent, type, escales, recherche)

### Page 2 — Analyse des prix (`/analyse`)
- Cards cliquables par destination (sélection multiple)
- Graphique d'évolution des prix par date de collecte
- Comparaison Min/Moy/Max (top 10)
- Heatmap des prix par date de départ (30 jours)
- Scatter plot : corrélation prix ↔ durée
- Top 15 meilleurs prix en tableau

---

## 🗂️ Structure du projet

```
abidjan_flights/
├── docker-compose.yml
├── .env                         ← Vos identifiants (ne pas committer)
├── .env.example                 ← Template
├── requirements.txt
│
├── flight_scraper/              ← Spider Scrapy
│   ├── scrapy.cfg
│   └── flight_scraper/
│       ├── settings.py          ← Config + connexion DB
│       ├── items.py             ← Modèle VolItem
│       ├── pipelines.py         ← Cleaning + PostgreSQL (upsert) + JSON
│       └── spiders/
│           └── skyscanner_spider.py  ← Spider principal (45 destinations)
│
├── celery_app/
│   ├── __init__.py
│   └── tasks.py                 ← Tâches Celery + Beat (2x/jour)
│
├── api/
│   ├── app.py                   ← Flask API (11 endpoints)
│   └── templates/
│       ├── dashboard.html       ← Page 1 Vue d'ensemble
│       └── analyse.html         ← Page 2 Analyse des prix
│
└── docker/
    ├── Dockerfile.api
    ├── init.sql                 ← Schéma + vues PostgreSQL
    └── pgadmin_servers.json     ← Connexion PgAdmin auto
```

---

## 🛠️ Commandes utiles

```bash
# Voir les logs en temps réel
docker-compose logs -f api
docker-compose logs -f celery_worker

# Accéder à PostgreSQL directement
docker-compose exec db psql -U postgres -d flights_db

# Vérifier les vols collectés
docker-compose exec db psql -U postgres -d flights_db -c "SELECT COUNT(*) FROM vols;"

# Voir les statistiques par destination
docker-compose exec db psql -U postgres -d flights_db -c "SELECT * FROM v_meilleurs_prix LIMIT 10;"

# Lancer le scraping manuellement
curl -X POST http://localhost:5000/api/scrape

# Redémarrer un service
docker-compose restart api
docker-compose restart celery_worker

# Arrêter tout (en gardant les données)
docker-compose down

# Arrêter et supprimer les données
docker-compose down -v
```

---

## ⚖️ Charte éthique

- ✅ Source : API officielle Skyscanner via RapidAPI (pas de scraping direct)
- ✅ User-Agent : `ENSEA Educational Project (konatengolo@ufhb.edu.ci)`
- ✅ Délai entre requêtes : 1.5 secondes minimum
- ✅ Volume limité : MAX_ITEMS_PER_DEST = 10 résultats par destination
- ✅ Aucune donnée personnelle collectée
- ✅ Usage purement académique et éducatif

---

## 🛠️ Technologies

| Composant | Tech | Version |
|-----------|------|---------|
| Scraping | Scrapy | 2.11 |
| HTTP | requests | 2.32 |
| API | Flask + CORS | 3.0 |
| Base de données | PostgreSQL | 15 |
| Driver DB | psycopg2 | 2.9 |
| Nettoyage | pandas | 2.2 |
| Tâches async | Celery | 5.3 |
| Broker | Redis | 7 |
| Conteneurisation | Docker Compose | - |
| Interface DB | PgAdmin 4 | latest |
| Dashboard | HTML5 + Chart.js | 4.4 |

---

*Bon courage à tous ! 🚀*
