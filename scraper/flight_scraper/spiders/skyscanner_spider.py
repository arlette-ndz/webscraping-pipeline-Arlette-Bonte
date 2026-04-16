import scrapy
import json
import os
from datetime import datetime, timedelta

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "")
BASE_URL = "https://skyscanner-flights-travel-api.p.rapidapi.com/flights"

HEADERS = {
    "Content-Type": "application/json",
    "x-rapidapi-host": "skyscanner-flights-travel-api.p.rapidapi.com",
    "x-rapidapi-key": RAPIDAPI_KEY,
}

ORIGIN_SKY_ID = "ABJ"
ORIGIN_ENTITY_ID = "27537400"

DESTINATIONS = [
    # Afrique Ouest
    {"name": "Dakar",         "zone": "Afrique Ouest",   "skyId": "DKRA", "entityId": "27540289"},
    {"name": "Accra",         "zone": "Afrique Ouest",   "skyId": "ACC",  "entityId": "27537821"},
    {"name": "Lomé",          "zone": "Afrique Ouest",   "skyId": "LFW",  "entityId": "27540547"},
    {"name": "Bamako",        "zone": "Afrique Ouest",   "skyId": "BKO",  "entityId": "27537901"},
    {"name": "Ouagadougou",   "zone": "Afrique Ouest",   "skyId": "OUA",  "entityId": "27539953"},
    {"name": "Lagos",         "zone": "Afrique Ouest",   "skyId": "LAGO", "entityId": "27541699"},
    {"name": "Cotonou",       "zone": "Afrique Ouest",   "skyId": "COO",  "entityId": "27538175"},
    {"name": "Conakry",       "zone": "Afrique Ouest",   "skyId": "CKY",  "entityId": "27538131"},
    # Afrique Centre
    {"name": "Douala",        "zone": "Afrique Centre",  "skyId": "DLA",  "entityId": "27538320"},
    {"name": "Yaoundé",       "zone": "Afrique Centre",  "skyId": "NSI",  "entityId": "27540874"},
    {"name": "Libreville",    "zone": "Afrique Centre",  "skyId": "LBV",  "entityId": "27540481"},
    # Afrique Est
    {"name": "Nairobi",       "zone": "Afrique Est",     "skyId": "NBO",  "entityId": "27536781"},
    {"name": "Addis-Abeba",   "zone": "Afrique Est",     "skyId": "ADD",  "entityId": "27537375"},
    {"name": "Niamey",        "zone": "Afrique",         "skyId": "NIM",  "entityId": "27539762"},
    # Afrique Sud
    {"name": "Johannesburg",  "zone": "Afrique Sud",     "skyId": "JNB",  "entityId": "27536598"},
    {"name": "Cape Town",     "zone": "Afrique Sud",     "skyId": "CPT",  "entityId": "27536547"},
    # Afrique Nord
    {"name": "Casablanca",    "zone": "Afrique Nord",    "skyId": "CASA", "entityId": "27542096"},
    {"name": "Marrakech",     "zone": "Afrique Nord",    "skyId": "RAK",  "entityId": "27540606"},
    {"name": "Le Caire",      "zone": "Afrique Nord",    "skyId": "CAIR", "entityId": "27537634"},
    # Europe
    {"name": "Paris",         "zone": "Europe",          "skyId": "PARI", "entityId": "27539733"},
    {"name": "Bruxelles",     "zone": "Europe",          "skyId": "BRUS", "entityId": "27539562"},
    {"name": "Istanbul",      "zone": "Europe",          "skyId": "ISTA", "entityId": "27540956"},
    {"name": "Londres",       "zone": "Europe",          "skyId": "LOND", "entityId": "27544008"},
    {"name": "Amsterdam",     "zone": "Europe",          "skyId": "AMS",  "entityId": "27537543"},
    {"name": "Francfort",     "zone": "Europe",          "skyId": "FRAN", "entityId": "27537471"},
    {"name": "Madrid",        "zone": "Europe",          "skyId": "MAD",  "entityId": "27544015"},
    {"name": "Lille",         "zone": "Europe",          "skyId": "LIL",  "entityId": "27540494"},
    # Moyen-Orient
    {"name": "Doha",          "zone": "Moyen-Orient",    "skyId": "DOH",  "entityId": "27538261"},
    {"name": "Dubaï",         "zone": "Moyen-Orient",    "skyId": "DXBA", "entityId": "27537925"},
    {"name": "Beyrouth",      "zone": "Moyen-Orient",    "skyId": "BEY",  "entityId": "27537974"},
    # Amériques
    {"name": "New York",      "zone": "Amériques",       "skyId": "NYCA", "entityId": "27537542"},
    {"name": "Montréal",      "zone": "Amériques",       "skyId": "MTRL", "entityId": "27539361"},
    {"name": "Washington",    "zone": "Amériques",       "skyId": "WASA", "entityId": "27537547"},
    {"name": "Chicago",       "zone": "Amériques",       "skyId": "CHIA", "entityId": "27537543"},
    {"name": "Miami",         "zone": "Amériques",       "skyId": "MIAA", "entityId": "27537610"},
    {"name": "Los Angeles",   "zone": "Amériques",       "skyId": "LAXA", "entityId": "27537468"},
    {"name": "Québec",        "zone": "Amériques",       "skyId": "YQB",  "entityId": "27539634"},
    {"name": "Ottawa",        "zone": "Amériques",       "skyId": "YOWA", "entityId": "27537789"},
    # Asie
    {"name": "Guangzhou",     "zone": "Asie",            "skyId": "CAN",  "entityId": "27537543"},
    {"name": "Pékin",         "zone": "Asie",            "skyId": "BEIA", "entityId": "27537471"},
    {"name": "Mumbai",        "zone": "Asie",            "skyId": "BOMB", "entityId": "27537543"},
]


def get_months(n=3):
    """Calcule dynamiquement les n prochains mois depuis aujourd'hui."""
    months = []
    now = datetime.utcnow()
    for i in range(1, n + 1):
        total_month = now.month + i
        year = now.year + (total_month - 1) // 12
        month = (total_month - 1) % 12 + 1
        months.append(f"{year}-{month:02d}")
    return months


def first_day_next_month():
    """Premier jour du mois prochain — dynamique."""
    now = datetime.utcnow()
    if now.month == 12:
        return f"{now.year + 1}-01-01"
    return f"{now.year}-{now.month + 1:02d}-01"


class SkyscannerSpider(scrapy.Spider):
    name = "skyscanner"
    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 15,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": "ENSEA Educational Project - Flight Price Monitor (konatengolo@ufhb.edu.ci)",
        "ITEM_PIPELINES": {
            "flight_scraper.pipelines.CleaningPipeline": 300,
            "flight_scraper.pipelines.PostgresPipeline": 400,
        },
        "FEEDS": {
            "/app/data/raw_data.json": {
                "format": "json",
                "overwrite": False,
                "encoding": "utf8",
            }
        },
        "LOG_LEVEL": "INFO",
    }

    def start_requests(self):
        months = get_months(3)
        limit = int(os.environ.get("SCRAPE_LIMIT", 500))
        count = 0

        # 1. getCheapestOneway : chaque destination × 3 prochains mois
        for dest in DESTINATIONS:
            for month in months:
                if count >= limit:
                    break
                url = (
                    f"{BASE_URL}/getCheapestOneway"
                    f"?originSkyId={ORIGIN_SKY_ID}"
                    f"&destinationSkyId={dest['skyId']}"
                    f"&market=FR&currency=EUR"
                    f"&month={month}"
                )
                yield scrapy.Request(
                    url, headers=HEADERS, callback=self.parse_cheapest,
                    meta={"destination": dest, "month": month},
                    errback=self.handle_error, dont_filter=True,
                )
                count += 1
            if count >= limit:
                break

        # 2. searchFlights détaillé — dates calculées dynamiquement
        from_date = first_day_next_month()
        for dest in DESTINATIONS:
            if count >= limit:
                break
            url = (
                f"{BASE_URL}/searchFlights"
                f"?countryCode=CI&adults=1&childrens=0&infants=0"
                f"&originSkyId={ORIGIN_SKY_ID}"
                f"&originEntityId={ORIGIN_ENTITY_ID}"
                f"&destinationSkyId={dest['skyId']}"
                f"&destinationEntityId={dest['entityId']}"
                f"&date={from_date}"
                f"&market=FR&currency=EUR"
            )
            yield scrapy.Request(
                url, headers=HEADERS, callback=self.parse_search_flights,
                meta={"destination": dest, "departure_date": from_date},
                errback=self.handle_error, dont_filter=True,
            )
            count += 1

    # ────────────────────────────────────────────────────────────────────
    def parse_cheapest(self, response):
        dest = response.meta["destination"]
        now_iso = datetime.utcnow().isoformat()
        if response.status != 200:
            self.logger.warning(f"[{dest['name']}] HTTP {response.status}")
            return
        try:
            data = json.loads(response.text)
        except Exception:
            return

        days = (
            data.get("data", {}).get("flights", {}).get("days")
            or data.get("data", {}).get("days")
            or data.get("days") or []
        )
        self.logger.info(f"[getCheapest][{dest['name']}] {len(days)} jours")

        for day in days:
            price = day.get("price") or day.get("cost") or day.get("amount")
            date_str = day.get("day") or day.get("date") or day.get("departureDate")
            if not price or not date_str:
                continue
            try:
                price = float(price)
            except Exception:
                continue

            stops_raw = day.get("stops")
            stop_details_raw = day.get("stopDetails") or []
            yield {
                "scraped_at": now_iso,
                "query_type": "cheapest_oneway",
                "origin_sky_id": ORIGIN_SKY_ID,
                "origin_name": "Abidjan",
                "destination_sky_id": dest["skyId"],
                "destination_name": dest["name"],
                "zone": dest["zone"],
                "departure_date": str(date_str)[:10],
                "return_date": None,
                "price": price,
                "currency": "EUR",
                "cabin_class": day.get("cabinClass") or "auto",
                "stops": stops_raw,
                "stop_details": json.dumps(stop_details_raw),
                "stop_summary": self._stop_summary(stops_raw, stop_details_raw),
                "airline": day.get("airline") or day.get("carrier"),
                "flight_number": day.get("flightNumber"),
                "duration_minutes": day.get("duration") or day.get("durationInMinutes"),
                "is_direct": (stops_raw == 0) if stops_raw is not None else None,
                "score": None, "tags": "[]",
            }

    # ────────────────────────────────────────────────────────────────────
    def parse_search_flights(self, response):
        dest = response.meta["destination"]
        departure_date = response.meta["departure_date"]
        now_iso = datetime.utcnow().isoformat()
        if response.status != 200:
            self.logger.warning(f"[{dest['name']}] HTTP {response.status}")
            return
        try:
            data = json.loads(response.text)
        except Exception:
            return

        # Pagination
        session_id = data.get("sessionId") or data.get("data", {}).get("sessionId")
        ctx = data.get("data", {}).get("context") or {}
        if session_id and ctx.get("status") == "incomplete":
            yield scrapy.Request(
                f"{BASE_URL}/searchIncomplete?sessionId={session_id}&currency=EUR&countryCode=CI",
                headers=HEADERS, callback=self.parse_search_flights,
                meta=response.meta, errback=self.handle_error, dont_filter=True,
            )

        itineraries = data.get("data", {}).get("itineraries") or data.get("itineraries") or []
        carriers_map = {
            (c.get("id") or c.get("carrierId")): c.get("name", "Unknown")
            for c in (data.get("data", {}).get("carriers") or [])
            if c.get("id") or c.get("carrierId")
        }
        self.logger.info(f"[searchFlights][{dest['name']}] {len(itineraries)} itinéraires")

        for it in itineraries:
            price_info = it.get("price") or {}
            price = price_info.get("raw") or price_info.get("amount") or price_info.get("formatted")
            if isinstance(price, str):
                price = "".join(c for c in price if c.isdigit() or c == ".")
            try:
                price = float(price) if price else None
            except Exception:
                price = None

            legs = it.get("legs") or []
            stops_list, airlines = [], []
            total_stops, duration = 0, 0
            cabin_class = "auto"

            for leg in legs:
                total_stops += leg.get("stopCount", 0) or 0
                duration += leg.get("durationInMinutes", 0) or leg.get("duration", 0) or 0
                for stop in (leg.get("stops") or []):
                    stops_list.append({
                        "airport": stop.get("displayCode") or stop.get("id") or stop.get("iata"),
                        "city": stop.get("name") or stop.get("city") or stop.get("cityName"),
                        "layover_minutes": stop.get("layoverInMinutes") or stop.get("layover"),
                    })
                for seg in (leg.get("segments") or []):
                    op = seg.get("operatingCarrier") or {}
                    cid = op.get("id") or seg.get("carrierId")
                    cname = op.get("name") or carriers_map.get(cid) or "Unknown"
                    if cname not in airlines:
                        airlines.append(cname)
                    if cabin_class == "auto":
                        cabin_class = seg.get("cabinClass") or seg.get("cabin_class") or "auto"

            yield {
                "scraped_at": now_iso,
                "query_type": "search_flights",
                "origin_sky_id": ORIGIN_SKY_ID,
                "origin_name": "Abidjan",
                "destination_sky_id": dest["skyId"],
                "destination_name": dest["name"],
                "zone": dest["zone"],
                "departure_date": departure_date,
                "return_date": None,
                "price": price,
                "currency": "EUR",
                "cabin_class": cabin_class,
                "stops": total_stops,
                "stop_details": json.dumps(stops_list),
                "stop_summary": self._stop_summary(total_stops, stops_list),
                "airline": ", ".join(airlines) if airlines else None,
                "flight_number": it.get("id"),
                "duration_minutes": duration or None,
                "is_direct": total_stops == 0,
                "score": it.get("score"),
                "tags": json.dumps(it.get("tags") or []),
            }

    # ────────────────────────────────────────────────────────────────────
    def _stop_summary(self, stops, stop_details):
        if stops is None:
            return "N/A"
        if stops == 0:
            return "Vol direct"
        cities = []
        if isinstance(stop_details, list):
            for sd in stop_details:
                if isinstance(sd, dict):
                    c = sd.get("city") or sd.get("airport") or ""
                    if c:
                        cities.append(c)
        suffix = f" ({', '.join(cities[:stops])})" if cities else ""
        return f"{stops} escale{'s' if stops > 1 else ''}{suffix}"

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.value} | {failure.request.url}")
