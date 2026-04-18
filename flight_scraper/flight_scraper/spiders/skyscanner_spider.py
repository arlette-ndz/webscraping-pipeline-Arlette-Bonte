"""
Spider Skyscanner — Vols depuis Abidjan (ABJ)
Paramètres VALIDÉS par test réel (73 vols retournés ABJ->PARI) :
  originSkyId=ABJ, originEntityId=27544008, market=CI, currency=XOF

Dates AUTOMATIQUES glissantes — aucune date fixe dans le code.
45 destinations avec sky_id et entity_id corrects.

"""

import scrapy
import json
import os
import re
import hashlib
from datetime import datetime, timedelta
from flight_scraper.items import VolItem

XOF_TO_USD = 1 / 620.0   # 1 USD ≈ 620 XOF

# sky_id     = SkyId Skyscanner (PARI, LOND, DXBA...)
# code       = IATA pour affichage (CDG, LHR, DXB...)
# entity_id  = EntityId interne Skyscanner
DESTINATIONS = [
    # Afrique Ouest
    #  {"sky_id":"DSS",  "code":"DKR",  "entity_id":"27538633","ville":"Dakar",        "pays":"Sénégal",             "continent":"Afrique Ouest"},
    #  {"sky_id":"ACC",  "code":"ACC",  "entity_id":"27537902","ville":"Accra",         "pays":"Ghana",               "continent":"Afrique Ouest"},
    # {"sky_id":"LFW",  "code":"LFW",  "entity_id":"27542020","ville":"Lomé",          "pays":"Togo",                "continent":"Afrique Ouest"},
    # {"sky_id":"BKO",  "code":"BKO",  "entity_id":"27541179","ville":"Bamako",        "pays":"Mali",                "continent":"Afrique Ouest"},
    # {"sky_id":"OUA",  "code":"OUA",  "entity_id":"27537685","ville":"Ouagadougou",   "pays":"Burkina Faso",        "continent":"Afrique Ouest"},
    # {"sky_id":"LOS",  "code":"LOS",  "entity_id":"27539773","ville":"Lagos",         "pays":"Nigeria",             "continent":"Afrique Ouest"},
    # {"sky_id":"COO",  "code":"COO",  "entity_id":"27537440","ville":"Cotonou",       "pays":"Bénin",               "continent":"Afrique Ouest"},
    # {"sky_id":"CKY",  "code":"CKY",  "entity_id":"27538097","ville":"Conakry",       "pays":"Guinée",              "continent":"Afrique Ouest"},
    # # Afrique Centrale
    #  {"sky_id":"DLA",  "code":"DLA",  "entity_id":"27538208","ville":"Douala",        "pays":"Cameroun",            "continent":"Afrique Centre"},
    #  {"sky_id":"YAO",  "code":"YAO",  "entity_id":"27542609","ville":"Yaoundé",       "pays":"Cameroun",            "continent":"Afrique Centre"},
    # {"sky_id":"LBV",  "code":"LBV",  "entity_id":"27539271","ville":"Libreville",    "pays":"Gabon",               "continent":"Afrique Centre"},
    # # Afrique Est
    # {"sky_id":"NBO",  "code":"NBO",  "entity_id":"27541925","ville":"Nairobi",       "pays":"Kenya",               "continent":"Afrique Est"},
    # {"sky_id":"ADD",  "code":"ADD",  "entity_id":"27537436","ville":"Addis-Abeba",   "pays":"Éthiopie",            "continent":"Afrique Est"},
    #  {"sky_id":"NIM",  "code":"NIM",  "entity_id":"27541885","ville":"Niamey",        "pays":"Niger",               "continent":"Afrique"},
    # # Afrique Sud
    # {"sky_id":"JNB",  "code":"JNB",  "entity_id":"27539716","ville":"Johannesburg",  "pays":"Afrique du Sud",      "continent":"Afrique Sud"},
    # {"sky_id":"CPT",  "code":"CPT",  "entity_id":"27537980","ville":"Cape Town",     "pays":"Afrique du Sud",      "continent":"Afrique Sud"},
    # # Afrique Nord
    # {"sky_id":"CMN",  "code":"CMN",  "entity_id":"27541910","ville":"Casablanca",    "pays":"Maroc",               "continent":"Afrique Nord"},
    #  {"sky_id":"RAK",  "code":"RAK",  "entity_id":"27542167","ville":"Marrakech",     "pays":"Maroc",               "continent":"Afrique Nord"},
    # {"sky_id":"CAI",  "code":"CAI",  "entity_id":"27537693","ville":"Le Caire",      "pays":"Égypte",              "continent":"Afrique Nord"},
    # Europe
    #  {"sky_id":"PARI", "code":"CDG",  "entity_id":"27544001","ville":"Paris",         "pays":"France",              "continent":"Europe"},
#     {"sky_id":"BRU",  "code":"BRU",  "entity_id":"95673415","ville":"Bruxelles",     "pays":"Belgique",            "continent":"Europe"},
#     {"sky_id":"ISTA", "code":"IST",  "entity_id":"27537533","ville":"Istanbul",      "pays":"Turquie",             "continent":"Europe"},
#     {"sky_id":"LOND", "code":"LHR",  "entity_id":"27544008","ville":"Londres",       "pays":"Royaume-Uni",         "continent":"Europe"},
#     {"sky_id":"AMS",  "code":"AMS",  "entity_id":"95565065","ville":"Amsterdam",     "pays":"Pays-Bas",            "continent":"Europe"},
#     {"sky_id":"FRA",  "code":"FRA",  "entity_id":"27538586","ville":"Francfort",     "pays":"Allemagne",           "continent":"Europe"},
#     {"sky_id":"MAD",  "code":"MAD",  "entity_id":"27540164","ville":"Madrid",        "pays":"Espagne",             "continent":"Europe"},
#     {"sky_id":"LIL",  "code":"LIL",  "entity_id":"27539508","ville":"Lille",         "pays":"France",              "continent":"Europe"},
#     # Moyen-Orient
#     {"sky_id":"DOH",  "code":"DOH",  "entity_id":"27538090","ville":"Doha",          "pays":"Qatar",               "continent":"Moyen-Orient"},
    #  {"sky_id":"DXBA", "code":"DXB",  "entity_id":"27544336","ville":"Dubaï",         "pays":"Émirats arabes unis", "continent":"Moyen-Orient"},
#     {"sky_id":"BEY",  "code":"BEY",  "entity_id":"27537618","ville":"Beyrouth",      "pays":"Liban",               "continent":"Moyen-Orient"},
#     # Amériques
    # {"sky_id":"NYCA", "code":"JFK",  "entity_id":"27537542","ville":"New York",      "pays":"États-Unis",          "continent":"Amériques"},
#     {"sky_id":"YUL",  "code":"YUL",  "entity_id":"27541621","ville":"Montréal",      "pays":"Canada",              "continent":"Amériques"},
    # {"sky_id":"WAS",  "code":"IAD",  "entity_id":"27536960","ville":"Washington",    "pays":"États-Unis",          "continent":"Amériques"},
#     {"sky_id":"CHI",  "code":"ORD",  "entity_id":"27536823","ville":"Chicago",       "pays":"États-Unis",          "continent":"Amériques"},
    # {"sky_id":"MIA",  "code":"MIA",  "entity_id":"27536717","ville":"Miami",         "pays":"États-Unis",          "continent":"Amériques"},
    # {"code": "IAD", "ville": "Washington", "pays": "États-Unis", "continent": "Amériques"}
    #  {"sky_id":"LAXA", "code":"LAX",  "entity_id":"27536492","ville":"Los Angeles",   "pays":"États-Unis",          "continent":"Amériques"},
#     {"sky_id":"YQB",  "code":"YQB",  "entity_id":"27541619","ville":"Québec",        "pays":"Canada",              "continent":"Amériques"},
#     {"sky_id":"YOW",  "code":"YOW",  "entity_id":"27541620","ville":"Ottawa",        "pays":"Canada",              "continent":"Amériques"},
#     # Asie
    #  {"sky_id":"CAN",  "code":"CAN",  "entity_id":"27537921","ville":"Guangzhou",     "pays":"Chine",               "continent":"Asie"},
#     {"sky_id":"BEJB", "code":"PEK",  "entity_id":"27536875","ville":"Pékin",         "pays":"Chine",               "continent":"Asie"},
#     {"sky_id":"BOM",  "code":"BOM",  "entity_id":"27537663","ville":"Mumbai",        "pays":"Inde",                "continent":"Asie"},
#     # Côte d'Ivoire (vols intérieurs)
#     {"sky_id":"SPY",  "code":"SPY",  "entity_id":"27542010","ville":"San-Pédro",     "pays":"Côte d'Ivoire",       "continent":"Côte d'Ivoire"},
#     {"sky_id":"HGO",  "code":"HGO",  "entity_id":"27538731","ville":"Korhogo",       "pays":"Côte d'Ivoire",       "continent":"Côte d'Ivoire"},
#     {"sky_id":"BYK",  "code":"BYK",  "entity_id":"27537720","ville":"Bouaké",        "pays":"Côte d'Ivoire",       "continent":"Côte d'Ivoire"},
#     {"sky_id":"KEO",  "code":"KEO",  "entity_id":"27539176","ville":"Odienné",       "pays":"Côte d'Ivoire",       "continent":"Côte d'Ivoire"},
#  {"sky_id":"RIO","code":"RIO","entity_id":"27537543","ville":"Rio de Janeiro","pays":"Brésil","continent":"Amérique du Sud"}
       {"sky_id": "RIO", "code": "RIO", "entity_id": "27537543", "ville": "Rio de Janeiro", "pays": "Brésil", "continent": "Amérique du Sud"},

  ]

ORIGIN_SKY_ID    = "ABJ"       # VALIDE - confirmé par 73 résultats
ORIGIN_ENTITY_ID = "27544008"  # VALIDE - confirmé par 73 résultats
MARKET           = "CI"
CURRENCY         = "XOF"
BASE_URL         = "https://skyscanner-flights-travel-api.p.rapidapi.com/flights"


class SkyscannerSpider(scrapy.Spider):
    name            = "skyscanner_vols"
    allowed_domains = ["skyscanner-flights-travel-api.p.rapidapi.com"]
    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = os.getenv("RAPIDAPI_KEY", "")
        self.now     = datetime.now()
        # Dates glissantes automatiques
        # self.date_depart  = (self.now + timedelta(days=30)).strftime("%Y-%m-%d")
        # self.date_retour  = (self.now + timedelta(days=44)).strftime("%Y-%m-%d")
        self.mois_courant = (self.now + timedelta(days=30)).strftime("%Y-%m")
        self.mois_suivant = (self.now + timedelta(days=60)).strftime("%Y-%m")
        self.date_depart = "2026-08-01"
        self.date_retour = "2026-08-30"
        self.items_count  = 0
        self.errors       = 0
        self.logger.info(
            f"ABJ -> {len(DESTINATIONS)} destinations | depart={self.date_depart} "
            f"| retour={self.date_retour} | mois={self.mois_courant},{self.mois_suivant}"
        )

    @property
    def _headers(self):
        return {
            "x-rapidapi-key":  self.api_key,
            "x-rapidapi-host": "skyscanner-flights-travel-api.p.rapidapi.com",
            "Content-Type":    "application/json",
            "User-Agent":      "ENSEA Educational Project",
        }

    def start_requests(self):
        if not self.api_key:
            self.logger.error("RAPIDAPI_KEY non configuree dans .env")
            return

        for dest in DESTINATIONS:
            sky = dest["sky_id"]
            eid = dest["entity_id"]

            # 1. getCheapestOneway mois courant
            yield scrapy.Request(
                url=(f"{BASE_URL}/getCheapestOneway"
                     f"?originSkyId={ORIGIN_SKY_ID}&destinationSkyId={sky}"
                     f"&market={MARKET}&currency={CURRENCY}&month={self.mois_courant}"),
                headers=self._headers, callback=self.parse_cheapest,
                errback=self.errback, dont_filter=True,
                meta={"dest": dest, "type_vol": "oneway", "endpoint": "getCheapestOneway"},
            )

            # 2. getCheapestOneway mois suivant
            yield scrapy.Request(
                url=(f"{BASE_URL}/getCheapestOneway"
                     f"?originSkyId={ORIGIN_SKY_ID}&destinationSkyId={sky}"
                     f"&market={MARKET}&currency={CURRENCY}&month={self.mois_suivant}"),
                headers=self._headers, callback=self.parse_cheapest,
                errback=self.errback, dont_filter=True,
                meta={"dest": dest, "type_vol": "oneway", "endpoint": "getCheapestOneway_m2"},
            )

            # 3. searchFlights aller simple
            yield scrapy.Request(
                url=(f"{BASE_URL}/searchFlights"
                     f"?originSkyId={ORIGIN_SKY_ID}&originEntityId={ORIGIN_ENTITY_ID}"
                     f"&destinationSkyId={sky}&destinationEntityId={eid}"
                     f"&date={self.date_depart}&adults=1"
                     f"&market={MARKET}&currency={CURRENCY}&countryCode=CI"),
                headers=self._headers, callback=self.parse_search,
                errback=self.errback, dont_filter=True,
                meta={"dest": dest, "type_vol": "oneway", "endpoint": "searchFlights"},
            )

            # 4. searchFlights aller-retour
            yield scrapy.Request(
                url=(f"{BASE_URL}/searchFlights"
                     f"?originSkyId={ORIGIN_SKY_ID}&originEntityId={ORIGIN_ENTITY_ID}"
                     f"&destinationSkyId={sky}&destinationEntityId={eid}"
                     f"&date={self.date_depart}&returnDate={self.date_retour}&adults=1"
                     f"&market={MARKET}&currency={CURRENCY}&countryCode=CI"),
                headers=self._headers, callback=self.parse_search,
                errback=self.errback, dont_filter=True,
                meta={"dest": dest, "type_vol": "return", "endpoint": "searchFlights_return"},
            )

    # ── Parsers ──────────────────────────────────────────────────────────────

    def parse_cheapest(self, response):
        dest, type_vol, endpoint = (response.meta["dest"],
                                    response.meta["type_vol"],
                                    response.meta["endpoint"])
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.errors += 1
            self.logger.warning(f"JSON invalide cheapest {dest['ville']}")
            return

        count = 0
        for r in self._find_results(data):
            item = self._build_from_cheapest(r, dest, type_vol, endpoint)
            if item:
                yield item
                count += 1
                self.items_count += 1
        self.logger.info(f"  cheapest ABJ->{dest['ville']}: {count} vols")

    def parse_search(self, response):
        dest, type_vol, endpoint = (response.meta["dest"],
                                    response.meta["type_vol"],
                                    response.meta["endpoint"])
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.errors += 1
            self.logger.warning(f"JSON invalide search {dest['ville']}")
            return

        # Structure validee: data['data']['itineraries'] OU data['itineraries']
        itineraries = (
            data.get("data", {}).get("itineraries")
            or data.get("itineraries")
            or self._find_results(data)
            or []
        )

        count = 0
        for itin in itineraries:
            item = self._build_from_itinerary(itin, dest, type_vol, endpoint)
            if item:
                yield item
                count += 1
                self.items_count += 1
        self.logger.info(f"  search({type_vol[:2]}) ABJ->{dest['ville']}: {count} vols")

    # ── Constructeurs ────────────────────────────────────────────────────────

    def _build_from_cheapest(self, result, dest, type_vol, endpoint):
        prix_xof = self._get_price(result)
        if not prix_xof:
            return None

        now  = datetime.now()
        leg  = result.get("outboundLeg") or {}
        dep  = leg.get("departureDateTime") or {}
        arr  = leg.get("arrivalDateTime")   or {}

        date_depart   = self._parse_date(dep)  or self.date_depart
        heure_depart  = self._parse_heure(dep)
        heure_arrivee = self._parse_heure(arr)
        compagnie     = self._compagnies(result, leg)
        escales, vesc = self._escales(result, leg)
        duree         = self._safe_int(leg.get("durationInMinutes") or result.get("durationInMinutes"))
        classe        = self._classe(result)

        item = VolItem()
        item["vol_id"]            = self._vol_id(dest["code"], date_depart, prix_xof, type_vol)
        item["origine"]           = ORIGIN_SKY_ID
        item["destination"]       = dest["code"]
        item["ville_origine"]     = "Abidjan"
        item["ville_destination"] = dest["ville"]
        item["pays_destination"]  = dest["pays"]
        item["continent"]         = dest["continent"]
        item["date_depart"]       = date_depart
        item["date_arrivee"]      = None
        item["heure_depart"]      = heure_depart
        item["heure_arrivee"]     = heure_arrivee
        item["duree_minutes"]     = duree
        item["prix"]              = round(prix_xof * XOF_TO_USD, 2)
        item["devise"]            = "USD"
        item["prix_xof"]          = int(prix_xof)
        item["compagnie"]         = compagnie
        item["classe_cabine"]     = classe
        item["escales"]           = escales
        item["villes_escale"]     = vesc
        item["type_vol"]          = type_vol
        item["date_collecte"]     = now.strftime("%Y-%m-%d")
        item["heure_collecte"]    = now.strftime("%H:%M:%S")
        item["source_endpoint"]   = endpoint
        return item

    def _build_from_itinerary(self, itin, dest, type_vol, endpoint):
        # Prix — l'API retourne "XOF 587081.50" ou raw=587081.5
        price_obj = itin.get("price") or {}
        if isinstance(price_obj, dict):
            prix_xof = (self._parse_prix_formate(price_obj.get("formatted"))
                        or self._safe_float(price_obj.get("raw")))
        else:
            prix_xof = self._safe_float(price_obj)

        if not prix_xof or prix_xof <= 0:
            return None

        now     = datetime.now()
        legs    = itin.get("legs") or []
        leg_out = legs[0] if legs else {}
        leg_in  = legs[1] if len(legs) > 1 else {}

        dep_str       = leg_out.get("departure") or ""
        arr_str       = leg_out.get("arrival")   or ""
        date_depart   = dep_str[:10]  if len(dep_str) >= 10 else self.date_depart
        date_arrivee  = arr_str[:10]  if len(arr_str) >= 10 else None
        heure_depart  = dep_str[11:16] if len(dep_str) >= 16 else None
        heure_arrivee = arr_str[11:16] if len(arr_str) >= 16 else None

        if leg_in and not date_arrivee:
            ret = leg_in.get("departure") or ""
            if len(ret) >= 10:
                date_arrivee = ret[:10]

        # Compagnies (structure validee: carriers.marketing[0].name)
        carriers = leg_out.get("carriers") or []
        noms = []
        if isinstance(carriers, list):
            for c in carriers:
                n = c.get("name") if isinstance(c, dict) else ""
                if n and n not in noms:
                    noms.append(n)  # ← Supprime le point
        elif isinstance(carriers, dict):
            for cl in ["marketing", "operating"]:
                for c in (carriers.get(cl) or []):
                    n = c.get("name") if isinstance(c, dict) else ""
                    if n and n not in noms:
                        noms.append(n)  # ← Supprime le point

        compagnie = " / ".join(noms) if noms else ""
        # Escales
        stops = self._safe_int(leg_out.get("stopCount") or 0)
        stop_names = []

        # Récupère les segments du vol
        segments = leg_out.get("segments", [])

        for i, seg in enumerate(segments):
            # Pour chaque segment, récupère l'aéroport de destination
            # Sauf pour le dernier segment (c'est la destination finale)
            if i < len(segments) - 1:
                dest_seg = seg.get("destination", {})
                city = dest_seg.get("name") or dest_seg.get("cityName") or ""
                if not city:
                    city = dest_seg.get("flightPlaceId", "")
                if city and city not in stop_names:
                    stop_names.append(city)

        villes_escale = ", ".join(stop_names)

        # Si toujours vide, essaie une autre structure
        if not villes_escale and "layovers" in leg_out:
            layovers = leg_out.get("layovers", [])
            for lay in layovers:
                city = lay.get("name") or lay.get("cityName") or ""
                if city and city not in stop_names:
                    stop_names.append(city)
            villes_escale = ", ".join(stop_names)

        # Si toujours vide mais stopCount > 0, mets un message
        if not villes_escale and stops > 0:
            villes_escale = f"{stops} escale(s) non spécifiée(s)"
        duree  = self._safe_int(leg_out.get("durationInMinutes") or 0)
        tags   = itin.get("fareAttributes") or itin.get("tags") or {}
        classe = (tags.get("cabinClass") or "") if isinstance(tags, dict) else ""

        item = VolItem()
        item["vol_id"]            = self._vol_id(dest["code"], date_depart, prix_xof, type_vol)
        item["origine"]           = ORIGIN_SKY_ID
        item["destination"]       = dest["code"]
        item["ville_origine"]     = "Abidjan"
        item["ville_destination"] = dest["ville"]
        item["pays_destination"]  = dest["pays"]
        item["continent"]         = dest["continent"]
        item["date_depart"]       = date_depart
        item["date_arrivee"]      = date_arrivee
        item["heure_depart"]      = heure_depart
        item["heure_arrivee"]     = heure_arrivee
        item["duree_minutes"]     = duree
        item["prix"]              = round(prix_xof * XOF_TO_USD, 2)
        item["devise"]            = "USD"
        item["prix_xof"]          = int(prix_xof)
        item["compagnie"]         = compagnie
        item["classe_cabine"]     = classe
        item["escales"]           = stops
        item["villes_escale"]     = ", ".join(stop_names)
        item["type_vol"]          = type_vol
        item["date_collecte"]     = now.strftime("%Y-%m-%d")
        item["heure_collecte"]    = now.strftime("%H:%M:%S")
        item["source_endpoint"]   = endpoint
        return item

    # ── Utilitaires ──────────────────────────────────────────────────────────

    def _find_results(self, data):
        for c in [
            data.get("data", {}).get("flightQuotes", {}).get("results", []),
            data.get("data", {}).get("cheapest", []),
            data.get("data", {}).get("results", []),
            data.get("results", []),
        ]:
            if c:
                return c
        if isinstance(data.get("data"), list):
            return data["data"]
        return []

    def _get_price(self, obj):
        for k in ["price", "rawPrice", "amount", "value", "minPrice", "cheapestPrice"]:
            f = self._safe_float(obj.get(k))
            if f and f > 0:
                return f
        return None

    def _parse_prix_formate(self, s):
        """'XOF 587081.50' -> 587081.5"""
        if not s:
            return None
        m = re.search(r"[\d]+\.?\d*", str(s).replace(",", ""))
        try:
            return float(m.group()) if m else None
        except (ValueError, AttributeError):
            return None

    def _compagnies(self, result, leg):
        noms = []
        for src in [result, leg]:
            for key in ["carriers", "marketingCarrier", "operatingCarrier", "operatingCarriers"]:
                val = src.get(key)
                if isinstance(val, list):
                    for c in val:
                        n = (c.get("name") if isinstance(c, dict) else str(c)).strip()
                        if n and n not in noms:
                            noms.append(n)
                elif isinstance(val, dict):
                    n = val.get("name", "").strip()
                    if n and n not in noms:
                        noms.append(n)
        return " / ".join(noms) if noms else ""

    def _escales(self, result, leg):
        stops = self._safe_int(result.get("stopCount") or leg.get("stopCount") or 0)
        noms  = []
        segs  = leg.get("segments") or result.get("segments") or []
        if len(segs) > 1:
            stops = max(stops, len(segs) - 1)
            for seg in segs[:-1]:
                d = seg.get("destination") or {}
                city = (d.get("name") or d.get("cityName") or d.get("flightPlaceId")
                        or (d.get("parent") or {}).get("name") or "")
                if city and city not in noms:
                    noms.append(city)
        for lay in (result.get("layovers") or leg.get("layovers") or []):
            city = (lay.get("name") or lay.get("cityName") or "") if isinstance(lay, dict) else str(lay)
            if city and city not in noms:
                noms.append(city)
        return stops, ", ".join(noms)

    def _get_cabin_class(self, itinerary):
        for key in ['cabin', 'cabinClass', 'class', 'fareClass']:
            if key in itinerary and itinerary[key]:
                return itinerary[key]
        
        legs = itinerary.get('legs', [])
        for leg in legs:
            for key in ['cabin', 'cabinClass']:
                if key in leg and leg[key]:
                    return leg[key]
        
        for leg in legs:
            segments = leg.get('segments', [])
            for segment in segments:
                for key in ['cabin', 'cabinClass']:
                    if key in segment and segment[key]:
                        return segment[key]
        return ""
    
    def _get_cabin_class(self, itinerary):
        """Extrait et normalise la classe cabine (4 classes)."""
        # 1. Cherche dans l'itinerary principal
        for key in ['cabin', 'cabinClass', 'class', 'fareClass']:
            if key in itinerary and itinerary[key]:
                return self._normalize_cabin_class(itinerary[key])
        
        # 2. Cherche dans les legs
        legs = itinerary.get('legs', [])
        for leg in legs:
            for key in ['cabin', 'cabinClass']:
                if key in leg and leg[key]:
                    return self._normalize_cabin_class(leg[key])
        
        # 3. Cherche dans les segments
        for leg in legs:
            segments = leg.get('segments', [])
            for segment in segments:
                for key in ['cabin', 'cabinClass']:
                    if key in segment and segment[key]:
                        return self._normalize_cabin_class(segment[key])
        
        # 4. Par défaut
        return "ECONOMY"

    
    def _parse_date(self, dt):
        if isinstance(dt, dict) and "year" in dt:
            return f"{dt['year']}-{int(dt.get('month',1)):02d}-{int(dt.get('day',1)):02d}"
        if isinstance(dt, str) and len(dt) >= 10:
            return dt[:10]
        return None

    def _parse_heure(self, dt):
        if isinstance(dt, dict) and "hour" in dt:
            return f"{int(dt.get('hour',0)):02d}:{int(dt.get('minute',0)):02d}"
        if isinstance(dt, str) and len(dt) >= 16:
            return dt[11:16]
        return None

    def _safe_float(self, v):
        try:
            f = float(str(v).replace(",", "").replace("$", "").replace("XOF","").strip())
            return f if f > 0 else None
        except (TypeError, ValueError):
            return None

    def _safe_int(self, v):
        try:
            return max(0, int(float(str(v))))
        except (TypeError, ValueError):
            return 0

    def _vol_id(self, dest, date, prix, type_vol):
        return hashlib.md5(f"ABJ_{dest}_{date}_{int(prix)}_{type_vol}".encode()).hexdigest()[:20]

    def errback(self, failure):
        self.errors += 1
        self.logger.error(f"Erreur: {failure.request.url[:80]} — {failure.value}")

    def closed(self, reason):
        self.logger.info(f"Spider termine | items={self.items_count} | erreurs={self.errors}")
