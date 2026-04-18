import scrapy


class VolItem(scrapy.Item):
    # Identifiant
    vol_id            = scrapy.Field()

    # Trajet
    origine           = scrapy.Field()
    destination       = scrapy.Field()
    ville_origine     = scrapy.Field()
    ville_destination = scrapy.Field()
    pays_destination  = scrapy.Field()
    continent         = scrapy.Field()

    # Dates & horaires
    date_depart       = scrapy.Field()
    date_arrivee      = scrapy.Field()
    duree_minutes     = scrapy.Field()
    heure_depart      = scrapy.Field()
    heure_arrivee     = scrapy.Field()

    # Prix
    prix              = scrapy.Field()
    devise            = scrapy.Field()
    prix_xof          = scrapy.Field()

    # Vol
    compagnie         = scrapy.Field()
    classe_cabine     = scrapy.Field()
    escales           = scrapy.Field()
    villes_escale     = scrapy.Field()
    type_vol          = scrapy.Field()

    # Collecte
    date_collecte     = scrapy.Field()
    heure_collecte    = scrapy.Field()
    source_endpoint   = scrapy.Field()
