# Tables de la couche Silver : données nettoyées par PySpark

resource "google_bigquery_table" "silver_disponibilites" {
  dataset_id          = google_bigquery_dataset.couches["silver"].dataset_id
  table_id            = "disponibilites"
  description         = "Etat des stations Velib a chaque collecte, nettoye"
  deletion_protection = false

  # Partitionnement : un morceau par jour de collecte
  time_partitioning {
    type  = "DAY"
    field = "horodatage_collecte"
  }

  # Clustering : les lignes sont triees par station
  clustering = ["station_id"]

  schema = jsonencode([
    { name = "horodatage_collecte", type = "TIMESTAMP", mode = "REQUIRED", description = "Heure de la collecte (UTC)" },
    { name = "station_id", type = "STRING", mode = "REQUIRED", description = "Identifiant de la station" },
    { name = "code_station", type = "STRING", mode = "NULLABLE", description = "Code de la station" },
    { name = "velos_disponibles", type = "INT64", mode = "NULLABLE", description = "Nombre total de velos" },
    { name = "velos_mecaniques", type = "INT64", mode = "NULLABLE", description = "Velos mecaniques" },
    { name = "velos_electriques", type = "INT64", mode = "NULLABLE", description = "Velos electriques" },
    { name = "places_libres", type = "INT64", mode = "NULLABLE", description = "Places libres" },
    { name = "est_installee", type = "BOOL", mode = "NULLABLE", description = "Station installee" },
    { name = "location_possible", type = "BOOL", mode = "NULLABLE", description = "On peut prendre un velo" },
    { name = "retour_possible", type = "BOOL", mode = "NULLABLE", description = "On peut deposer un velo" },
    { name = "derniere_mise_a_jour", type = "TIMESTAMP", mode = "NULLABLE", description = "Dernier etat envoye par la station (UTC)" }
  ])

  labels = {
    projet = "velib-paris"
    couche = "silver"
  }
}

resource "google_bigquery_table" "silver_stations" {
  dataset_id          = google_bigquery_dataset.couches["silver"].dataset_id
  table_id            = "stations"
  description         = "Fiche de chaque station Velib (version la plus recente)"
  deletion_protection = false

  schema = jsonencode([
    { name = "station_id", type = "STRING", mode = "REQUIRED", description = "Identifiant de la station" },
    { name = "code_station", type = "STRING", mode = "NULLABLE", description = "Code de la station" },
    { name = "nom_station", type = "STRING", mode = "NULLABLE", description = "Nom de la station" },
    { name = "latitude", type = "FLOAT64", mode = "NULLABLE", description = "Latitude" },
    { name = "longitude", type = "FLOAT64", mode = "NULLABLE", description = "Longitude" },
    { name = "capacite", type = "INT64", mode = "NULLABLE", description = "Nombre de bornettes" }
  ])

  labels = {
    projet = "velib-paris"
    couche = "silver"
  }
}