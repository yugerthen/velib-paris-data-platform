# Tables de la couche Bronze : un bloc JSON brut par station et par collecte
resource "google_bigquery_table" "bronze" {
  for_each = toset(["station_information", "station_status"])

  dataset_id          = google_bigquery_dataset.couches["bronze"].dataset_id
  table_id            = each.key
  description         = "Données brutes Vélib' (${each.key}), sans transformation"
  deletion_protection = false

  # Partitionnement : un morceau par jour de collecte
  time_partitioning {
    type  = "DAY"
    field = "horodatage_collecte"
  }

  # Clustering : les lignes sont triées par station
  clustering = ["station_id"]

  schema = jsonencode([
    { name = "horodatage_collecte", type = "TIMESTAMP", mode = "REQUIRED", description = "Heure de la collecte (UTC)" },
    { name = "station_id", type = "STRING", mode = "REQUIRED", description = "Identifiant de la station" },
    { name = "donnees", type = "JSON", mode = "NULLABLE", description = "Bloc JSON brut de la station" }
  ])

  labels = {
    projet = "velib-paris"
    couche = "bronze"
  }
}