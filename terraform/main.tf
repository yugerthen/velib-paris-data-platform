locals {
  # Les 3 couches de l'architecture médaillon
  couches = {
    bronze = "Données brutes"
    silver = "Données nettoyées"
    gold   = "Données prêtes pour la BI (modèle en étoile)"
  }

  # Bac à sable BigQuery : les tables et les partitions expirent après 60 jours maximum
  expiration_60_jours_ms = 60 * 24 * 60 * 60 * 1000
}

resource "google_bigquery_dataset" "couches" {
  for_each = local.couches

  dataset_id                      = each.key
  description                     = each.value
  location                        = var.region
  default_table_expiration_ms     = local.expiration_60_jours_ms
  default_partition_expiration_ms = local.expiration_60_jours_ms

  labels = {
    projet = "velib-paris"
    couche = each.key
  }
}