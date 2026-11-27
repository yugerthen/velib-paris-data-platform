"""Couche Silver : nettoyage des données brutes Vélib' avec PySpark, puis chargement dans BigQuery."""

import json
import os
import sys
from pathlib import Path

# Sur Windows : Spark utilise le Python de l'environnement virtuel
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from google.cloud import bigquery
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PROJET = "velib-paris-yugerthen"
DOSSIER_BRONZE = Path("data/bronze_charges")


def demarrer_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("velib-silver")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")  # toutes les dates en UTC
        .getOrCreate()
    )


def lire_bronze(spark: SparkSession, nom_flux: str) -> DataFrame:
    """Lit les fichiers JSON bruts avec Python, puis les donne à Spark.

    Sur Windows, on évite ainsi que Spark lise lui-même les dossiers (problème winutils).
    Résultat : une ligne par station et par collecte.
    """
    lignes = []
    for fichier in sorted((DOSSIER_BRONZE / nom_flux).glob("*.json")):
        contenu = json.loads(fichier.read_text(encoding="utf-8"))
        horodatage = fichier.stem.split("_")[-1]  # ex. 20261126T163414Z
        for station in contenu["data"]["stations"]:
            lignes.append(json.dumps({"horodatage": horodatage, "s": station}))

    print(f"{nom_flux} : {len(lignes)} lignes lues")

    rdd = spark.sparkContext.parallelize(lignes)
    return spark.read.json(rdd).select(
        "s",
        F.to_timestamp("horodatage", "yyyyMMdd'T'HHmmss'Z'").alias("horodatage_collecte"),
    )


def nettoyer_disponibilites(spark: SparkSession) -> DataFrame:
    brut = lire_bronze(spark, "station_status")

    propre = brut.select(
        "horodatage_collecte",
        F.col("s.station_id").cast("string").alias("station_id"),
        F.col("s.stationCode").alias("code_station"),
        F.col("s.num_bikes_available").cast("int").alias("velos_disponibles"),
        # On aplatit la liste [{"mechanical": 0}, {"ebike": 4}] en 2 colonnes
        F.expr("try_element_at(filter(s.num_bikes_available_types, x -> x.mechanical IS NOT NULL), 1).mechanical")
        .cast("int").alias("velos_mecaniques"),
        F.expr("try_element_at(filter(s.num_bikes_available_types, x -> x.ebike IS NOT NULL), 1).ebike")
        .cast("int").alias("velos_electriques"),
        F.col("s.num_docks_available").cast("int").alias("places_libres"),
        # 1 / 0  ->  vrai / faux
        (F.col("s.is_installed") == 1).alias("est_installee"),
        (F.col("s.is_renting") == 1).alias("location_possible"),
        (F.col("s.is_returning") == 1).alias("retour_possible"),
        # Nombre de secondes depuis 1970  ->  vraie date et heure
        F.timestamp_seconds(F.col("s.last_reported")).alias("derniere_mise_a_jour"),
    )

    return (
        propre
        .dropDuplicates(["station_id", "horodatage_collecte"])  # pas de doublons
        .filter((F.col("velos_disponibles") >= 0) & (F.col("places_libres") >= 0))  # pas de valeurs impossibles
    )


def nettoyer_stations(spark: SparkSession) -> DataFrame:
    brut = lire_bronze(spark, "station_information")

    infos = brut.select(
        "horodatage_collecte",
        F.col("s.station_id").cast("string").alias("station_id"),
        F.col("s.stationCode").alias("code_station"),
        F.col("s.name").alias("nom_station"),
        F.col("s.lat").cast("double").alias("latitude"),
        F.col("s.lon").cast("double").alias("longitude"),
        F.col("s.capacity").cast("int").alias("capacite"),
    )

    # Fonction de fenêtre : pour chaque station, on numérote ses versions de la plus récente à la plus ancienne
    fenetre = Window.partitionBy("station_id").orderBy(F.col("horodatage_collecte").desc())

    return (
        infos.withColumn("rang", F.row_number().over(fenetre))
        .filter(F.col("rang") == 1)  # on garde seulement la version la plus récente
        .drop("rang", "horodatage_collecte")
    )


def charger_bigquery(df: DataFrame, nom_table: str) -> None:
    """Remplace le contenu d'une table Silver par le résultat du nettoyage (idempotent)."""
    table = f"{PROJET}.silver.{nom_table}"

    # Les dates deviennent du texte en UTC (ex. 2026-11-27T12:33:59Z), pour éviter tout décalage horaire
    colonnes = [
        F.date_format(nom, "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(nom) if type_ == "timestamp" else F.col(nom)
        for nom, type_ in df.dtypes
    ]
    lignes = [ligne.asDict() for ligne in df.select(colonnes).collect()]

    client = bigquery.Client(project=PROJET)
    config = bigquery.LoadJobConfig(
        schema=client.get_table(table).schema,  # on garde le schéma créé par Terraform
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # on remplace, on n'ajoute pas
    )
    job = client.load_table_from_json(lignes, table, job_config=config)
    job.result()  # attend la fin du chargement (et s'arrête en cas d'erreur)

    print(f"{table} : {len(lignes)} lignes chargées")


def main() -> None:
    spark = demarrer_spark()

    disponibilites = nettoyer_disponibilites(spark)
    stations = nettoyer_stations(spark)

    print("\n=== Contrôle : nombre de stations par collecte ===")
    disponibilites.groupBy("horodatage_collecte").count().orderBy("horodatage_collecte").show()

    print("=== Chargement dans BigQuery (couche Silver) ===")
    charger_bigquery(disponibilites, "disponibilites")
    charger_bigquery(stations, "stations")

    spark.stop()


if __name__ == "__main__":
    main()