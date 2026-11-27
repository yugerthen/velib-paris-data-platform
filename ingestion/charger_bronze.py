"""Charge les fichiers JSON bruts de data/bronze dans BigQuery (dataset bronze)."""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

PROJET = "velib-paris-yugerthen"
FLUX = ["station_information", "station_status"]

DOSSIER_BRONZE = Path("data/bronze")            # fichiers à charger
DOSSIER_CHARGES = Path("data/bronze_charges")   # fichiers déjà chargés

# Le même schéma que la table créée par Terraform
SCHEMA = [
    bigquery.SchemaField("horodatage_collecte", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("donnees", "JSON"),
]


def horodatage_depuis_nom(fichier: Path) -> str:
    """Retrouve l'heure de collecte dans le nom du fichier (ex. ..._20261126T163414Z.json)."""
    texte = fichier.stem.split("_")[-1]
    heure = datetime.strptime(texte, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return heure.isoformat()


def preparer_lignes(fichier: Path) -> list[dict]:
    """Une ligne par station, avec le bloc JSON brut tel quel."""
    contenu = json.loads(fichier.read_text(encoding="utf-8"))
    horodatage = horodatage_depuis_nom(fichier)
    return [
        {
            "horodatage_collecte": horodatage,
            "station_id": str(station["station_id"]),
            "donnees": station,
        }
        for station in contenu["data"]["stations"]
    ]


def charger_flux(client: bigquery.Client, nom_flux: str) -> None:
    fichiers = sorted((DOSSIER_BRONZE / nom_flux).glob("*.json"))
    if not fichiers:
        print(f"{nom_flux} : aucun nouveau fichier à charger")
        return

    table = f"{PROJET}.bronze.{nom_flux}"
    config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # on ajoute, on n'écrase pas
    )

    for fichier in fichiers:
        lignes = preparer_lignes(fichier)
        job = client.load_table_from_json(lignes, table, job_config=config)
        job.result()  # attend la fin du chargement (et s'arrête en cas d'erreur)

        # On range le fichier dans "déjà chargés" pour ne jamais le recharger
        destination = DOSSIER_CHARGES / nom_flux
        destination.mkdir(parents=True, exist_ok=True)
        shutil.move(str(fichier), destination / fichier.name)

        print(f"{nom_flux} : {len(lignes)} lignes chargées depuis {fichier.name}")


def main() -> None:
    client = bigquery.Client(project=PROJET)
    for nom_flux in FLUX:
        charger_flux(client, nom_flux)


if __name__ == "__main__":
    main()