"""Collecte des données Vélib' (format GBFS) et sauvegarde brute (couche Bronze)."""

import json
from datetime import datetime, timezone
from pathlib import Path

import requests

# Adresse de l'API Vélib' (open data, sans clé)
URL_BASE = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"

# Les 2 fichiers qu'on récupère
FLUX = ["station_information", "station_status"]

# Dossier local où on range les données brutes
DOSSIER_BRONZE = Path("data/bronze")


def telecharger(nom_flux: str) -> dict:
    """Télécharge un fichier JSON de l'API Vélib'."""
    reponse = requests.get(f"{URL_BASE}/{nom_flux}.json", timeout=30)
    reponse.raise_for_status()  # arrête le script si l'API renvoie une erreur
    return reponse.json()


def sauvegarder(nom_flux: str, donnees: dict, horodatage: str) -> Path:
    """Enregistre le JSON tel quel, sans rien modifier (c'est la règle de Bronze)."""
    dossier = DOSSIER_BRONZE / nom_flux
    dossier.mkdir(parents=True, exist_ok=True)
    chemin = dossier / f"{nom_flux}_{horodatage}.json"
    chemin.write_text(json.dumps(donnees, ensure_ascii=False), encoding="utf-8")
    return chemin


def main() -> None:
    # L'heure de la collecte, en UTC, dans le nom du fichier
    horodatage = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for nom_flux in FLUX:
        donnees = telecharger(nom_flux)
        chemin = sauvegarder(nom_flux, donnees, horodatage)
        nb_stations = len(donnees["data"]["stations"])
        print(f"{nom_flux} : {nb_stations} stations -> {chemin}")


if __name__ == "__main__":
    main()