# 🚲 Vélib' Paris Data Platform

Plateforme data de bout en bout sur **Google Cloud Platform** pour analyser l'usage du vélo à Paris.

## Objectifs
- Savoir **où et quand** il manque des vélos dans les stations Vélib'
- Identifier les **pistes cyclables les plus fréquentées**, et à quelles heures

## Données (open data)
- **Vélib' Métropole** : disponibilité des stations en temps réel (format GBFS)
- **Ville de Paris** : historique des comptages vélo

## Architecture (médaillon)
- 🥉 **Bronze** : données brutes (Cloud Storage)
- 🥈 **Silver** : données nettoyées (PySpark sur Dataproc)
- 🥇 **Gold** : modèle en étoile (dbt sur BigQuery)
- 📊 **Visualisation** : Power BI

## Stack technique
BigQuery · Cloud Storage · Dataproc (PySpark) · Eventarc · Workflows · Cloud Run · Cloud Scheduler · Cloud Composer · dbt · Terraform · GitHub Actions · Power BI

## Avancement
- [x] Phase 0 : fondations
- [ ] Phase 1 : Bronze
- [ ] Phase 2 : Silver
- [ ] Phase 3 : automatisation
- [ ] Phase 4 : Gold
- [ ] Phase 5 : données en direct
- [ ] Phase 6 : orchestration
- [ ] Phase 7 : BI et documentation