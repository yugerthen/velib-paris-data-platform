-- Faits : les chiffres de chaque station à chaque collecte
{{ config(
    partition_by={'field': 'horodatage_collecte', 'data_type': 'timestamp', 'granularity': 'day'},
    cluster_by=['station_id']
) }}

select
    horodatage_collecte,                                   -- lien vers dim_temps
    station_id,                                            -- lien vers dim_station
    velos_disponibles,
    velos_mecaniques,
    velos_electriques,
    places_libres,
    safe_divide(velos_disponibles, velos_disponibles + places_libres) as taux_remplissage,
    velos_disponibles = 0                                  as est_vide,
    places_libres = 0                                      as est_pleine
from {{ source('silver', 'disponibilites') }}
where est_installee                                        -- on ignore les stations pas installées