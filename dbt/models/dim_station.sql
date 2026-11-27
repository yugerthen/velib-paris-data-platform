-- Dimension "où ?" : une ligne par station
select
    station_id,
    code_station,
    nom_station,
    latitude,
    longitude,
    capacite
from {{ source('silver', 'stations') }}