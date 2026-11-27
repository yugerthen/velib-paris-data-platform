-- Dimension "quand ?" : une ligne par collecte, avec l'heure de Paris
with collectes as (
    select distinct horodatage_collecte
    from {{ source('silver', 'disponibilites') }}
)

select
    horodatage_collecte,
    date(horodatage_collecte, 'Europe/Paris')                             as date_locale,
    extract(hour from horodatage_collecte at time zone 'Europe/Paris')    as heure_locale,
    case extract(dayofweek from date(horodatage_collecte, 'Europe/Paris'))
        when 1 then 'dimanche'
        when 2 then 'lundi'
        when 3 then 'mardi'
        when 4 then 'mercredi'
        when 5 then 'jeudi'
        when 6 then 'vendredi'
        when 7 then 'samedi'
    end                                                                   as jour_semaine,
    extract(dayofweek from date(horodatage_collecte, 'Europe/Paris')) in (1, 7) as est_weekend
from collectes