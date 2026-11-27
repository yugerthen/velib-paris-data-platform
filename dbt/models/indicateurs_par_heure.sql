-- Indicateurs prêts pour Power BI : % de stations vides et pleines, par heure
select
    t.date_locale,
    t.heure_locale,
    t.jour_semaine,
    count(*)                                   as nb_stations,
    round(avg(if(f.est_vide, 1, 0)) * 100, 1)  as pct_stations_vides,
    round(avg(if(f.est_pleine, 1, 0)) * 100, 1) as pct_stations_pleines,
    round(avg(f.taux_remplissage) * 100, 1)    as taux_remplissage_moyen
from {{ ref('fct_disponibilites') }} as f
join {{ ref('dim_temps') }} as t using (horodatage_collecte)
group by t.date_locale, t.heure_locale, t.jour_semaine