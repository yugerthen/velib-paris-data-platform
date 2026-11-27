-- Le test échoue s'il trouve des lignes : un taux de remplissage doit être entre 0 et 1
select *
from {{ ref('fct_disponibilites') }}
where taux_remplissage < 0
   or taux_remplissage > 1