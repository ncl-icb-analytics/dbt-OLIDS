-- Staging model for olids_core.PERSON
-- Base layer: base_olids_person (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    id,
    nhs_number_hash,
    title,
    gender_concept_id,
    birth_year,
    birth_month,
    death_year,
    death_month
from {{ ref('base_olids_person') }}
