{{ config(materialized='table') }}

with
    concept_base as (
        select distinct * from {{ ref("base_olids__concept") }}
    ),

    snomed_base as (select * from {{ ref("base_snomed__concept") }}),

    concept_w_vocab as (
        select
            c.id as db_concept_id,
            'OLIDS' as db_concept_id_type,
            c.code as concept_code,
            case
                when c.system = 'http://snomed.info/sct' then coalesce(s.preferred_term, c.display)
                else c.display
            end as concept_name,
            case
                when c.system = 'http://snomed.info/sct' then 'SNOMED'
                else c.system
            end as concept_vocabulary,
            c.system as concept_system,
            c.lds_start_date_time::date as valid_from,
            null::date as valid_to
        from concept_base c
        left join snomed_base s on c.system = 'http://snomed.info/sct' and c.code = s.sk_snomed_concept_id::varchar
    )

select
    db_concept_id,
    db_concept_id_type,
    concept_code,
    concept_name,
    concept_vocabulary,
    concept_system,
    valid_from,
    valid_to
from concept_w_vocab