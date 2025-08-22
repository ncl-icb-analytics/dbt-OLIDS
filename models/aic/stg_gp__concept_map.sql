{{ config(materialized='view') }}

with
    concept_map_base as (select * from {{ ref("base_olids__concept_map") }}),

    concept_base as (select * from {{ ref("base_olids__concept") }}),

    snomed_base as (select * from {{ ref("base_snomed__concept") }}),

    concept_map_full as (
        select
            cm.id as map_id,
            cm.source_code_id as source_db_concept_id,
            'OLIDS' as source_db_concept_id_type,
            sc.code as source_concept_code,
            case
                when sc.system = 'http://snomed.info/sct' then coalesce(s_snomed.preferred_term, sc.display)
                else sc.display
            end as source_concept_name,
            case
                when sc.system = 'http://snomed.info/sct' then 'SNOMED'
                else sc.system
            end as source_concept_vocabulary,
            cm.target_code_id as target_db_concept_id,
            'OLIDS' as target_db_concept_id_type,
            tc.code as target_concept_code,
            case
                when tc.system = 'http://snomed.info/sct' then coalesce(t_snomed.preferred_term, tc.display)
                else tc.display
            end as target_concept_name,
            case
                when tc.system = 'http://snomed.info/sct' then 'SNOMED'
                else tc.system
            end as target_concept_vocabulary
        from concept_map_base cm
        left join concept_base sc on cm.source_code_id = sc.id  -- source_code_id to id gets source concept
        left join concept_base tc on cm.target_code_id = tc.id  -- target_code_id to id gets target concept
        left join snomed_base s_snomed on sc.system = 'http://snomed.info/sct' and sc.code = s_snomed.sk_snomed_concept_id::varchar
        left join snomed_base t_snomed on tc.system = 'http://snomed.info/sct' and tc.code = t_snomed.sk_snomed_concept_id::varchar
    )

select
    map_id,
    source_db_concept_id,
    source_db_concept_id_type,
    source_concept_code,
    source_concept_name,
    source_concept_vocabulary,
    target_db_concept_id,
    target_db_concept_id_type,
    target_concept_code,
    target_concept_name,
    target_concept_vocabulary
from concept_map_full