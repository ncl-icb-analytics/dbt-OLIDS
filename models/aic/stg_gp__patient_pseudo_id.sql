{{ config(materialized='table') }}

-- this is a fudge to get round bad person id disambiguation in UAT version
-- we take sk_patient_id as canonical identifier
-- however, there are cases where multiple sk_patient_id are mapped to the same patient_id
-- therefore, a master_person_id is created that is the minimum sk_patient_id for any given patient
-- a many-to-one relationship from master_person_id to patient_id is allowed
-- a many-to-one relationship from master_person_id to sk_patient_id is also allowed

with
    patient_base as (
        select * from {{ ref("base_olids__patient") }}
    ),

    -- (1) Get all patient_id <-> sk_patient_id relationships
    patient_sk_raw as (
        select distinct
            id as patient_id,
            sk_patient_id
        from patient_base
        where sk_patient_id is not null
    ),

    -- (2) For each patient_id, find the minimum sk_patient_id (this becomes master_person_id)
    patient_master_mapping as (
        select
            patient_id,
            min(cast(sk_patient_id as varchar)) as master_person_id
        from patient_sk_raw
        group by patient_id
    ),

    -- (3) Now map ALL original sk_patient_ids to their master_person_id
    final_mapping as (
        select
            psr.patient_id,
            psr.sk_patient_id,
            pmm.master_person_id
        from patient_sk_raw psr
        left join patient_master_mapping pmm on psr.patient_id = pmm.patient_id
    ),

-- Create unified lookup table with both patient_id and sk_patient_id lookups
    patient_id_lookup as (
        select distinct
            cast(patient_id as varchar) as id_value,
            'patient_id' as id_type,
            cast(master_person_id as varchar) as master_person_id
        from final_mapping
    ),

    sk_patient_id_lookup as (
        select distinct
            cast(sk_patient_id as varchar) as id_value,
            'sk_patient_id' as id_type,
            cast(master_person_id as varchar) as master_person_id
        from final_mapping
    ),

    unified_lookup as (
        select * from patient_id_lookup
        union all
        select * from sk_patient_id_lookup
    )

select distinct
    id_value,
    id_type,
    master_person_id
from unified_lookup