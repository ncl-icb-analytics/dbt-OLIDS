{{ config(materialized='table') }}

-- note: sk_patient_id as person_id
-- note: gender concept ids (uuids) in source are mixed case, and appear in different cases for the same id.
-- These are cast to lower case, but the fix should ideally be at source

with
    patient_base as (select * from {{ ref("base_olids__patient") }}),

    concept as (select * from {{ ref("stg_gp__concept") }}),

    id_map as (select * from {{ ref("stg_gp__patient_pseudo_id") }}),

    patient_w_gender as (
        select
            p.*,
            gc.concept_code as gender_concept_code,
            gc.concept_name as gender_concept_name,
            gc.concept_vocabulary as gender_concept_vocabulary
        from patient_base p
        left join concept gc on lower(p.gender_concept_id) = gc.db_concept_id
    ),

    -- patient mapping for master_person_id lookup
    patient_mapping as (
        select distinct
            id_value,
            master_person_id
        from id_map
        where id_type = 'patient_id'
    ),

    practitioner_base as (
        select * from {{ ref("base_olids__patient_registered_practitioner_in_role") }}
    ),

    reg_base as (
        select
            patient_id,
            max(
                case
                    when end_date is null
                    then 1
                    when end_date > current_date()
                    then 1
                    else 0
                end
            ) as has_active_registration
        from practitioner_base
        group by patient_id
    ),

    person_w_reg as (
        select
            p.*,
            pm.master_person_id,
            coalesce(r.has_active_registration = 1, false) as active_registration_flag
        from patient_w_gender p
        left join patient_mapping pm on p.id = pm.id_value
        left join reg_base r on p.id = r.patient_id
    )

select
    master_person_id as person_id,
    lower(id) as patient_id,
    sk_patient_id,
    null::varchar as current_address_id,
    birth_year,
    birth_month,
    null::date as date_of_birth,
    case
        when death_year is not null and death_month is not null
        then date_from_parts(death_year, death_month, 1)
        else null
    end as date_of_death,
    lower(gender_concept_id) as gender_concept_id,
    gender_concept_code,
    gender_concept_name,
    gender_concept_vocabulary,
    null::varchar as ethnicity_concept_id,
    null::varchar as ethnicity_concept_code,
    null::varchar as ethnicity_concept_name,
    null::varchar as ethnicity_concept_vocabulary,
    lds_start_date_time::date as valid_from,
    lds_end_date_time::date as valid_to,
    case when death_year is not null then true else false end as death_flag,
    active_registration_flag
from person_w_reg