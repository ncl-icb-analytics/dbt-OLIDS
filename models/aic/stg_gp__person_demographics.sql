{{ config(materialized='view') }}

with
    patient_base as (select * from {{ ref("base_olids__patient") }}),

    patient_person_base as (select * from {{ ref("base_olids__patient_person") }}),

    practitioner_base as (
        select * from {{ ref("base_olids__patient_registered_practitioner_in_role") }}
    ),

    person_patients as (
        select
            pp.person_id,
            p.*
        from patient_person_base pp
        inner join patient_base p on pp.patient_id = p.id
    ),

    reg_base as (
        select
            person_id,
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
        group by person_id
    ),

    person_w_reg as (
        select
            p.*,
            coalesce(r.has_active_registration = 1, false) as active_registration_flag
        from person_patients p
        left join reg_base r on p.person_id = r.person_id
    ),

    concept as (select * from {{ ref("stg_gp__concept") }}),

    person_w_gender as (
        select
            p.*,
            gc.concept_code as gender_concept_code,
            gc.concept_name as gender_concept_name,
            gc.concept_vocabulary as gender_concept_vocabulary
        from person_w_reg p
        left join concept gc on p.gender_concept_id = gc.db_concept_id
    )

select
    person_id,
    id as patient_id,
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
    gender_concept_id,
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
from person_w_gender