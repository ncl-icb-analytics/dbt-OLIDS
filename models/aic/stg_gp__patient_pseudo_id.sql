{{ config(materialized='view') }}

with
    patient_base as (select * from {{ ref("base_olids__patient") }}),

    patient_person_base as (select * from {{ ref("base_olids__patient_person") }})

select distinct
    p.id as patient_id,
    pp.person_id as person_id,
    p.sk_patient_id as sk_patient_id
from patient_base p
inner join patient_person_base pp on p.id = pp.patient_id
where p.sk_patient_id is not null