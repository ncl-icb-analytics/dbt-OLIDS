{{ config(
    materialized = 'table',
    tags = ['dimension', 'person'],
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Person dimension consolidating all person and patient identifiers for simplified joins and conformed dimensions.'"
    ]
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['pp.person_id']) }} AS person_sk,
  pp.person_id,
  pp.patient_id,
  p.sk_patient_id
FROM {{ ref('stg_olids_patient_person') }} pp
LEFT JOIN {{ ref('stg_olids_patient') }} p
  ON pp.patient_id = p.id 