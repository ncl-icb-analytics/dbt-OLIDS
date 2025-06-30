{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'sex'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Dimension table for person sex. Uses hardcoded gender_concept_id values to determine sex due to issues with Concept Map/Concept tables.'"
        ]
    )
}}

-- Person Sex Dimension Table
-- Derives sex from hardcoded gender_concept_id values

SELECT DISTINCT
    pp.person_id,
    -- Derives sex by mapping specific gender_concept_id values to 'Female' or 'Male'
    -- Any other gender_concept_id or a NULL value results in 'Unknown'
    CASE
        WHEN p.gender_concept_id = '4907ce31-7168-4385-b91d-a7fe171a1c8f' THEN 'Female' -- Hardcoded ID for Female
        WHEN p.gender_concept_id = '3ae10994-efd0-47db-ade4-e440eaf0f973' THEN 'Male'   -- Hardcoded ID for Male
        ELSE 'Unknown' -- Default for NULL or any other gender_concept_id values
    END AS sex
FROM {{ ref('stg_olids_patient') }} AS p
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON p.id = pp.patient_id
