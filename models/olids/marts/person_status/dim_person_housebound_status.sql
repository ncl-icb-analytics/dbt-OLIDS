{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'housebound', 'mobility'],
        cluster_by=['person_id'])
}}

-- Person Housebound Status Dimension Table
-- Holds the latest housebound status for persons who have a recorded housebound status
-- Only includes persons who have a record in HOUSEBOUND or NO_LONGER_HOUSEBOUND clusters

WITH latest_housebound_status AS (
    SELECT
        hbs.person_id,
        pp.sk_patient_id,
        hbs.clinical_effective_date,
        hbs.concept_code,
        hbs.code_description,
        hbs.source_cluster_id,
        -- Determine current housebound status
        CASE
            WHEN hbs.source_cluster_id = 'HOUSEBOUND' THEN TRUE
            WHEN hbs.source_cluster_id = 'NO_LONGER_HOUSEBOUND' THEN FALSE
            ELSE NULL
        END AS is_housebound,
        -- Status description
        CASE
            WHEN hbs.source_cluster_id = 'HOUSEBOUND' THEN 'Housebound'
            WHEN hbs.source_cluster_id = 'NO_LONGER_HOUSEBOUND' THEN 'Not Housebound'
            ELSE 'Unknown'
        END AS housebound_status
    FROM {{ ref('int_housebound_status_all') }} hbs
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON hbs.person_id = pp.person_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY hbs.person_id
        ORDER BY hbs.clinical_effective_date DESC, hbs.observation_id DESC
    ) = 1
)

SELECT
    person_id,
    sk_patient_id,
    clinical_effective_date AS latest_housebound_status_date,
    concept_code,
    code_description,
    source_cluster_id,
    is_housebound,
    housebound_status
FROM latest_housebound_status