{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'birth_death'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Core birth and death information for each person. Provides foundation data for age calculations and demographic analysis. Calculates birth/death dates as the exact midpoint of the recorded month for optimal statistical precision.'"
        ]
    )
}}

-- Person Birth and Death Dimension Table
-- Core birth and death information for each person
-- Designed to be reused by other dimension tables for age calculations

SELECT DISTINCT 
    pp.person_id,
    p.sk_patient_id,
    p.birth_year,
    p.birth_month,
    -- Calculate approximate birth date using exact midpoint of the month
    CASE 
        WHEN p.birth_year IS NOT NULL AND p.birth_month IS NOT NULL 
        THEN DATEADD(day, 
            FLOOR(DAY(LAST_DAY(TO_DATE(p.birth_year || '-' || p.birth_month || '-01'))) / 2), 
            TO_DATE(p.birth_year || '-' || p.birth_month || '-01')
        )
        ELSE NULL
    END AS birth_date_approx,
    p.death_year,
    p.death_month,
    -- Calculate approximate death date using exact midpoint of the month
    CASE 
        WHEN p.death_year IS NOT NULL AND p.death_month IS NOT NULL 
        THEN DATEADD(day, 
            FLOOR(DAY(LAST_DAY(TO_DATE(p.death_year || '-' || p.death_month || '-01'))) / 2), 
            TO_DATE(p.death_year || '-' || p.death_month || '-01')
        )
        ELSE NULL
    END AS death_date_approx,
    p.death_year IS NOT NULL AS is_deceased,
    COALESCE(p.is_dummy_patient, FALSE) AS is_dummy_patient
FROM {{ ref('stg_olids_patient') }} p
INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
    ON p.id = pp.patient_id
WHERE p.birth_year IS NOT NULL AND p.birth_month IS NOT NULL 