/*
Analysis: Impact of registration-related filtering
Purpose: Understand how registration requirements affect observation counts
*/

-- First, check the registration filtering in dim_person_demographics
WITH all_persons_with_bp AS (
    SELECT DISTINCT pp.person_id
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Persons that pass dim_person_demographics filters
persons_in_dim AS (
    SELECT DISTINCT person_id
    FROM {{ ref('dim_person_demographics') }}
),

-- Check int_patient_registrations coverage
persons_with_registrations AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_patient_registrations') }}
),

-- Check which persons are filtered out at each stage
filtering_analysis AS (
    SELECT 
        COUNT(DISTINCT bp.person_id) as total_persons_with_bp_obs,
        COUNT(DISTINCT CASE WHEN pr.person_id IS NOT NULL THEN bp.person_id END) as persons_with_registrations,
        COUNT(DISTINCT CASE WHEN pd.person_id IS NOT NULL THEN bp.person_id END) as persons_in_dim_demographics,
        COUNT(DISTINCT CASE WHEN pr.person_id IS NULL THEN bp.person_id END) as persons_without_registrations,
        COUNT(DISTINCT CASE WHEN pd.person_id IS NULL THEN bp.person_id END) as persons_excluded_from_dim
    FROM all_persons_with_bp bp
    LEFT JOIN persons_with_registrations pr ON bp.person_id = pr.person_id
    LEFT JOIN persons_in_dim pd ON bp.person_id = pd.person_id
)

SELECT 
    *,
    ROUND(100.0 * persons_without_registrations / NULLIF(total_persons_with_bp_obs, 0), 2) as pct_without_registrations,
    ROUND(100.0 * persons_excluded_from_dim / NULLIF(total_persons_with_bp_obs, 0), 2) as pct_excluded_from_dim
FROM filtering_analysis;

-- Break down by practice to see if some practices are more affected
WITH practice_breakdown AS (
    SELECT 
        o.record_owner_organisation_code as practice_code,
        org.name as practice_name,
        COUNT(DISTINCT o.id) as total_observations,
        COUNT(DISTINCT pp.person_id) as total_persons,
        COUNT(DISTINCT CASE WHEN pr.person_id IS NOT NULL THEN pp.person_id END) as persons_with_registration,
        COUNT(DISTINCT CASE WHEN pd.person_id IS NOT NULL THEN pp.person_id END) as persons_in_dim,
        COUNT(DISTINCT CASE WHEN pr.person_id IS NOT NULL THEN o.id END) as obs_with_registration,
        COUNT(DISTINCT CASE WHEN pd.person_id IS NOT NULL THEN o.id END) as obs_in_dim
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp
        ON o.patient_id = pp.patient_id
    LEFT JOIN {{ ref('int_patient_registrations') }} pr
        ON pp.person_id = pr.person_id
    LEFT JOIN {{ ref('dim_person_demographics') }} pd
        ON pp.person_id = pd.person_id
    LEFT JOIN {{ ref('stg_olids_organisation') }} org
        ON o.record_owner_organisation_code = org.organisation_code
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
    GROUP BY o.record_owner_organisation_code, org.name
)

SELECT 
    practice_code,
    practice_name,
    total_observations,
    total_persons,
    persons_with_registration,
    persons_in_dim,
    obs_with_registration,
    obs_in_dim,
    ROUND(100.0 * (total_observations - obs_in_dim) / NULLIF(total_observations, 0), 2) as pct_obs_lost,
    ROUND(100.0 * (total_persons - persons_in_dim) / NULLIF(total_persons, 0), 2) as pct_persons_lost
FROM practice_breakdown
ORDER BY pct_obs_lost DESC;