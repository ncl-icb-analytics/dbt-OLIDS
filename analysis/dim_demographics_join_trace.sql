/*
Analysis: Trace data loss through dim_person_demographics component joins
Purpose: Identify which specific joins/filters in dim_person_demographics cause data loss
*/

-- Start with all BP observations
WITH base_observations AS (
    SELECT 
        o.id as observation_id,
        o.patient_id,
        pp.person_id,
        o.record_owner_organisation_code as practice_code
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

-- Check dim_person_birth_death (base table for dim_person_demographics)
with_birth_death AS (
    SELECT 
        bo.*,
        CASE WHEN bd.person_id IS NOT NULL THEN 1 ELSE 0 END as has_birth_death,
        CASE WHEN bd.birth_date_approx IS NOT NULL THEN 1 ELSE 0 END as has_birth_date
    FROM base_observations bo
    LEFT JOIN {{ ref('dim_person_birth_death') }} bd
        ON bo.person_id = bd.person_id
),

-- Check current_registrations CTE logic from dim_person_demographics
with_current_registration AS (
    SELECT 
        wbd.*,
        CASE WHEN cr.person_id IS NOT NULL THEN 1 ELSE 0 END as has_current_registration
    FROM with_birth_death wbd
    LEFT JOIN (
        SELECT 
            person_id,
            practice_ods_code as practice_code,
            practice_name,
            registration_start_date,
            registration_end_date,
            is_current_registration,
            is_latest_registration
        FROM {{ ref('int_patient_registrations') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY 
                is_current_registration DESC,
                registration_start_date DESC,
                registration_record_id DESC
        ) = 1
    ) cr ON wbd.person_id = cr.person_id
),

-- Summary by practice
practice_summary AS (
    SELECT 
        practice_code,
        COUNT(DISTINCT observation_id) as total_obs,
        COUNT(DISTINCT person_id) as total_persons,
        SUM(has_birth_death) as obs_with_birth_death,
        SUM(has_birth_date) as obs_with_birth_date,
        SUM(has_current_registration) as obs_with_registration,
        -- Calculate cumulative loss
        COUNT(DISTINCT observation_id) - SUM(has_birth_death) as lost_at_birth_death,
        SUM(has_birth_death) - SUM(has_birth_date) as lost_at_birth_date,
        SUM(has_birth_date) - SUM(has_current_registration) as lost_at_registration
    FROM with_current_registration
    GROUP BY practice_code
)

SELECT 
    ps.practice_code,
    org.name as practice_name,
    ps.total_obs,
    ps.total_persons,
    ps.obs_with_birth_death,
    ps.obs_with_birth_date,
    ps.obs_with_registration,
    ps.lost_at_birth_death,
    ps.lost_at_birth_date,
    ps.lost_at_registration,
    ROUND(100.0 * lost_at_birth_death / NULLIF(total_obs, 0), 2) as pct_lost_birth_death,
    ROUND(100.0 * lost_at_birth_date / NULLIF(obs_with_birth_death, 0), 2) as pct_lost_birth_date,
    ROUND(100.0 * lost_at_registration / NULLIF(obs_with_birth_date, 0), 2) as pct_lost_registration
FROM practice_summary ps
LEFT JOIN {{ ref('stg_olids_organisation') }} org
    ON ps.practice_code = org.organisation_code
ORDER BY total_obs DESC;

-- Also check overall summary
SELECT 
    'OVERALL' as practice_code,
    'All Practices' as practice_name,
    SUM(total_obs) as total_obs,
    SUM(total_persons) as total_persons,
    SUM(obs_with_birth_death) as obs_with_birth_death,
    SUM(obs_with_birth_date) as obs_with_birth_date,
    SUM(obs_with_registration) as obs_with_registration,
    SUM(lost_at_birth_death) as lost_at_birth_death,
    SUM(lost_at_birth_date) as lost_at_birth_date,
    SUM(lost_at_registration) as lost_at_registration,
    ROUND(100.0 * SUM(lost_at_birth_death) / NULLIF(SUM(total_obs), 0), 2) as pct_lost_birth_death,
    ROUND(100.0 * SUM(lost_at_birth_date) / NULLIF(SUM(obs_with_birth_death), 0), 2) as pct_lost_birth_date,
    ROUND(100.0 * SUM(lost_at_registration) / NULLIF(SUM(obs_with_birth_date), 0), 2) as pct_lost_registration
FROM practice_summary;