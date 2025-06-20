{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All foot examination records and related observations.
Includes unsuitable/declined checks, foot status, risk assessments, and Townson scale.
Complex model that aggregates multiple foot-related cluster IDs by person and date.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Enhanced with analytics-ready fields and legacy structure alignment.
*/

WITH foot_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.sk_patient_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Check if code term contains 'left' or 'right' (case insensitive)
        REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*left.*') AS has_left,
        REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*right.*') AS has_right,
        
        -- Check if code is a Townson scale and extract level
        REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level.*') AS is_townson,
        CASE 
            WHEN REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level 1.*') THEN 'Level 1'
            WHEN REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level 2.*') THEN 'Level 2'
            WHEN REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level 3.*') THEN 'Level 3'
            WHEN REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level 4.*') THEN 'Level 4'
            ELSE NULL
        END AS townson_level,
        
        -- Extract risk level from description
        CASE 
            WHEN LOWER(obs.mapped_concept_display) LIKE '%low risk%' THEN 'Low'
            WHEN LOWER(obs.mapped_concept_display) LIKE '%moderate risk%' THEN 'Moderate'
            WHEN LOWER(obs.mapped_concept_display) LIKE '%increased risk%' THEN 'Moderate'
            WHEN LOWER(obs.mapped_concept_display) LIKE '%high risk%' THEN 'High'
            WHEN LOWER(obs.mapped_concept_display) LIKE '%ulcerated%' THEN 'Ulcerated'
            -- Map Townson scale levels to risk levels
            WHEN REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level 1.*') THEN 'Low'
            WHEN REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level 2.*') THEN 'Moderate'
            WHEN REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level 3.*') THEN 'High'
            WHEN REGEXP_LIKE(LOWER(obs.mapped_concept_display), '.*townson.*scale.*level 4.*') THEN 'High'
            ELSE NULL
        END AS risk_level
        
    FROM ({{ get_observations("'FEPU_COD', 'FEDEC_COD', 'FRC_COD', 'CONABL_COD', 'CONABR_COD', 'AMPL_COD', 'AMPR_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

-- First aggregate foot status (amputations/absences) across all time
foot_status AS (
    SELECT 
        person_id,
        MAX(CASE WHEN source_cluster_id = 'CONABL_COD' THEN TRUE ELSE FALSE END) AS left_foot_absent,
        MAX(CASE WHEN source_cluster_id = 'CONABR_COD' THEN TRUE ELSE FALSE END) AS right_foot_absent,
        MAX(CASE WHEN source_cluster_id = 'AMPL_COD' THEN TRUE ELSE FALSE END) AS left_foot_amputated,
        MAX(CASE WHEN source_cluster_id = 'AMPR_COD' THEN TRUE ELSE FALSE END) AS right_foot_amputated
    FROM foot_observations
    GROUP BY person_id
),

-- Then get the check details for each date
check_details AS (
    SELECT 
        person_id,
        -- Get sk_patient_id (all should be same for person+date, so MAX is fine)
        MAX(sk_patient_id) AS sk_patient_id,
        clinical_effective_date,
        
        -- Check status
        MAX(CASE WHEN source_cluster_id = 'FEPU_COD' THEN TRUE ELSE FALSE END) AS is_unsuitable,
        MAX(CASE WHEN source_cluster_id = 'FEDEC_COD' THEN TRUE ELSE FALSE END) AS is_declined,
        
        -- Foot checks - left foot is checked if either explicit left foot check OR Townson scale
        MAX(CASE 
            WHEN source_cluster_id = 'FRC_COD' AND (has_left OR is_townson) THEN TRUE 
            ELSE FALSE 
        END) AS left_foot_checked,
        
        -- Right foot is checked if either explicit right foot check OR Townson scale
        MAX(CASE 
            WHEN source_cluster_id = 'FRC_COD' AND (has_right OR is_townson) THEN TRUE 
            ELSE FALSE 
        END) AS right_foot_checked,
        
        -- Both feet checked if Townson scale used
        MAX(CASE 
            WHEN source_cluster_id = 'FRC_COD' AND is_townson THEN TRUE
            ELSE FALSE 
        END) AS both_feet_checked,
        
        -- Get risk levels for each foot
        MAX(CASE 
            WHEN source_cluster_id = 'FRC_COD' AND (has_left OR is_townson) THEN risk_level
            ELSE NULL 
        END) AS left_foot_risk_level,
        
        MAX(CASE 
            WHEN source_cluster_id = 'FRC_COD' AND (has_right OR is_townson) THEN risk_level
            ELSE NULL 
        END) AS right_foot_risk_level,
        
        -- Get Townson scale level if used
        MAX(CASE 
            WHEN is_townson THEN townson_level
            ELSE NULL 
        END) AS townson_scale_level,
        
        -- Collect all codes and terms for traceability
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code) AS all_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (ORDER BY concept_display) AS all_concept_displays,
        ARRAY_AGG(DISTINCT source_cluster_id) WITHIN GROUP (ORDER BY source_cluster_id) AS all_source_cluster_ids
        
    FROM foot_observations
    GROUP BY person_id, clinical_effective_date
)

-- Final selection combining check details with foot status
SELECT 
    cd.person_id,
    cd.sk_patient_id,
    cd.clinical_effective_date,
    cd.is_unsuitable,
    cd.is_declined,
    cd.left_foot_checked,
    cd.right_foot_checked,
    cd.both_feet_checked,
    fs.left_foot_absent,
    fs.right_foot_absent,
    fs.left_foot_amputated,
    fs.right_foot_amputated,
    cd.left_foot_risk_level,
    cd.right_foot_risk_level,
    cd.townson_scale_level,
    cd.all_concept_codes,
    cd.all_concept_displays,
    cd.all_source_cluster_ids,
    
    -- Enhanced analytics fields (improvements over legacy)
    -- Check completion status
    CASE 
        WHEN cd.is_unsuitable THEN 'Unsuitable'
        WHEN cd.is_declined THEN 'Declined'
        WHEN cd.both_feet_checked THEN 'Complete - Both Feet'
        WHEN cd.left_foot_checked AND cd.right_foot_checked THEN 'Complete - Both Feet'
        WHEN cd.left_foot_checked AND (fs.right_foot_absent OR fs.right_foot_amputated) THEN 'Complete - Left Only (Right Missing)'
        WHEN cd.right_foot_checked AND (fs.left_foot_absent OR fs.left_foot_amputated) THEN 'Complete - Right Only (Left Missing)'
        WHEN cd.left_foot_checked THEN 'Partial - Left Only'
        WHEN cd.right_foot_checked THEN 'Partial - Right Only'
        ELSE 'Not Done'
    END AS examination_status,
    

    
    -- Diabetes foot risk classification for analytics
    CASE 
        WHEN cd.left_foot_risk_level = 'Ulcerated' OR cd.right_foot_risk_level = 'Ulcerated' THEN 'Ulcerated (High Risk)'
        WHEN cd.left_foot_risk_level = 'High' OR cd.right_foot_risk_level = 'High' THEN 'High Risk'
        WHEN cd.left_foot_risk_level = 'Moderate' OR cd.right_foot_risk_level = 'Moderate' THEN 'Moderate Risk'
        WHEN cd.left_foot_risk_level = 'Low' AND cd.right_foot_risk_level = 'Low' THEN 'Low Risk'
        WHEN cd.left_foot_risk_level = 'Low' OR cd.right_foot_risk_level = 'Low' THEN 'Low Risk'
        WHEN cd.left_foot_checked OR cd.right_foot_checked THEN 'Risk Not Specified'
        ELSE 'No Valid Examination'
    END AS diabetes_foot_risk_category

FROM check_details cd
LEFT JOIN foot_status fs ON cd.person_id = fs.person_id
ORDER BY cd.person_id, cd.clinical_effective_date DESC 