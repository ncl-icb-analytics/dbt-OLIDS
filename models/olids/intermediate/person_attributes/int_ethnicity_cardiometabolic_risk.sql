{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'cardiometabolic_risk', 'demographics'],
        cluster_by=['person_id'])
}}

/*
Intermediate Ethnicity Cardiometabolic Risk - Identifies populations requiring lower BMI thresholds.
Based on NICE guidance: South Asian, Chinese, other Asian, Middle Eastern, Black African, 
or African-Caribbean family backgrounds have cardiometabolic risk at lower BMI levels.
Uses ETH2016*_COD cluster IDs from QOF ethnicity observations.
*/

WITH mapped_observations AS (
    -- Get all observations with proper concept mapping
    SELECT
        o.id AS observation_id,
        o.patient_id,
        pp.person_id,
        p.sk_patient_id,
        o.clinical_effective_date,
        o.observation_source_concept_id,
        mc.concept_id AS mapped_concept_id,
        mc.concept_code AS mapped_concept_code,
        mc.code_description AS mapped_concept_display,
        mc.cluster_id,
        mc.cluster_description
    FROM {{ ref('stg_olids_observation') }} AS o
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON o.patient_id = p.id
    INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
        ON p.id = pp.patient_id
    INNER JOIN {{ ref('stg_reference_mapped_concepts') }} AS mc
        ON
            o.observation_source_concept_id = mc.source_code_id
            AND mc.cluster_id LIKE 'ETH2016%_COD'  -- Filter directly on mapped_concepts
    WHERE o.clinical_effective_date IS NOT NULL
),

ethnicity_cardiometabolic_risk AS (
    -- Classify populations requiring lower BMI thresholds per NICE guidance
    SELECT
        mo.*,
        
        -- NICE-defined populations requiring lower BMI thresholds
        -- South Asian populations
        CASE 
            WHEN mo.cluster_id IN (
                'ETH2016AI_COD',   -- Indian
                'ETH2016AP_COD',   -- Pakistani
                'ETH2016AB_COD'    -- Bangladeshi
            ) THEN TRUE
            ELSE FALSE
        END AS is_south_asian,
        
        -- Chinese population
        CASE 
            WHEN mo.cluster_id = 'ETH2016AC_COD' THEN TRUE
            ELSE FALSE
        END AS is_chinese,
        
        -- Other Asian populations 
        CASE 
            WHEN mo.cluster_id IN (
                'ETH2016AO_COD',   -- Any other Asian background
                'ETH2016MWA_COD'   -- White and Asian (Mixed)
            ) THEN TRUE
            ELSE FALSE
        END AS is_other_asian,
        
        -- Middle Eastern / Arab populations
        CASE 
            WHEN mo.cluster_id = 'ETH2016OA_COD' THEN TRUE
            ELSE FALSE
        END AS is_middle_eastern,
        
        -- Black African populations
        CASE 
            WHEN mo.cluster_id IN (
                'ETH2016BA_COD',   -- African
                'ETH2016MWBA_COD'  -- White and Black African (Mixed)
            ) THEN TRUE
            ELSE FALSE
        END AS is_black_african,
        
        -- African-Caribbean populations
        CASE 
            WHEN mo.cluster_id IN (
                'ETH2016BC_COD',   -- Caribbean
                'ETH2016MWBC_COD', -- White and Black Caribbean (Mixed)
                'ETH2016BO_COD'    -- Any other Black or African or Caribbean background
            ) THEN TRUE
            ELSE FALSE
        END AS is_african_caribbean
    FROM mapped_observations AS mo
),

ethnicity_with_risk_flags AS (
    -- Add composite risk flags
    SELECT
        ecr.*,
        
        -- Overall flag for requiring lower BMI thresholds
        CASE 
            WHEN (ecr.is_south_asian OR ecr.is_chinese OR ecr.is_other_asian OR 
                  ecr.is_middle_eastern OR ecr.is_black_african OR ecr.is_african_caribbean)
            THEN TRUE
            ELSE FALSE
        END AS requires_lower_bmi_thresholds,
        
        -- Ethnicity group for reporting
        CASE 
            WHEN ecr.is_south_asian THEN 'South Asian'
            WHEN ecr.is_chinese THEN 'Chinese'
            WHEN ecr.is_other_asian THEN 'Other Asian'
            WHEN ecr.is_middle_eastern THEN 'Middle Eastern'
            WHEN ecr.is_black_african THEN 'Black African'
            WHEN ecr.is_african_caribbean THEN 'African-Caribbean'
            ELSE 'Other/White'
        END AS cardiometabolic_risk_ethnicity_group
    FROM ethnicity_cardiometabolic_risk AS ecr
),

person_level_aggregation AS (
    -- Aggregate to person level, taking latest observation with priority for risk populations
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_ethnicity_date,
        
        -- Use latest observation, but prioritise risk populations if multiple ethnicities recorded
        MAX(requires_lower_bmi_thresholds::int)::boolean AS requires_lower_bmi_thresholds,
        
        -- Get the ethnicity group from the latest qualifying observation
        ARRAY_AGG(DISTINCT cardiometabolic_risk_ethnicity_group) AS all_ethnicity_groups,
        ARRAY_AGG(DISTINCT mapped_concept_code) AS all_ethnicity_concept_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) AS all_ethnicity_concept_displays
    FROM ethnicity_with_risk_flags
    GROUP BY person_id
),

latest_observations AS (
    -- Get one row per person with their latest ethnicity observation
    SELECT
        ewrf.*,
        ROW_NUMBER() OVER (
            PARTITION BY ewrf.person_id 
            ORDER BY 
                ewrf.requires_lower_bmi_thresholds DESC, -- Prioritise risk populations
                ewrf.clinical_effective_date DESC
        ) AS rn
    FROM ethnicity_with_risk_flags AS ewrf
)

-- Final selection with person-level cardiometabolic risk flags
SELECT
    lo.person_id,
    lo.sk_patient_id,
    lo.observation_id,
    lo.clinical_effective_date,
    lo.mapped_concept_code AS concept_code,
    lo.mapped_concept_display AS concept_display,
    lo.cluster_id AS source_cluster_id,
    
    -- Individual ethnicity flags
    lo.is_south_asian,
    lo.is_chinese,
    lo.is_other_asian,
    lo.is_middle_eastern,
    lo.is_black_african,
    lo.is_african_caribbean,
    
    -- Composite flags
    pla.requires_lower_bmi_thresholds,
    lo.cardiometabolic_risk_ethnicity_group,
    
    -- Aggregated data
    pla.latest_ethnicity_date,
    pla.all_ethnicity_groups,
    pla.all_ethnicity_concept_codes,
    pla.all_ethnicity_concept_displays

FROM latest_observations AS lo
LEFT JOIN person_level_aggregation AS pla
    ON lo.person_id = pla.person_id
WHERE lo.rn = 1

ORDER BY lo.person_id