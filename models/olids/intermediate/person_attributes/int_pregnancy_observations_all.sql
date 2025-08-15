{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All pregnancy-related observations with enhanced analytics features.
Uses QOF pregnancy cluster IDs: 
- PREG_COD: Pregnancy status codes only
- PREGDEL_COD: Superset including both pregnancy AND delivery/outcome codes

Important: PREGDEL_COD contains all pregnancy-related codes, including those in PREG_COD.
To identify current pregnancy, look for recent PREGDEL_COD codes that are also pregnancy codes (not delivery outcomes).

Enhanced Analytics Features:
- Pregnancy status categorisation and clinical context
- Clear identification of pregnancy vs delivery/outcome codes
- Clinical safety integration support (e.g., medication contraindications)

Single Responsibility: Pregnancy observation data collection with analytics enhancement.
Includes ALL persons following intermediate layer principles.
*/

-- First, identify which codes exist in each cluster
WITH code_clusters AS (
    SELECT DISTINCT
        mapped_concept_code,
        mapped_concept_display,
        cluster_id
    FROM ({{ get_observations("'PREG_COD', 'PREGDEL_COD'") }})
),

-- Categorize codes based on their cluster membership and description
code_categories AS (
    SELECT DISTINCT
        mapped_concept_code,
        mapped_concept_display,
        MAX(CASE WHEN cluster_id = 'PREG_COD' THEN 1 ELSE 0 END) AS in_preg_cod,
        MAX(CASE WHEN cluster_id = 'PREGDEL_COD' THEN 1 ELSE 0 END) AS in_pregdel_cod,
        -- Categorize based on description patterns
        CASE
            WHEN LOWER(mapped_concept_display) LIKE '%delivery%' 
                OR LOWER(mapped_concept_display) LIKE '%birth%'
                OR LOWER(mapped_concept_display) LIKE '%caesarean%'
                OR LOWER(mapped_concept_display) LIKE '%cesarean%'
                OR LOWER(mapped_concept_display) LIKE '%born%'
                OR LOWER(mapped_concept_display) LIKE '%labour%'
                OR LOWER(mapped_concept_display) LIKE '%labor%'
            THEN 'delivery'
            WHEN LOWER(mapped_concept_display) LIKE '%termination%' 
                OR LOWER(mapped_concept_display) LIKE '%abortion%'
                OR LOWER(mapped_concept_display) LIKE '%miscarriage%'
                OR LOWER(mapped_concept_display) LIKE '%stillbirth%'
                OR LOWER(mapped_concept_display) LIKE '%ectopic%'
            THEN 'pregnancy_loss'
            WHEN LOWER(mapped_concept_display) LIKE '%pregnant%'
                OR LOWER(mapped_concept_display) LIKE '%pregnancy%'
                OR LOWER(mapped_concept_display) LIKE '%gravid%'
                OR LOWER(mapped_concept_display) LIKE '%antenatal%'
                OR LOWER(mapped_concept_display) LIKE '%prenatal%'
                OR LOWER(mapped_concept_display) LIKE '%maternity%'
            THEN 'pregnancy'
            ELSE 'other'
        END AS code_type
    FROM code_clusters
    GROUP BY mapped_concept_code, mapped_concept_display
)

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Cluster membership flags
    CASE WHEN obs.cluster_id = 'PREG_COD' THEN TRUE ELSE FALSE END AS from_preg_cod_cluster,
    CASE WHEN obs.cluster_id = 'PREGDEL_COD' THEN TRUE ELSE FALSE END AS from_pregdel_cod_cluster,
    
    -- Code categorization based on analysis
    cc.in_preg_cod = 1 AS code_exists_in_preg_cod,
    cc.in_pregdel_cod = 1 AS code_exists_in_pregdel_cod,
    cc.code_type,
    
    -- Simplified flags for fact model use
    -- A pregnancy code is one that indicates active pregnancy (not delivery/outcome)
    CASE 
        WHEN cc.code_type = 'pregnancy' THEN TRUE 
        ELSE FALSE 
    END AS is_pregnancy_code,
    
    -- A delivery/outcome code is one that indicates pregnancy has ended
    CASE 
        WHEN cc.code_type IN ('delivery', 'pregnancy_loss') THEN TRUE 
        ELSE FALSE 
    END AS is_delivery_outcome_code,

    -- Enhanced pregnancy status categorisation
    CASE
        WHEN cc.code_type = 'pregnancy' AND LOWER(obs.mapped_concept_display) LIKE '%pregnant%' THEN 'Active Pregnancy'
        WHEN cc.code_type = 'pregnancy' AND LOWER(obs.mapped_concept_display) LIKE '%gravid%' THEN 'Pregnancy Confirmed'
        WHEN cc.code_type = 'delivery' AND (LOWER(obs.mapped_concept_display) LIKE '%delivery%' OR LOWER(obs.mapped_concept_display) LIKE '%birth%') THEN 'Live Birth'
        WHEN cc.code_type = 'delivery' AND LOWER(obs.mapped_concept_display) LIKE '%caesarean%' THEN 'Caesarean Birth'
        WHEN cc.code_type = 'pregnancy_loss' AND (LOWER(obs.mapped_concept_display) LIKE '%termination%' OR LOWER(obs.mapped_concept_display) LIKE '%abortion%') THEN 'Termination of Pregnancy'
        WHEN cc.code_type = 'pregnancy_loss' AND LOWER(obs.mapped_concept_display) LIKE '%miscarriage%' THEN 'Miscarriage'
        WHEN cc.code_type = 'pregnancy' THEN 'Pregnancy Status'
        WHEN cc.code_type IN ('delivery', 'pregnancy_loss') THEN 'Pregnancy Outcome'
        ELSE 'Pregnancy Related'
    END AS pregnancy_status_category,

    -- Clinical context for safety and care planning
    CASE
        WHEN cc.code_type = 'pregnancy' THEN 'Active Pregnancy (medication safety critical)'
        WHEN cc.code_type IN ('delivery', 'pregnancy_loss') THEN 'Post-pregnancy care period'
        ELSE 'Maternity care context'
    END AS clinical_safety_context

FROM ({{ get_observations("'PREG_COD', 'PREGDEL_COD'") }}) obs
LEFT JOIN code_categories cc 
    ON obs.mapped_concept_code = cc.mapped_concept_code
LEFT JOIN {{ ref('dim_person_active_patients') }} ap
    ON obs.person_id = ap.person_id
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC