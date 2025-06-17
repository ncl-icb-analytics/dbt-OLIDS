{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All HbA1c measurements from observations.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Handles both IFCC and DCCT measurement types with proper unit tracking.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(6,2)) AS hba1c_value,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value,
        
        -- Flag measurement types
        CASE 
            WHEN obs.cluster_id AS source_cluster_id = 'IFCCHBAM_COD' THEN TRUE 
            ELSE FALSE 
        END AS is_ifcc,
        
        CASE 
            WHEN obs.cluster_id AS source_cluster_id = 'DCCTHBA1C_COD' THEN TRUE 
            ELSE FALSE 
        END AS is_dcct
        
    FROM ({{ get_observations("'IFCCHBAM_COD', 'DCCTHBA1C_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    hba1c_value,
    concept_code,
    concept_display,
    source_cluster_id,
    is_ifcc,
    is_dcct,
    original_result_value,
    
    -- Data quality validation (basic range checks)
    CASE 
        WHEN is_ifcc AND hba1c_value BETWEEN 10 AND 200 THEN TRUE  -- IFCC: mmol/mol
        WHEN is_dcct AND hba1c_value BETWEEN 3 AND 20 THEN TRUE    -- DCCT: %
        ELSE FALSE
    END AS is_valid_hba1c,
    
    -- Clinical categorisation for DCCT values
    CASE 
        WHEN is_dcct AND hba1c_value < 6.5 THEN 'Normal'
        WHEN is_dcct AND hba1c_value < 7.0 THEN 'Target'
        WHEN is_dcct AND hba1c_value < 7.5 THEN 'Above Target'
        WHEN is_dcct AND hba1c_value < 10.0 THEN 'Poor Control'
        WHEN is_dcct AND hba1c_value >= 10.0 THEN 'Very Poor Control'
        WHEN is_ifcc AND hba1c_value < 48 THEN 'Normal'          -- <6.5%
        WHEN is_ifcc AND hba1c_value < 53 THEN 'Target'          -- <7.0%
        WHEN is_ifcc AND hba1c_value < 58 THEN 'Above Target'    -- <7.5%
        WHEN is_ifcc AND hba1c_value < 86 THEN 'Poor Control'    -- <10.0%
        WHEN is_ifcc AND hba1c_value >= 86 THEN 'Very Poor Control'
        ELSE 'Invalid'
    END AS hba1c_category

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC 