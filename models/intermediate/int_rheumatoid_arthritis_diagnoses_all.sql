{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All rheumatoid arthritis diagnosis observations from clinical records.
Uses QOF rheumatoid arthritis cluster ID:
- RARTH_COD: Rheumatoid arthritis diagnoses

Clinical Purpose:
- QOF rheumatoid arthritis register data collection (aged 16+)
- Inflammatory arthritis management monitoring
- Disease-modifying therapy planning
- Joint health assessment

Key QOF Requirements:
- Register inclusion: Rheumatoid arthritis diagnosis (RARTH_COD) for patients aged 16+
- No resolution codes - RA is considered permanent condition
- Age restrictions for register eligibility
- Important for rheumatology care pathways

Note: Rheumatoid arthritis does not have resolved codes as it is considered a permanent condition.
The register is based purely on the presence of diagnostic codes for eligible ages.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_rheumatoid_arthritis_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag rheumatoid arthritis diagnosis codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'RARTH_COD' THEN TRUE ELSE FALSE END AS is_rheumatoid_arthritis_diagnosis_code
        
    FROM {{ get_observations("'RARTH_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_rheumatoid_arthritis_diagnosis_code

FROM base_observations

-- Sort for consistent output
ORDER BY obs.person_id, obs.clinical_effective_date DESC 