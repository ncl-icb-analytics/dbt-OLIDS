{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All familial hypercholesterolaemia diagnosis observations from clinical records.
Uses QOF familial hypercholesterolaemia cluster ID:
- FHYP_COD: Familial hypercholesterolaemia diagnoses

Clinical Purpose:
- QOF familial hypercholesterolaemia register data collection
- Genetic lipid disorder management monitoring
- Cardiovascular risk stratification
- Family screening cascade planning

Key QOF Requirements:
- Register inclusion: Familial hypercholesterolaemia diagnosis (FHYP_COD)
- No resolution codes - FHYP is considered permanent genetic condition
- No age restrictions for FHYP register
- Important for high-intensity statin therapy and family screening

Note: Familial hypercholesterolaemia does not have resolved codes as it is a permanent genetic condition.
The register is based purely on the presence of diagnostic codes.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_familial_hypercholesterolaemia_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag familial hypercholesterolaemia diagnosis codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'FHYP_COD' THEN TRUE ELSE FALSE END AS is_fhyp_diagnosis_code
        
    FROM {{ get_observations("'FHYP_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_fhyp_diagnosis_code

FROM base_observations

-- Sort for consistent output
ORDER BY obs.person_id, obs.clinical_effective_date DESC 