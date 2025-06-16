{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All stroke and transient ischaemic attack (TIA) diagnosis observations from clinical records.
Uses QOF stroke/TIA cluster IDs:
- STRK_COD: Stroke diagnoses
- TIA_COD: Transient ischaemic attack diagnoses

Clinical Purpose:
- QOF stroke/TIA register data collection
- Cerebrovascular disease monitoring
- Secondary stroke prevention planning
- Neurological outcome tracking

Key QOF Requirements:
- Register inclusion: Presence of stroke (STRK_COD) OR TIA (TIA_COD) diagnosis codes
- No age restrictions for stroke/TIA register
- No resolution codes - stroke/TIA are considered permanent conditions
- Critical for secondary prevention and anticoagulation decisions

Note: Stroke and TIA do not have resolved codes as they are considered permanent conditions.
The register is based purely on the presence of diagnostic codes.

Includes ALL persons following intermediate layer principles.
Use as input for fct_person_stroke_tia_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag different types of stroke/TIA codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'STRK_COD' THEN TRUE ELSE FALSE END AS is_stroke_diagnosis_code,
        CASE WHEN obs.source_cluster_id = 'TIA_COD' THEN TRUE ELSE FALSE END AS is_tia_diagnosis_code
        
    FROM {{ get_observations("'STRK_COD', 'TIA_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_diagnosis_aggregates AS (
    
    SELECT
        person_id,
        
        -- Stroke diagnosis dates (STRK_COD)
        MIN(CASE WHEN is_stroke_diagnosis_code THEN clinical_effective_date END) AS earliest_stroke_diagnosis_date,
        MAX(CASE WHEN is_stroke_diagnosis_code THEN clinical_effective_date END) AS latest_stroke_diagnosis_date,
        COUNT(CASE WHEN is_stroke_diagnosis_code THEN 1 END) AS total_stroke_diagnoses,
        
        -- TIA diagnosis dates (TIA_COD)
        MIN(CASE WHEN is_tia_diagnosis_code THEN clinical_effective_date END) AS earliest_tia_diagnosis_date,
        MAX(CASE WHEN is_tia_diagnosis_code THEN clinical_effective_date END) AS latest_tia_diagnosis_date,
        COUNT(CASE WHEN is_tia_diagnosis_code THEN 1 END) AS total_tia_diagnoses,
        
        -- Combined stroke/TIA dates for QOF register logic
        LEAST(
            COALESCE(MIN(CASE WHEN is_stroke_diagnosis_code THEN clinical_effective_date END), '9999-12-31'),
            COALESCE(MIN(CASE WHEN is_tia_diagnosis_code THEN clinical_effective_date END), '9999-12-31')
        ) AS earliest_stroke_tia_date,
        GREATEST(
            COALESCE(MAX(CASE WHEN is_stroke_diagnosis_code THEN clinical_effective_date END), '1900-01-01'),
            COALESCE(MAX(CASE WHEN is_tia_diagnosis_code THEN clinical_effective_date END), '1900-01-01')
        ) AS latest_stroke_tia_date,
        
        -- Arrays for traceability
        ARRAY_AGG(DISTINCT concept_code) 
            WITHIN GROUP (ORDER BY concept_code) 
            AS all_stroke_tia_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) 
            WITHIN GROUP (ORDER BY concept_display) 
            AS all_stroke_tia_concept_displays
            
    FROM base_observations
    GROUP BY person_id
),

final_with_derived_fields AS (
    
    SELECT
        bo.person_id,
        bo.observation_id,
        bo.clinical_effective_date,
        bo.concept_code,
        bo.concept_display,
        bo.source_cluster_id,
        bo.is_stroke_diagnosis_code,
        bo.is_tia_diagnosis_code,
        
        -- Person-level aggregates
        pda.earliest_stroke_diagnosis_date,
        pda.latest_stroke_diagnosis_date,
        pda.total_stroke_diagnoses,
        pda.earliest_tia_diagnosis_date,
        pda.latest_tia_diagnosis_date,
        pda.total_tia_diagnoses,
        CASE 
            WHEN pda.earliest_stroke_tia_date != '9999-12-31' THEN pda.earliest_stroke_tia_date
            ELSE NULL
        END AS earliest_stroke_tia_date,
        CASE 
            WHEN pda.latest_stroke_tia_date != '1900-01-01' THEN pda.latest_stroke_tia_date
            ELSE NULL
        END AS latest_stroke_tia_date,
        
        -- Classification of this specific observation
        CASE 
            WHEN bo.is_stroke_diagnosis_code THEN 'Stroke Diagnosis'
            WHEN bo.is_tia_diagnosis_code THEN 'TIA Diagnosis'
            ELSE 'Unknown'
        END AS stroke_tia_observation_type,
        
        -- QOF context fields for register inclusion
        CASE WHEN pda.total_stroke_diagnoses > 0 THEN TRUE ELSE FALSE END AS has_stroke_diagnosis,
        CASE WHEN pda.total_tia_diagnoses > 0 THEN TRUE ELSE FALSE END AS has_tia_diagnosis,
        CASE WHEN pda.total_stroke_diagnoses > 0 OR pda.total_tia_diagnoses > 0 THEN TRUE ELSE FALSE END AS has_stroke_or_tia_diagnosis,
        
        -- Cerebrovascular event categorisation
        CASE 
            WHEN pda.total_stroke_diagnoses > 0 AND pda.total_tia_diagnoses > 0 THEN 'Both Stroke and TIA'
            WHEN pda.total_stroke_diagnoses > 0 THEN 'Stroke Only'
            WHEN pda.total_tia_diagnoses > 0 THEN 'TIA Only'
            ELSE 'No Events'
        END AS cerebrovascular_event_type,
        
        -- Clinical flags for care planning
        CASE 
            WHEN pda.latest_stroke_tia_date >= CURRENT_DATE - INTERVAL '12 months' THEN TRUE
            ELSE FALSE
        END AS has_recent_stroke_tia_diagnosis,
        CASE 
            WHEN pda.latest_stroke_tia_date >= CURRENT_DATE - INTERVAL '24 months' THEN TRUE
            ELSE FALSE
        END AS has_stroke_tia_diagnosis_last_24m,
        
        -- Disease management indicators
        CASE WHEN (pda.total_stroke_diagnoses + pda.total_tia_diagnoses) = 1 THEN TRUE ELSE FALSE END AS is_single_cerebrovascular_event,
        CASE WHEN (pda.total_stroke_diagnoses + pda.total_tia_diagnoses) > 1 THEN TRUE ELSE FALSE END AS has_multiple_cerebrovascular_events,
        
        -- Secondary prevention planning fields
        CASE 
            WHEN pda.earliest_stroke_tia_date IS NOT NULL 
            THEN CURRENT_DATE - pda.earliest_stroke_tia_date
            ELSE NULL
        END AS days_since_first_stroke_tia,
        
        CASE 
            WHEN pda.earliest_stroke_tia_date >= CURRENT_DATE - INTERVAL '1 year' THEN TRUE
            ELSE FALSE
        END AS is_newly_diagnosed_stroke_tia,
        CASE 
            WHEN pda.earliest_stroke_tia_date < CURRENT_DATE - INTERVAL '1 year' THEN TRUE
            ELSE FALSE
        END AS is_established_stroke_tia,
        CASE 
            WHEN pda.earliest_stroke_tia_date < CURRENT_DATE - INTERVAL '5 years' THEN TRUE
            ELSE FALSE
        END AS is_long_term_stroke_tia,
        
        -- Recurrence risk indicators
        CASE 
            WHEN (pda.total_stroke_diagnoses + pda.total_tia_diagnoses) > 1 
                AND (pda.latest_stroke_tia_date - pda.earliest_stroke_tia_date) > INTERVAL '6 months'
            THEN TRUE
            ELSE FALSE
        END AS has_recurrent_cerebrovascular_events,
        
        -- Stroke progression indicators (TIA → Stroke)
        CASE 
            WHEN pda.total_stroke_diagnoses > 0 
                AND pda.total_tia_diagnoses > 0
                AND pda.earliest_tia_diagnosis_date < pda.earliest_stroke_diagnosis_date
            THEN TRUE
            ELSE FALSE
        END AS has_tia_to_stroke_progression,
        
        -- Arrays for complete traceability
        pda.all_stroke_tia_concept_codes,
        pda.all_stroke_tia_concept_displays
        
    FROM base_observations bo
    LEFT JOIN person_diagnosis_aggregates pda
        ON bo.person_id = pda.person_id
)

SELECT * FROM final_with_derived_fields
ORDER BY person_id, clinical_effective_date 