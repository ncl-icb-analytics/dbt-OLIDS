{{
    config(
        materialized='table',
        cluster_by=['person_id', 'condition_name'])
}}

-- Clinical Condition Episodes Fact Table
-- Historical tracking of all condition episodes without QOF restrictions
-- Captures multiple on/off cycles per person per condition

WITH all_condition_events AS (
    -- Asthma events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Asthma' as condition_name,
        'AST' as condition_code,
        'Respiratory' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_asthma_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Atrial Fibrillation events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Atrial Fibrillation' as condition_name,
        'AF' as condition_code,
        'Cardiovascular' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_atrial_fibrillation_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Cancer events (diagnosis only - no resolution codes)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Cancer' as condition_name,
        'CAN' as condition_code,
        'Oncology' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_cancer_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- CHD events (diagnosis only - permanent condition)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Coronary Heart Disease' as condition_name,
        'CHD' as condition_code,
        'Cardiovascular' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_chd_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- CKD events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Chronic Kidney Disease' as condition_name,
        'CKD' as condition_code,
        'Renal' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_ckd_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- COPD events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'COPD' as condition_name,
        'COPD' as condition_code,
        'Respiratory' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_copd_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Dementia events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Dementia' as condition_name,
        'DEM' as condition_code,
        'Mental Health' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_dementia_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- Depression events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Depression' as condition_name,
        'DEP' as condition_code,
        'Mental Health' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_depression_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Diabetes events (now standardized)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Diabetes' as condition_name,
        'DM' as condition_code,
        'Metabolic' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_diabetes_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Epilepsy events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Epilepsy' as condition_name,
        'EP' as condition_code,
        'Neurology' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_epilepsy_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Familial Hypercholesterolaemia events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Familial Hypercholesterolaemia' as condition_name,
        'FH' as condition_code,
        'Genetics' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_familial_hypercholesterolaemia_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- Frailty events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Frailty' as condition_name,
        'FRAIL' as condition_code,
        'Geriatric' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_frailty_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- Gestational Diabetes events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Gestational Diabetes' as condition_name,
        'GESTDIAB' as condition_code,
        'Maternity' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_gestational_diabetes_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- Heart Failure events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Heart Failure' as condition_name,
        'HF' as condition_code,
        'Cardiovascular' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_heart_failure_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Hypertension events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Hypertension' as condition_name,
        'HTN' as condition_code,
        'Cardiovascular' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_hypertension_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Learning Disability events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Learning Disability' as condition_name,
        'LD' as condition_code,
        'Neurodevelopmental' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_learning_disability_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- NAFLD events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'NAFLD' as condition_name,
        'NAFLD' as condition_code,
        'Hepatology' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_nafld_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- NDH events (using composite diagnosis flag)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Non-Diabetic Hyperglycaemia' as condition_name,
        'NDH' as condition_code,
        'Metabolic' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_ndh_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- Osteoporosis events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Osteoporosis' as condition_name,
        'OST' as condition_code,
        'Musculoskeletal' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_osteoporosis_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- PAD events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Peripheral Arterial Disease' as condition_name,
        'PAD' as condition_code,
        'Cardiovascular' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_pad_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- Palliative Care events (now standardized)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Palliative Care' as condition_name,
        'PC' as condition_code,
        'Palliative Care' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_palliative_care_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Rheumatoid Arthritis events (diagnosis only)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Rheumatoid Arthritis' as condition_name,
        'RA' as condition_code,
        'Musculoskeletal' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_rheumatoid_arthritis_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    UNION ALL
    
    -- SMI events
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Severe Mental Illness' as condition_name,
        'SMI' as condition_code,
        'Mental Health' as clinical_domain,
        CASE 
            WHEN is_diagnosis_code THEN 'onset'
            WHEN is_resolved_code THEN 'resolved'
        END as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_smi_diagnoses_all') }}
    WHERE is_diagnosis_code OR is_resolved_code
    
    UNION ALL
    
    -- Stroke/TIA events (using composite diagnosis flag)
    SELECT 
        person_id,
        clinical_effective_date as event_date,
        'Stroke and TIA' as condition_name,
        'STIA' as condition_code,
        'Neurology' as clinical_domain,
        'diagnosis' as event_type,
        concept_code,
        concept_display
    FROM {{ ref('int_stroke_tia_diagnoses_all') }}
    WHERE is_diagnosis_code
    
    -- Note: Obesity register is BMI-based, not diagnosis code based
    -- CYP Asthma uses same diagnosis codes as regular asthma - age filtering happens in QOF layer
    -- Learning Disability All Ages uses same diagnosis codes as regular LD - age filtering happens in QOF layer
),

events_with_row_numbers AS (
    -- Add row numbers first
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY person_id, condition_name 
            ORDER BY event_date, event_type
        ) as row_num,
        LAG(event_type) OVER (
            PARTITION BY person_id, condition_name 
            ORDER BY event_date, event_type
        ) as prev_event_type
    FROM all_condition_events
),

episode_starts AS (
    -- Identify when new episodes begin
    SELECT 
        *,
        -- Mark episode starts: first event for each person-condition
        CASE 
            WHEN row_num = 1 THEN 1
            -- For onset/resolved conditions: new episode after resolved
            WHEN event_type = 'onset' AND prev_event_type = 'resolved' THEN 1
            ELSE 0
        END as is_episode_start
    FROM events_with_row_numbers
),

episodes_numbered AS (
    -- Assign episode numbers using running sum of episode starts (ensuring >= 1)
    SELECT 
        *,
        GREATEST(1, SUM(is_episode_start) OVER (
            PARTITION BY person_id, condition_name 
            ORDER BY event_date, event_type
            ROWS UNBOUNDED PRECEDING
        )) as episode_number
    FROM episode_starts
),

episode_summary AS (
    -- Aggregate to episode level
    SELECT 
        person_id,
        condition_code,
        condition_name,
        clinical_domain,
        episode_number,
        
        -- Episode dates (use earliest event if no specific onset/diagnosis)
        COALESCE(
            MIN(CASE WHEN event_type IN ('onset', 'diagnosis') THEN event_date END),
            MIN(event_date)
        ) as episode_start_date,
        -- Ensure episode end date is not before start date (data quality check)
        CASE 
            WHEN MAX(CASE WHEN event_type = 'resolved' THEN event_date END) IS NOT NULL
                AND MAX(CASE WHEN event_type = 'resolved' THEN event_date END) >= 
                    COALESCE(
                        MIN(CASE WHEN event_type IN ('onset', 'diagnosis') THEN event_date END),
                        MIN(event_date)
                    )
                THEN MAX(CASE WHEN event_type = 'resolved' THEN event_date END)
            ELSE NULL
        END as episode_end_date,
        MAX(event_date) as episode_last_event_date,
        
        -- Episode status (only resolved if valid end date exists)
        CASE 
            WHEN MAX(CASE WHEN event_type = 'resolved' THEN event_date END) IS NOT NULL
                AND MAX(CASE WHEN event_type = 'resolved' THEN event_date END) >= 
                    COALESCE(
                        MIN(CASE WHEN event_type IN ('onset', 'diagnosis') THEN event_date END),
                        MIN(event_date)
                    )
                THEN 'resolved'
            ELSE 'active'
        END as episode_status,
        
        -- Episode duration (days) - only when resolved and end >= start
        CASE 
            WHEN MAX(CASE WHEN event_type = 'resolved' THEN event_date END) IS NOT NULL
                AND MAX(CASE WHEN event_type = 'resolved' THEN event_date END) >= 
                    COALESCE(
                        MIN(CASE WHEN event_type IN ('onset', 'diagnosis') THEN event_date END),
                        MIN(event_date)
                    )
                THEN DATEDIFF('day', 
                    COALESCE(
                        MIN(CASE WHEN event_type IN ('onset', 'diagnosis') THEN event_date END),
                        MIN(event_date)
                    ),
                    MAX(CASE WHEN event_type = 'resolved' THEN event_date END)
                )
            ELSE NULL
        END as episode_duration_days,
        
        -- Event counts
        COUNT(DISTINCT CASE WHEN event_type IN ('onset', 'diagnosis') THEN event_date END) as diagnosis_event_count,
        COUNT(DISTINCT CASE WHEN event_type = 'resolved' THEN event_date END) as resolution_event_count
        
    FROM episodes_numbered
    GROUP BY 
        person_id, 
        condition_code, 
        condition_name, 
        clinical_domain, 
        episode_number
)

-- Final output with episode metrics
SELECT 
    person_id,
    condition_code,
    condition_name,
    clinical_domain,
    episode_number,
    episode_start_date,
    episode_end_date,
    episode_last_event_date,
    episode_status,
    episode_duration_days,
    diagnosis_event_count,
    resolution_event_count,
    
    -- Person-level condition metrics
    MAX(episode_number) OVER (PARTITION BY person_id, condition_name) as total_episodes_for_condition,
    MIN(episode_start_date) OVER (PARTITION BY person_id, condition_name) as first_ever_diagnosis_date,
    MAX(episode_last_event_date) OVER (PARTITION BY person_id, condition_name) as most_recent_event_date,
    
    -- Current status for this condition (latest episode)
    CASE 
        WHEN episode_number = MAX(episode_number) OVER (PARTITION BY person_id, condition_name)
            THEN episode_status 
    END as current_condition_status

FROM episode_summary
ORDER BY person_id, condition_name, episode_number