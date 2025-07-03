{{ config(
    materialized='table',
    description='Intermediate table extracting all neurology-related events for each person, using mapped concepts, observation, and valproate program codes (category NEUROLOGY).',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: Valproate Neurology Events - Comprehensive collection of all neurology-related events for valproate patient safety monitoring.

Clinical Purpose:
• Gathers all neurology specialty events for patients on valproate therapy
• Supports neurology pathway tracking for valproate patient safety monitoring
• Enables comprehensive monitoring of neurological care in valproate safety programme
• Provides foundation data for valproate programme neurology specialty analysis

Data Granularity:
• One row per neurology event observation for patients on valproate therapy
• Covers all neurology-related observations using NEUROLOGY code category
• Includes clinical effective dates for temporal analysis and neurology pathway tracking
• Contains concept codes and descriptions for detailed neurology event documentation

Key Features:
• NEUROLOGY code category filtering for comprehensive neurology event capture
• Integration with valproate programme codes for neurology pathway tracking
• Support for neurology specialty monitoring in valproate safety programme
• Foundation data for neurological care pathway analysis in valproate patients'"
    ]
) }}

SELECT
    pp.person_id,
    o.clinical_effective_date::date AS neurology_event_date,
    o.id AS neurology_observation_id,
    mc.concept_code AS neurology_concept_code,
    mc.code_description AS neurology_concept_display,
    vpc.code_category AS neurology_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
    ON o.observation_core_concept_id = mc.source_code_id
INNER JOIN {{ ref('stg_codesets_valproate_prog_codes') }} AS vpc
    ON mc.concept_code = vpc.code
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'NEUROLOGY'
