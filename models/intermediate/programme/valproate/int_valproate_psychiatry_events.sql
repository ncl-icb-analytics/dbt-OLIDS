{{ config(
    materialized='table',
    description='Intermediate table extracting all psychiatry-related events for each person, using mapped concepts, observation, and valproate program codes (category PSYCH).',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: Valproate Psychiatry Events - Comprehensive collection of all psychiatry-related events for valproate patient safety monitoring.

Clinical Purpose:
• Gathers all psychiatry specialty events for patients on valproate therapy
• Supports psychiatry pathway tracking for valproate patient safety monitoring
• Enables comprehensive monitoring of psychiatric care in valproate safety programme
• Provides foundation data for valproate programme psychiatry specialty analysis

Data Granularity:
• One row per psychiatry event observation for patients on valproate therapy
• Covers all psychiatry-related observations using PSYCH code category
• Includes clinical effective dates for temporal analysis and psychiatry pathway tracking
• Contains concept codes and descriptions for detailed psychiatry event documentation

Key Features:
• PSYCH code category filtering for comprehensive psychiatry event capture
• Integration with valproate programme codes for psychiatry pathway tracking
• Support for psychiatry specialty monitoring in valproate safety programme
• Foundation data for psychiatric care pathway analysis in valproate patients'"
    ]
) }}

SELECT
    pp.person_id,
    o.clinical_effective_date::date AS psych_event_date,
    o.id AS psych_observation_id,
    mc.concept_code AS psych_concept_code,
    mc.code_description AS psych_concept_display,
    vpc.code_category AS psych_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
    ON o.observation_core_concept_id = mc.source_code_id
INNER JOIN {{ ref('stg_codesets_valproate_prog_codes') }} AS vpc
    ON mc.concept_code = vpc.code
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'PSYCH'
