{{ config(
    materialized='table',
    description='Intermediate table extracting all ARAF referral-related events for each person, using mapped concepts, observation, and valproate program codes (category REFERRAL).',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: Valproate ARAF Referral Events - Comprehensive collection of all ARAF referral events for valproate patient safety monitoring.

Clinical Purpose:
• Gathers all ARAF referral events for patients on valproate therapy requiring specialist input
• Supports referral tracking for Annual Risk Acknowledgement Form completion
• Enables comprehensive monitoring of specialist referrals in valproate safety programme
• Provides foundation data for valproate programme referral pathway analysis

Data Granularity:
• One row per ARAF referral event observation for patients on valproate therapy
• Covers all referral-related observations using REFERRAL code category
• Includes clinical effective dates for temporal analysis and referral tracking
• Contains concept codes and descriptions for detailed referral documentation

Key Features:
• REFERRAL code category filtering for comprehensive referral event capture
• Integration with valproate programme codes for referral pathway tracking
• Support for specialist referral monitoring in valproate safety programme
• Foundation data for ARAF completion pathway analysis and referral outcomes'"
    ]
) }}

SELECT
    pp.person_id,
    o.clinical_effective_date::date AS araf_referral_event_date,
    o.id AS araf_referral_observation_id,
    mc.concept_code AS araf_referral_concept_code,
    mc.code_description AS araf_referral_concept_display,
    vpc.code_category AS araf_referral_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
    ON o.observation_core_concept_id = mc.source_code_id
INNER JOIN {{ ref('stg_codesets_valproate_prog_codes') }} AS vpc
    ON mc.concept_code = vpc.code
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'REFERRAL'
