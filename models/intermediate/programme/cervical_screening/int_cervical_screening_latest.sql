{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        persist_docs={"relation": true},
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Cervical Screening Latest - Most recent cervical screening status per person for current programme management.

Clinical Purpose:
• Current cervical screening status determination for programme coordination
• Real-time screening eligibility and invitation management
• Clinical decision support for current screening requirements
• Programme monitoring and individual patient pathway management

Data Granularity:
• One row per person representing most recent cervical screening observation
• Simple latest record selection without date-based business logic
• Includes all persons regardless of status for comprehensive programme coverage

Key Features:
• Latest screening status with comprehensive cytology and risk information
• Current clinical action requirements and follow-up determination
• Foundation for person-level screening programme analysis
• Essential for real-time programme management and patient care coordination'"
        ]
    )
}}

/*
Latest cervical screening status per person.
Simple QUALIFY-based latest record selection from int_cervical_screening_all.

Business Rules:
- Returns the most recent screening observation per person
- No date-based business logic (kept in fact layer)
- Foundation for person-level screening status analysis

Used for cervical screening programme analysis and current status determination.
*/

SELECT
    observation_id,
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    screening_observation_type,
    is_completed_screening,
    is_unsuitable_screening,
    is_declined_screening,
    is_non_response_screening,
    cytology_result_category,
    cervical_screening_risk_category,
    abnormality_grade,
    sample_adequacy,
    clinical_action_required

FROM {{ ref('int_cervical_screening_all') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id 
    ORDER BY clinical_effective_date DESC, observation_id DESC
) = 1

ORDER BY person_id 