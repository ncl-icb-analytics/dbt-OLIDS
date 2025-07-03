{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: Person Smoking Status Facts - Current and historical smoking status for clinical decision support and population health monitoring.

Business Purpose:
• Support clinical decision-making by providing current smoking status for care delivery
• Enable population health analytics and smoking cessation programme targeting
• Provide comprehensive smoking history for longitudinal health trend analysis
• Support quality measurement and smoking-related health outcomes tracking

Data Granularity:
• One row per person with smoking observation data
• Includes current status and complete smoking history
• Limited to people with recorded smoking observations

Key Features:
• Current smoking status based on most recent clinical observation
• Complete smoking history with earliest and all recorded observations
• Integrated with person demographics for population health analysis
• Real-time refresh tracking for data currency monitoring'"
        ]
    )
}}

-- Smoking Status Fact Table
-- Business Logic: Current smoking status based on most recent smoking observation

WITH smoking_history AS (
    SELECT
        person_id,
        MIN(clinical_effective_date) AS earliest_smoking_date,
        ARRAY_AGG(DISTINCT concept_code) AS all_smoking_concept_codes,
        ARRAY_AGG(DISTINCT code_description) AS all_smoking_concept_displays
    FROM {{ ref('int_smoking_status_all') }}
    GROUP BY person_id
),

current_smoking_status AS (
    SELECT
        p.person_id,
        latest.clinical_effective_date AS latest_smoking_date,
        latest.concept_code AS latest_concept_code,
        latest.code_description AS latest_code_description,
        latest.source_cluster_id AS latest_cluster_id,

        -- Determine smoking status based on latest record (use existing column)
        latest.smoking_status,

        -- Include history
        hist.earliest_smoking_date,
        hist.all_smoking_concept_codes,
        hist.all_smoking_concept_displays,

        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    LEFT JOIN
        {{ ref('int_smoking_status_latest') }} AS latest
        ON p.person_id = latest.person_id
    LEFT JOIN smoking_history AS hist ON p.person_id = hist.person_id
)

SELECT
    person_id,
    age,
    smoking_status,
    latest_smoking_date,
    earliest_smoking_date,
    latest_concept_code,
    latest_code_description,
    latest_cluster_id,
    all_smoking_concept_codes,
    all_smoking_concept_displays,
    CURRENT_TIMESTAMP() AS last_refresh_date
FROM current_smoking_status
WHERE latest_smoking_date IS NOT NULL -- Only include people with smoking data
