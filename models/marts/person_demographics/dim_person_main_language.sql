{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'language', 'interpreter'],
        cluster_by=['person_id'])
}}

-- Person Main Language Dimension Table
-- Holds the latest preferred language and interpreter needs for ALL persons
-- Starts from PATIENT_PERSON and LEFT JOINs the latest language and interpreter records if available
-- Language fields display 'Not Recorded' for persons with no recorded preferred language

WITH latest_language_per_person AS (
    -- Identifies the single most recent language record for each person from the OBSERVATION table
    -- Uses ROW_NUMBER() partitioned by person_id, ordered by clinical_effective_date (desc) and observation_lds_id (desc as tie-breaker)
    SELECT
        o.person_id,
        o.sk_patient_id,
        o.clinical_effective_date,
        o.mapped_concept_id AS concept_id,
        o.mapped_concept_code AS concept_code,
        o.mapped_concept_display AS term,
        -- Extract just the language name from the description
        CASE
            WHEN o.mapped_concept_display LIKE 'Main spoken language %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(o.mapped_concept_display, '^Main spoken language ', ''), ' \\(finding\\)$', '')
            WHEN o.mapped_concept_display LIKE 'Using %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(o.mapped_concept_display, '^Using ', ''), ' \\(observable entity\\)$', '')
            WHEN o.mapped_concept_display LIKE 'Uses %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(o.mapped_concept_display, '^Uses ', ''), ' \\(finding\\)$', '')
            WHEN o.mapped_concept_display LIKE 'Preferred method of communication: %' THEN
                REGEXP_REPLACE(o.mapped_concept_display, '^Preferred method of communication: ', '')
            ELSE o.mapped_concept_display
        END AS language,
        -- Categorise the language type
        CASE
            WHEN o.mapped_concept_display LIKE '%sign language%' OR
                 o.mapped_concept_display LIKE '%Sign Language%' THEN 'Sign'
            WHEN o.mapped_concept_display LIKE '%Makaton%' OR
                 o.mapped_concept_display LIKE '%Preferred method of communication%' THEN 'Other Communication Method'
            ELSE 'Spoken'
        END AS language_type,
        o.cluster_description AS language_category,
        o.observation_id AS observation_lds_id -- Include for potential tie-breaking
    FROM (
        {{ get_observations("'PREFLANG_COD'") }}
    ) o
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY o.person_id
            -- Order by date first, then by observation ID as a tie-breaker
            ORDER BY o.clinical_effective_date DESC, o.observation_id DESC
        ) = 1 -- Get only the latest record per person
),

latest_interpreter_needs AS (
    -- Identifies the latest interpreter needs for each person
    SELECT
        o.person_id,
        o.clinical_effective_date,
        o.mapped_concept_id AS concept_id,
        o.mapped_concept_display AS term,
        -- Determine if interpreter is needed
        CASE
            WHEN o.mapped_concept_display LIKE '%interpreter needed%' OR
                 o.mapped_concept_display LIKE '%Requires %interpreter%' OR
                 o.mapped_concept_display LIKE '%Uses %interpreter%' THEN TRUE
            WHEN o.mapped_concept_display LIKE '%interpreter not needed%' THEN FALSE
            ELSE NULL
        END AS interpreter_needed,
        -- Categorise interpreter type
        CASE
            WHEN o.mapped_concept_display LIKE '%sign language%' OR
                 o.mapped_concept_display LIKE '%Sign Language%' THEN 'Sign Language'
            WHEN o.mapped_concept_display LIKE '%deafblind%' THEN 'Deafblind'
            WHEN o.mapped_concept_display LIKE '%language interpreter%' THEN 'Language'
            WHEN o.mapped_concept_display LIKE '%lipspeaker%' OR
                 o.mapped_concept_display LIKE '%note taker%' OR
                 o.mapped_concept_display LIKE '%speech to text%' THEN 'Other'
            ELSE NULL
        END AS interpreter_type,
        -- Determine if additional communication support is needed
        CASE
            WHEN o.mapped_concept_display LIKE '%lipspeaker%' OR
                 o.mapped_concept_display LIKE '%note taker%' OR
                 o.mapped_concept_display LIKE '%speech to text%' OR
                 o.mapped_concept_display LIKE '%aphasia-friendly%' OR
                 o.mapped_concept_display LIKE '%support for %communication%' OR
                 o.mapped_concept_display LIKE '%deafblind%' OR
                 o.mapped_concept_display LIKE '%manual alphabet%' OR
                 o.mapped_concept_display LIKE '%block alphabet%' OR
                 o.mapped_concept_display LIKE '%sighted guide%' OR
                 o.mapped_concept_display LIKE '%communicator guide%' THEN TRUE
            WHEN o.mapped_concept_display LIKE '%interpreter not needed%' OR
                 o.mapped_concept_display LIKE '%no support needed%' THEN FALSE
            ELSE NULL
        END AS communication_support_needed,
        -- Categorise communication support type
        CASE
            WHEN o.mapped_concept_display LIKE '%lipspeaker%' THEN 'Lipspeaker'
            WHEN o.mapped_concept_display LIKE '%note taker%' THEN 'Note Taker'
            WHEN o.mapped_concept_display LIKE '%speech to text%' THEN 'Speech to Text'
            WHEN o.mapped_concept_display LIKE '%aphasia-friendly%' THEN 'Aphasia Support'
            WHEN o.mapped_concept_display LIKE '%deafblind%' THEN 'Deafblind Support'
            WHEN o.mapped_concept_display LIKE '%manual alphabet%' OR
                 o.mapped_concept_display LIKE '%block alphabet%' THEN 'Deafblind Alphabet'
            WHEN o.mapped_concept_display LIKE '%sighted guide%' THEN 'Sighted Guide'
            WHEN o.mapped_concept_display LIKE '%communicator guide%' THEN 'Communicator Guide'
            WHEN o.mapped_concept_display LIKE '%support for %communication%' THEN 'Communication Support'
            ELSE NULL
        END AS communication_support_type,
        o.observation_id AS observation_lds_id
    FROM (
        {{ get_observations("'REQINTERPRETER_COD'") }}
    ) o
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY o.person_id
            ORDER BY o.clinical_effective_date DESC, o.observation_id DESC
        ) = 1
),

-- Constructs the final dimension by starting with all persons who have language records,
-- then ensuring complete coverage with all persons from the person dimension

-- First get all persons with language records
persons_with_language AS (
    SELECT
        llpp.person_id,
        llpp.sk_patient_id,
        llpp.clinical_effective_date AS latest_language_date,
        llpp.concept_id,
        llpp.concept_code,
        llpp.term,
        llpp.language,
        llpp.language_type,
        llpp.language_category,
        lin.interpreter_needed,
        lin.interpreter_type,
        lin.communication_support_needed,
        lin.communication_support_type
    FROM latest_language_per_person llpp
    LEFT JOIN latest_interpreter_needs lin
        ON llpp.person_id = lin.person_id
),

-- Then get all persons to ensure complete coverage
all_persons AS (
    SELECT person_id
    FROM {{ ref('dim_person') }}
)

SELECT
    ap.person_id,
    COALESCE(pwl.sk_patient_id, NULL) AS sk_patient_id,
    pwl.latest_language_date,
    COALESCE(pwl.concept_id, 'Not Recorded') AS concept_id,
    COALESCE(pwl.concept_code, 'Not Recorded') AS concept_code,
    COALESCE(pwl.term, 'Not Recorded') AS term,
    COALESCE(pwl.language, 'Not Recorded') AS language,
    COALESCE(pwl.language_type, 'Not Recorded') AS language_type,
    COALESCE(pwl.language_category, 'Not Recorded') AS language_category,
    pwl.interpreter_needed,
    pwl.interpreter_type,
    pwl.communication_support_needed,
    pwl.communication_support_type
FROM all_persons AS ap
LEFT JOIN persons_with_language AS pwl
    ON ap.person_id = pwl.person_id
