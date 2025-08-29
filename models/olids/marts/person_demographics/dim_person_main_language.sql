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

WITH all_language_and_interpreter_records AS (
    -- Get all language preference records
    SELECT
        o.person_id,
        pp1.sk_patient_id,
        o.clinical_effective_date,
        o.mapped_concept_id AS concept_id,
        o.mapped_concept_code AS concept_code,
        o.code_description AS term,
        'PREFLANG_COD' AS record_type,
        -- Extract language name from language preference descriptions
        CASE
            WHEN o.code_description LIKE 'Main spoken language %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(o.code_description, '^Main spoken language ', ''), ' \\(finding\\)$', '')
            WHEN o.code_description LIKE 'Using %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(o.code_description, '^Using ', ''), ' \\(observable entity\\)$', '')
            WHEN o.code_description LIKE 'Uses %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(o.code_description, '^Uses ', ''), ' \\(finding\\)$', '')
            WHEN o.code_description LIKE 'Preferred method of communication: %' THEN
                REGEXP_REPLACE(o.code_description, '^Preferred method of communication: ', '')
            ELSE o.code_description
        END AS language,
        -- Categorise the language type
        CASE
            WHEN o.code_description LIKE '%sign language%' OR
                 o.code_description LIKE '%Sign Language%' THEN 'Sign'
            WHEN o.code_description LIKE '%Makaton%' OR
                 o.code_description LIKE '%Preferred method of communication%' THEN 'Other Communication Method'
            ELSE 'Spoken'
        END AS language_type,
        o.observation_id AS observation_lds_id
    FROM (
        {{ get_observations("'PREFLANG_COD'") }}
    ) o
    JOIN {{ ref('int_patient_person_unique') }} pp1
        ON o.patient_id = pp1.patient_id
    
    UNION ALL
    
    -- Get interpreter requirement records that specify a language
    SELECT
        o.person_id,
        pp2.sk_patient_id,
        o.clinical_effective_date,
        o.mapped_concept_id AS concept_id,
        o.mapped_concept_code AS concept_code,
        o.code_description AS term,
        'REQINTERPRETER_COD' AS record_type,
        -- Extract language from interpreter requirement descriptions
        CASE
            -- Special sign language types
            WHEN o.code_description LIKE 'British Sign Language interpreter needed%' THEN 'British Sign Language'
            WHEN o.code_description LIKE 'Makaton Sign Language interpreter needed%' THEN 'Makaton Sign Language'
            WHEN o.code_description LIKE 'Sign Supported English interpreter needed%' THEN 'Sign Supported English'
            WHEN o.code_description LIKE 'Visual frame sign language interpreter needed%' THEN 'Visual Frame Sign Language'
            WHEN o.code_description LIKE 'Hands-on signing interpreter needed%' THEN 'Hands-on Signing'
            -- Standard language patterns - check for "language interpreter needed" first
            WHEN o.code_description LIKE '%language interpreter needed%' THEN
                REGEXP_REPLACE(o.code_description, ' language interpreter needed.*$', '')
            -- Then check for just "interpreter needed"
            WHEN o.code_description LIKE '%interpreter needed%' AND 
                 o.code_description NOT LIKE 'Requires language interpretation service%' THEN
                REGEXP_REPLACE(o.code_description, ' interpreter needed.*$', '')
            ELSE NULL
        END AS language,
        -- Categorise the language type based on interpreter descriptions
        CASE
            WHEN o.code_description LIKE '%Sign Language%' OR
                 o.code_description LIKE '%sign language%' OR
                 o.code_description LIKE '%signing%' THEN 'Sign'
            ELSE 'Spoken'
        END AS language_type,
        o.observation_id AS observation_lds_id
    FROM (
        {{ get_observations("'REQINTERPRETER_COD'") }}
    ) o
    JOIN {{ ref('int_patient_person_unique') }} pp2
        ON o.patient_id = pp2.patient_id
    WHERE o.code_description LIKE '%interpreter needed%'
      AND o.code_description NOT LIKE 'Requires language interpretation service%'
      AND o.code_description NOT LIKE '%interpreter not needed%'
),

latest_language_per_person AS (
    -- Get the most recent language record (from either preference or interpreter requirement)
    SELECT
        person_id,
        sk_patient_id,
        clinical_effective_date,
        concept_id,
        concept_code,
        term,
        record_type,
        language,
        language_type,
        observation_lds_id
    FROM all_language_and_interpreter_records
    WHERE language IS NOT NULL  -- Only include records where we could extract a language
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY clinical_effective_date DESC, observation_lds_id DESC
    ) = 1
),

latest_interpreter_needs AS (
    -- Identifies the latest interpreter needs for each person
    SELECT
        o.person_id,
        o.clinical_effective_date,
        o.mapped_concept_id AS concept_id,
        o.code_description AS term,
        -- Determine if interpreter is needed
        CASE
            WHEN o.code_description LIKE '%interpreter needed%' OR
                 o.code_description LIKE '%Requires %interpreter%' OR
                 o.code_description LIKE '%Uses %interpreter%' THEN TRUE
            WHEN o.code_description LIKE '%interpreter not needed%' THEN FALSE
            ELSE NULL
        END AS interpreter_needed,
        -- Extract specific interpreter language/type
        CASE
            -- Special sign language types
            WHEN o.code_description LIKE 'British Sign Language interpreter needed%' THEN 'British Sign Language'
            WHEN o.code_description LIKE 'Makaton Sign Language interpreter needed%' THEN 'Makaton Sign Language'
            WHEN o.code_description LIKE 'Sign Supported English interpreter needed%' THEN 'Sign Supported English'
            WHEN o.code_description LIKE 'Visual frame sign language interpreter needed%' THEN 'Visual Frame Sign Language'
            WHEN o.code_description LIKE 'Hands-on signing interpreter needed%' THEN 'Hands-on Signing'
            -- Special service types
            WHEN o.code_description LIKE 'Requires language interpretation service%' THEN 'Language Interpretation Service'
            -- Standard language patterns - check for "language interpreter needed" first
            WHEN o.code_description LIKE '%language interpreter needed%' THEN
                REGEXP_REPLACE(o.code_description, ' language interpreter needed.*$', '')
            -- Then check for just "interpreter needed"
            WHEN o.code_description LIKE '%interpreter needed%' THEN
                REGEXP_REPLACE(o.code_description, ' interpreter needed.*$', '')
            -- Other communication support types
            WHEN o.code_description LIKE '%lipspeaker%' OR
                 o.code_description LIKE '%note taker%' OR
                 o.code_description LIKE '%speech to text%' THEN 'Other'
            ELSE NULL
        END AS interpreter_type,
        -- Determine if additional communication support is needed
        CASE
            WHEN o.code_description LIKE '%lipspeaker%' OR
                 o.code_description LIKE '%note taker%' OR
                 o.code_description LIKE '%speech to text%' OR
                 o.code_description LIKE '%aphasia-friendly%' OR
                 o.code_description LIKE '%support for %communication%' OR
                 o.code_description LIKE '%deafblind%' OR
                 o.code_description LIKE '%manual alphabet%' OR
                 o.code_description LIKE '%block alphabet%' OR
                 o.code_description LIKE '%sighted guide%' OR
                 o.code_description LIKE '%communicator guide%' THEN TRUE
            WHEN o.code_description LIKE '%interpreter not needed%' OR
                 o.code_description LIKE '%no support needed%' THEN FALSE
            ELSE NULL
        END AS communication_support_needed,
        -- Categorise communication support type
        CASE
            WHEN o.code_description LIKE '%lipspeaker%' THEN 'Lipspeaker'
            WHEN o.code_description LIKE '%note taker%' THEN 'Note Taker'
            WHEN o.code_description LIKE '%speech to text%' THEN 'Speech to Text'
            WHEN o.code_description LIKE '%aphasia-friendly%' THEN 'Aphasia Support'
            WHEN o.code_description LIKE '%deafblind%' THEN 'Deafblind Support'
            WHEN o.code_description LIKE '%manual alphabet%' OR
                 o.code_description LIKE '%block alphabet%' THEN 'Deafblind Alphabet'
            WHEN o.code_description LIKE '%sighted guide%' THEN 'Sighted Guide'
            WHEN o.code_description LIKE '%communicator guide%' THEN 'Communicator Guide'
            WHEN o.code_description LIKE '%support for %communication%' THEN 'Communication Support'
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
        llpp.record_type AS language_source,  -- Track whether main language came from preference or interpreter record
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
    pwl.language_source,
    pwl.interpreter_needed,
    pwl.interpreter_type,
    pwl.communication_support_needed,
    pwl.communication_support_type
FROM all_persons AS ap
LEFT JOIN persons_with_language AS pwl
    ON ap.person_id = pwl.person_id
