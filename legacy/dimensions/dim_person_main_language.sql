-- ==========================================================================
-- Dimension Dynamic Table holding the latest preferred language and interpreter needs for ALL persons.
-- Starts from PATIENT_PERSON and LEFT JOINs the latest language and interpreter records if available.
-- Language fields display 'Not Recorded' for persons with no recorded preferred language.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_MAIN_LANGUAGE (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID NUMBER, -- Surrogate key for the patient from the PATIENT table
    LATEST_LANGUAGE_DATE DATE, -- Date of the most recent language observation for the person; NULL if not recorded
    CONCEPT_ID VARCHAR, -- Concept ID of the latest language observation; 'Not Recorded' if NULL
    CONCEPT_CODE VARCHAR, -- Concept code of the latest language observation; 'Not Recorded' if NULL
    TERM VARCHAR, -- Full term/description of the latest language observation; 'Not Recorded' if NULL
    LANGUAGE VARCHAR, -- Clean language name (e.g., 'Somali' from 'Main spoken language Somali (finding)'); 'Not Recorded' if NULL
    LANGUAGE_TYPE VARCHAR, -- Type of language: 'Spoken', 'Sign', or 'Other Communication Method'; 'Not Recorded' if NULL
    LANGUAGE_CATEGORY VARCHAR, -- Broad language category (e.g., English, Other); 'Not Recorded' if NULL
    INTERPRETER_NEEDED BOOLEAN, -- Whether an interpreter is needed (based on REQINTERPRETER_COD)
    INTERPRETER_TYPE VARCHAR, -- Type of interpreter needed (e.g., 'Language', 'Sign Language', 'Deafblind', 'Other'); NULL if no interpreter needed
    COMMUNICATION_SUPPORT_NEEDED BOOLEAN, -- Whether additional communication support is needed (e.g., lipspeaker, note taker)
    COMMUNICATION_SUPPORT_TYPE VARCHAR -- Type of communication support needed; NULL if no support needed
)
COMMENT = 'Dimension table providing the latest recorded preferred language and interpreter needs for every person. If no preferred language is recorded for a person, language-related fields default to \'Not Recorded\'. The LANGUAGE field provides a clean version of the language name without prefixes or suffixes. Includes both preferred language (PREFLANG_COD) and interpreter requirements (REQINTERPRETER_COD).'
TARGET_LAG = '4 hours'
REFRESH_MODE = auto
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH LatestLanguagePerPerson AS (
    -- Identifies the single most recent language record for each person from the OBSERVATION table.
    -- Uses ROW_NUMBER() partitioned by person_id, ordered by clinical_effective_date (desc) and observation_lds_id (desc as tie-breaker).
    SELECT
        pp."person_id",
        p."sk_patient_id",
        o."clinical_effective_date",
        mc.concept_id,
        mc.concept_code,
        mc.code_description AS term,
        -- Extract just the language name from the description
        CASE
            WHEN mc.code_description LIKE 'Main spoken language %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(mc.code_description, '^Main spoken language ', ''), ' \\(finding\\)$', '')
            WHEN mc.code_description LIKE 'Using %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(mc.code_description, '^Using ', ''), ' \\(observable entity\\)$', '')
            WHEN mc.code_description LIKE 'Uses %' THEN
                REGEXP_REPLACE(REGEXP_REPLACE(mc.code_description, '^Uses ', ''), ' \\(finding\\)$', '')
            WHEN mc.code_description LIKE 'Preferred method of communication: %' THEN
                REGEXP_REPLACE(mc.code_description, '^Preferred method of communication: ', '')
            ELSE mc.code_description
        END AS language,
        -- Categorise the language type
        CASE
            WHEN mc.code_description LIKE '%sign language%' OR
                 mc.code_description LIKE '%Sign Language%' THEN 'Sign'
            WHEN mc.code_description LIKE '%Makaton%' OR
                 mc.code_description LIKE '%Preferred method of communication%' THEN 'Other Communication Method'
            ELSE 'Spoken'
        END AS language_type,
        mc.cluster_description AS language_category,
        o."id" AS observation_lds_id -- Include for potential tie-breaking
    FROM
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS mc
        ON o."observation_core_concept_id" = mc.source_code_id
    JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
        ON o."patient_id" = pp."patient_id"
    JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
        ON o."patient_id" = p."id"
    WHERE
        mc.cluster_id = 'PREFLANG_COD'
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pp."person_id"
            -- Order by date first, then by observation ID as a tie-breaker
            ORDER BY o."clinical_effective_date" DESC, o."id" DESC
        ) = 1 -- Get only the latest record per person
),
LatestInterpreterNeeds AS (
    -- Identifies the latest interpreter needs for each person
    SELECT
        pp."person_id",
        o."clinical_effective_date",
        mc.concept_id,
        mc.code_description AS term,
        -- Determine if interpreter is needed
        CASE
            WHEN mc.code_description LIKE '%interpreter needed%' OR
                 mc.code_description LIKE '%Requires %interpreter%' OR
                 mc.code_description LIKE '%Uses %interpreter%' THEN TRUE
            WHEN mc.code_description LIKE '%interpreter not needed%' THEN FALSE
            ELSE NULL
        END AS interpreter_needed,
        -- Categorise interpreter type
        CASE
            WHEN mc.code_description LIKE '%sign language%' OR
                 mc.code_description LIKE '%Sign Language%' THEN 'Sign Language'
            WHEN mc.code_description LIKE '%deafblind%' THEN 'Deafblind'
            WHEN mc.code_description LIKE '%language interpreter%' THEN 'Language'
            WHEN mc.code_description LIKE '%lipspeaker%' OR
                 mc.code_description LIKE '%note taker%' OR
                 mc.code_description LIKE '%speech to text%' THEN 'Other'
            ELSE NULL
        END AS interpreter_type,
        -- Determine if additional communication support is needed
        CASE
            WHEN mc.code_description LIKE '%lipspeaker%' OR
                 mc.code_description LIKE '%note taker%' OR
                 mc.code_description LIKE '%speech to text%' OR
                 mc.code_description LIKE '%aphasia-friendly%' OR
                 mc.code_description LIKE '%support for %communication%' OR
                 mc.code_description LIKE '%deafblind%' OR
                 mc.code_description LIKE '%manual alphabet%' OR
                 mc.code_description LIKE '%block alphabet%' OR
                 mc.code_description LIKE '%sighted guide%' OR
                 mc.code_description LIKE '%communicator guide%' THEN TRUE
            WHEN mc.code_description LIKE '%interpreter not needed%' OR
                 mc.code_description LIKE '%no support needed%' THEN FALSE
            ELSE NULL
        END AS communication_support_needed,
        -- Categorise communication support type
        CASE
            WHEN mc.code_description LIKE '%lipspeaker%' THEN 'Lipspeaker'
            WHEN mc.code_description LIKE '%note taker%' THEN 'Note Taker'
            WHEN mc.code_description LIKE '%speech to text%' THEN 'Speech to Text'
            WHEN mc.code_description LIKE '%aphasia-friendly%' THEN 'Aphasia Support'
            WHEN mc.code_description LIKE '%deafblind%' THEN 'Deafblind Support'
            WHEN mc.code_description LIKE '%manual alphabet%' OR
                 mc.code_description LIKE '%block alphabet%' THEN 'Deafblind Alphabet'
            WHEN mc.code_description LIKE '%sighted guide%' THEN 'Sighted Guide'
            WHEN mc.code_description LIKE '%communicator guide%' THEN 'Communicator Guide'
            WHEN mc.code_description LIKE '%support for %communication%' THEN 'Communication Support'
            ELSE NULL
        END AS communication_support_type,
        o."id" AS observation_lds_id
    FROM
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS mc
        ON o."observation_core_concept_id" = mc.source_code_id
    JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
        ON o."patient_id" = pp."patient_id"
    WHERE
        mc.cluster_id = 'REQINTERPRETER_COD'
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pp."person_id"
            ORDER BY o."clinical_effective_date" DESC, o."id" DESC
        ) = 1
)
-- Constructs the final dimension by selecting all persons from PATIENT_PERSON and PATIENT tables,
-- then LEFT JOINing their latest language and interpreter information (if available).
SELECT
    pp."person_id",
    p."sk_patient_id",
    -- Language fields from the latest record
    llpp."clinical_effective_date" AS latest_language_date,
    COALESCE(llpp.concept_id, 'Not Recorded') AS concept_id,
    COALESCE(llpp.concept_code, 'Not Recorded') AS concept_code,
    COALESCE(llpp.term, 'Not Recorded') AS term,
    COALESCE(llpp.language, 'Not Recorded') AS language,
    COALESCE(llpp.language_type, 'Not Recorded') AS language_type,
    COALESCE(llpp.language_category, 'Not Recorded') AS language_category,
    -- Interpreter and communication support fields
    lin.interpreter_needed,
    lin.interpreter_type,
    lin.communication_support_needed,
    lin.communication_support_type
FROM
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS pp
LEFT JOIN
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS p
    ON pp."patient_id" = p."id"
LEFT JOIN
    LatestLanguagePerPerson llpp ON pp."person_id" = llpp."person_id"
LEFT JOIN
    LatestInterpreterNeeds lin ON pp."person_id" = lin."person_id";
