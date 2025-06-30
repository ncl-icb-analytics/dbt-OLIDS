-- ==========================================================================
-- Dimension Dynamic Table holding the latest carer status for persons who have a recorded carer status.
-- Only includes persons who have a record in ISACARER_COD, NOTACARER_COD, or UNPAIDCARER_COD clusters.
-- Carer types and details are categorised based on code descriptions to maintain flexibility with changing codes.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_IS_CARER (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID NUMBER, -- Surrogate key for the patient from the PATIENT table
    LATEST_CARER_STATUS_DATE DATE, -- Date of the most recent carer status observation
    CONCEPT_ID VARCHAR, -- Concept ID of the latest carer status observation
    CONCEPT_CODE VARCHAR, -- Concept code of the latest carer status observation
    TERM VARCHAR, -- Full term/description of the latest carer status observation
    IS_CARER BOOLEAN, -- Whether the person is currently a carer (TRUE) or not (FALSE)
    CARER_TYPE VARCHAR, -- Type of carer (e.g., 'Primary Carer', 'Informal Carer', 'Unpaid Carer')
    CARER_DETAILS VARCHAR, -- Additional details about carer status (e.g., 'Caring for person with health condition', 'Caring for family member')
    SOURCE_CLUSTER_IDS ARRAY -- Array of cluster IDs that contributed to this carer status (e.g., ['ISACARER_COD', 'UNPAIDCARER_COD'])
)
COMMENT = 'Dimension table providing the latest recorded carer status for persons who have a carer status record. Only includes persons with a record in ISACARER_COD, NOTACARER_COD, or UNPAIDCARER_COD clusters. This table specifically tracks patients who are carers themselves, not patients who have carers.'
TARGET_LAG = '4 hours'
REFRESH_MODE = auto
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH LatestCarerStatusPerPerson AS (
    -- First, collect all cluster IDs and determine carer status for each observation
    WITH ObservationClusters AS (
        SELECT
            o."id" AS observation_id,
            ARRAY_AGG(DISTINCT mc.cluster_id) WITHIN GROUP (ORDER BY mc.cluster_id) AS cluster_ids,
            -- Pre-calculate carer status based on clusters
            CASE
                WHEN ARRAY_CONTAINS('NOTACARER_COD'::VARIANT, ARRAY_AGG(DISTINCT mc.cluster_id)) THEN FALSE
                WHEN ARRAY_CONTAINS('ISACARER_COD'::VARIANT, ARRAY_AGG(DISTINCT mc.cluster_id)) OR
                     ARRAY_CONTAINS('UNPAIDCARER_COD'::VARIANT, ARRAY_AGG(DISTINCT mc.cluster_id)) THEN TRUE
                ELSE NULL
            END AS is_carer
        FROM
            "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
        JOIN
            DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS mc
            ON o."observation_core_concept_id" = mc.source_code_id
        WHERE
            mc.cluster_id IN ('ISACARER_COD', 'NOTACARER_COD', 'UNPAIDCARER_COD')
        GROUP BY
            o."id"
    )
    -- Then get the latest observation per person with all its details
    SELECT
        pp."person_id",
        p."sk_patient_id",
        o."clinical_effective_date",
        mc.concept_id,
        mc.concept_code,
        mc.code_description AS term,
        oc.is_carer,
        -- Determine carer type based on code descriptions
        CASE
            WHEN LOWER(mc.code_description) LIKE '%primary caregiver%' THEN 'Primary Carer'
            WHEN LOWER(mc.code_description) LIKE '%informal caregiver%' THEN 'Informal Carer'
            WHEN LOWER(mc.code_description) LIKE '%unpaid caregiver%' THEN 'Unpaid Carer'
            WHEN LOWER(mc.code_description) LIKE '%professional%' OR
                 LOWER(mc.code_description) LIKE '%occupation%' THEN 'Professional Carer'
            WHEN LOWER(mc.code_description) LIKE '%carer allowance%' THEN 'Carer Receiving Allowance'
            WHEN oc.is_carer THEN 'Other Carer'
            ELSE NULL
        END AS carer_type,
        -- Determine carer details based on code descriptions
        CASE
            -- Health conditions
            WHEN LOWER(mc.code_description) LIKE '%dementia%' OR
                 LOWER(mc.code_description) LIKE '%chronic%' OR
                 LOWER(mc.code_description) LIKE '%disability%' OR
                 LOWER(mc.code_description) LIKE '%mental%' OR
                 LOWER(mc.code_description) LIKE '%terminal%' OR
                 LOWER(mc.code_description) LIKE '%alcohol%' OR
                 LOWER(mc.code_description) LIKE '%substance%' THEN 'Caring for person with health condition'
            -- Family relationships
            WHEN LOWER(mc.code_description) LIKE '%father%' OR
                 LOWER(mc.code_description) LIKE '%mother%' OR
                 LOWER(mc.code_description) LIKE '%spouse%' OR
                 LOWER(mc.code_description) LIKE '%husband%' OR
                 LOWER(mc.code_description) LIKE '%wife%' OR
                 LOWER(mc.code_description) LIKE '%partner%' OR
                 LOWER(mc.code_description) LIKE '%relative%' THEN 'Caring for family member'
            -- Other relationships
            WHEN LOWER(mc.code_description) LIKE '%neighbour%' OR
                 LOWER(mc.code_description) LIKE '%friend%' THEN 'Caring for non-family member'
            -- Special cases
            WHEN LOWER(mc.code_description) LIKE '%contingency plan%' THEN 'Has carer contingency plan'
            WHEN LOWER(mc.code_description) LIKE '%patient themselves providing care%' THEN 'Patient is carer'
            WHEN oc.is_carer THEN 'Caring for other'
            ELSE NULL
        END AS carer_details,
        oc.cluster_ids AS source_cluster_ids,
        o."id" AS observation_lds_id -- Include for potential tie-breaking
    FROM
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
    JOIN
        ObservationClusters oc
        ON o."id" = oc.observation_id
    JOIN
        DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS mc
        ON o."observation_core_concept_id" = mc.source_code_id
    JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
        ON o."patient_id" = pp."patient_id"
    JOIN
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
        ON o."patient_id" = p."id"
    WHERE
        mc.cluster_id IN ('ISACARER_COD', 'NOTACARER_COD', 'UNPAIDCARER_COD')
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pp."person_id"
            -- Order by date first, then by observation ID as a tie-breaker
            ORDER BY o."clinical_effective_date" DESC, o."id" DESC
        ) = 1 -- Get only the latest record per person
)
-- Select only persons who have a carer status record
SELECT
    lcsp."person_id",
    lcsp."sk_patient_id",
    lcsp."clinical_effective_date" AS latest_carer_status_date,
    lcsp.concept_id,
    lcsp.concept_code,
    lcsp.term,
    lcsp.is_carer,
    lcsp.carer_type,
    lcsp.carer_details,
    lcsp.source_cluster_ids AS SOURCE_CLUSTER_IDS
FROM
    LatestCarerStatusPerPerson lcsp;
