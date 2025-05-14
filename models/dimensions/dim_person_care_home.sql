-- ==========================================================================
-- Dimension Dynamic Table holding the latest care home/nursing home status for persons.
-- Only includes persons who have a recorded care home, nursing home, or temporary care home status.
-- Uses CAREHOME_COD, NURSEHOME_COD, and TEMPCARHOME_COD clusters.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_CARE_HOME (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID NUMBER, -- Surrogate key for the patient from the PATIENT table
    LATEST_RESIDENCE_DATE DATE, -- Date of the most recent care home/nursing home observation
    CONCEPT_ID VARCHAR, -- Concept ID of the latest observation
    CONCEPT_CODE VARCHAR, -- Concept code of the latest observation
    TERM VARCHAR, -- Full term/description of the latest observation
    IS_CARE_HOME_RESIDENT BOOLEAN, -- Whether the person is currently a care home resident
    IS_NURSING_HOME_RESIDENT BOOLEAN, -- Whether the person is currently a nursing home resident
    IS_TEMPORARY_RESIDENT BOOLEAN, -- Whether the person is temporarily in a care/nursing home
    RESIDENCE_TYPE VARCHAR, -- Type of residence ('Care Home', 'Nursing Home', 'Temporary Care Home')
    RESIDENCE_STATUS VARCHAR, -- Status of residence ('Permanent', 'Temporary')
    SOURCE_CLUSTER_IDS ARRAY -- Array of cluster IDs that contributed to this status (e.g., ['CAREHOME_COD', 'NURSEHOME_COD'])
)
COMMENT = 'Dimension table providing the latest recorded care home/nursing home status for persons who have a recorded residence status. Only includes persons with CAREHOME_COD, NURSEHOME_COD, or TEMPCARHOME_COD records.'
TARGET_LAG = '4 hours'
REFRESH_MODE = auto
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH LatestResidenceStatusPerPerson AS (
    -- First, collect all cluster IDs and determine residence status for each observation
    WITH ObservationClusters AS (
        SELECT
            o."id" AS observation_id,
            ARRAY_AGG(DISTINCT mc.cluster_id) WITHIN GROUP (ORDER BY mc.cluster_id) AS cluster_ids,
            -- Pre-calculate residence status based on clusters
            CASE 
                WHEN ARRAY_CONTAINS('CAREHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT mc.cluster_id)) THEN 'Care Home'
                WHEN ARRAY_CONTAINS('NURSEHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT mc.cluster_id)) THEN 'Nursing Home'
                WHEN ARRAY_CONTAINS('TEMPCARHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT mc.cluster_id)) THEN 'Temporary Care Home'
                ELSE NULL
            END AS residence_type,
            -- Determine if temporary
            ARRAY_CONTAINS('TEMPCARHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT mc.cluster_id)) AS is_temporary
        FROM
            "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
        JOIN
            DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS mc
            ON o."observation_core_concept_id" = mc.source_code_id
        WHERE
            mc.cluster_id IN ('CAREHOME_COD', 'NURSEHOME_COD', 'TEMPCARHOME_COD')
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
        -- Determine residence status
        oc.residence_type IS NOT NULL AS is_care_home_resident,
        oc.residence_type = 'Nursing Home' AS is_nursing_home_resident,
        oc.is_temporary AS is_temporary_resident,
        oc.residence_type AS residence_type,
        CASE
            WHEN oc.is_temporary THEN 'Temporary'
            ELSE 'Permanent'
        END AS residence_status,
        oc.cluster_ids AS source_cluster_ids,
        o."id" AS observation_lds_id -- Include for potential tie-breaking
    FROM
        "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
    JOIN
        ObservationClusters oc
        ON o."id" = oc.observation_id
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
        mc.cluster_id IN ('CAREHOME_COD', 'NURSEHOME_COD', 'TEMPCARHOME_COD')
        AND oc.residence_type IS NOT NULL -- Only include records with a valid residence type
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pp."person_id"
            -- Order by date first, then by observation ID as a tie-breaker
            ORDER BY o."clinical_effective_date" DESC, o."id" DESC
        ) = 1 -- Get only the latest record per person
)
-- Select only persons with a care home record
SELECT
    lrsp."person_id",
    lrsp."sk_patient_id",
    lrsp."clinical_effective_date" AS latest_residence_date,
    lrsp.concept_id,
    lrsp.concept_code,
    lrsp.term,
    lrsp.is_care_home_resident,
    lrsp.is_nursing_home_resident,
    lrsp.is_temporary_resident,
    lrsp.residence_type,
    lrsp.residence_status,
    lrsp.source_cluster_ids AS SOURCE_CLUSTER_IDS
FROM
    LatestResidenceStatusPerPerson lrsp; 