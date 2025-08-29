{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'care_home', 'residence'],
        cluster_by=['person_id'])
}}

-- Person Care Home Dimension Table
-- Holds the latest care home/nursing home status for persons
-- Only includes persons who have a recorded care home, nursing home, or temporary care home status
-- Uses CAREHOME_COD, NURSEHOME_COD, and TEMPCARHOME_COD clusters

WITH observation_clusters AS (
    -- First, collect all cluster IDs and determine residence status for each observation
    SELECT
        o.observation_id,
        ARRAY_AGG(DISTINCT o.cluster_id) WITHIN GROUP (ORDER BY o.cluster_id) AS cluster_ids,
        -- Pre-calculate residence status based on clusters
        CASE
            WHEN ARRAY_CONTAINS('CAREHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) THEN 'Care Home'
            WHEN ARRAY_CONTAINS('NURSEHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) THEN 'Nursing Home'
            WHEN ARRAY_CONTAINS('TEMPCARHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) THEN 'Temporary Care Home'
            ELSE NULL
        END AS residence_type,
        -- Determine if temporary
        ARRAY_CONTAINS('TEMPCARHOME_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) AS is_temporary
    FROM (
        {{ get_observations("'CAREHOME_COD', 'NURSEHOME_COD', 'TEMPCARHOME_COD'") }}
    ) o
    GROUP BY o.observation_id
),

latest_residence_status_per_person AS (
    -- Then get the latest observation per person with all its details
    SELECT
        o.person_id,
        pp.sk_patient_id,
        o.clinical_effective_date,
        o.mapped_concept_id AS concept_id,
        o.mapped_concept_code AS concept_code,
        o.mapped_concept_display AS term,
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
        o.observation_id AS observation_lds_id -- Include for potential tie-breaking
    FROM (
        {{ get_observations("'CAREHOME_COD', 'NURSEHOME_COD', 'TEMPCARHOME_COD'") }}
    ) o
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON o.patient_id = pp.patient_id
    JOIN observation_clusters oc
        ON o.observation_id = oc.observation_id
    WHERE oc.residence_type IS NOT NULL -- Only include records with a valid residence type
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY o.person_id
            -- Order by date first, then by observation ID as a tie-breaker
            ORDER BY o.clinical_effective_date DESC, o.observation_id DESC
        ) = 1 -- Get only the latest record per person
)

-- Select only persons with a care home record
SELECT
    lrsp.person_id,
    lrsp.sk_patient_id,
    lrsp.clinical_effective_date AS latest_residence_date,
    lrsp.concept_id,
    lrsp.concept_code,
    lrsp.term,
    lrsp.is_care_home_resident,
    lrsp.is_nursing_home_resident,
    lrsp.is_temporary_resident,
    lrsp.residence_type,
    lrsp.residence_status,
    lrsp.source_cluster_ids
FROM latest_residence_status_per_person lrsp
