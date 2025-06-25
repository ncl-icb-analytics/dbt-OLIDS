{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'carer', 'social_care'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Dimension table providing the latest recorded carer status for persons who have a carer status record. Only includes persons with a record in ISACARER_COD, NOTACARER_COD, or UNPAIDCARER_COD clusters. This table specifically tracks patients who are carers themselves, not patients who have carers.'"
        ]
    )
}}

-- Person Carer Status Dimension Table
-- Holds the latest carer status for persons who have a recorded carer status
-- Only includes persons who have a record in ISACARER_COD, NOTACARER_COD, or UNPAIDCARER_COD clusters
-- Carer types and details are categorised based on code descriptions to maintain flexibility with changing codes

WITH observation_clusters AS (
    -- First, collect all cluster IDs and determine carer status for each observation
    SELECT
        o.observation_id,
        ARRAY_AGG(DISTINCT o.cluster_id) WITHIN GROUP (ORDER BY o.cluster_id) AS cluster_ids,
        -- Pre-calculate carer status based on clusters
        CASE
            WHEN ARRAY_CONTAINS('NOTACARER_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) THEN FALSE
            WHEN ARRAY_CONTAINS('ISACARER_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) OR
                 ARRAY_CONTAINS('UNPAIDCARER_COD'::VARIANT, ARRAY_AGG(DISTINCT o.cluster_id)) THEN TRUE
            ELSE NULL
        END AS is_carer
    FROM (
        {{ get_observations("'ISACARER_COD', 'NOTACARER_COD', 'UNPAIDCARER_COD'") }}
    ) o
    GROUP BY o.observation_id
),

latest_carer_status_per_person AS (
    -- Then get the latest observation per person with all its details
    SELECT
        o.person_id,
        o.sk_patient_id,
        o.clinical_effective_date,
        o.mapped_concept_id AS concept_id,
        o.mapped_concept_code AS concept_code,
        o.mapped_concept_display AS term,
        oc.is_carer,
        -- Determine carer type based on code descriptions
        CASE
            WHEN LOWER(o.mapped_concept_display) LIKE '%primary caregiver%' THEN 'Primary Carer'
            WHEN LOWER(o.mapped_concept_display) LIKE '%informal caregiver%' THEN 'Informal Carer'
            WHEN LOWER(o.mapped_concept_display) LIKE '%unpaid caregiver%' THEN 'Unpaid Carer'
            WHEN LOWER(o.mapped_concept_display) LIKE '%professional%' OR
                 LOWER(o.mapped_concept_display) LIKE '%occupation%' THEN 'Professional Carer'
            WHEN LOWER(o.mapped_concept_display) LIKE '%carer allowance%' THEN 'Carer Receiving Allowance'
            WHEN oc.is_carer THEN 'Other Carer'
            ELSE NULL
        END AS carer_type,
        -- Determine carer details based on code descriptions
        CASE
            -- Health conditions
            WHEN LOWER(o.mapped_concept_display) LIKE '%dementia%' OR
                 LOWER(o.mapped_concept_display) LIKE '%chronic%' OR
                 LOWER(o.mapped_concept_display) LIKE '%disability%' OR
                 LOWER(o.mapped_concept_display) LIKE '%mental%' OR
                 LOWER(o.mapped_concept_display) LIKE '%terminal%' OR
                 LOWER(o.mapped_concept_display) LIKE '%alcohol%' OR
                 LOWER(o.mapped_concept_display) LIKE '%substance%' THEN 'Caring for person with health condition'
            -- Family relationships
            WHEN LOWER(o.mapped_concept_display) LIKE '%father%' OR
                 LOWER(o.mapped_concept_display) LIKE '%mother%' OR
                 LOWER(o.mapped_concept_display) LIKE '%spouse%' OR
                 LOWER(o.mapped_concept_display) LIKE '%husband%' OR
                 LOWER(o.mapped_concept_display) LIKE '%wife%' OR
                 LOWER(o.mapped_concept_display) LIKE '%partner%' OR
                 LOWER(o.mapped_concept_display) LIKE '%relative%' THEN 'Caring for family member'
            -- Other relationships
            WHEN LOWER(o.mapped_concept_display) LIKE '%neighbour%' OR
                 LOWER(o.mapped_concept_display) LIKE '%friend%' THEN 'Caring for non-family member'
            -- Special cases
            WHEN LOWER(o.mapped_concept_display) LIKE '%contingency plan%' THEN 'Has carer contingency plan'
            WHEN LOWER(o.mapped_concept_display) LIKE '%patient themselves providing care%' THEN 'Patient is carer'
            WHEN oc.is_carer THEN 'Caring for other'
            ELSE NULL
        END AS carer_details,
        oc.cluster_ids AS source_cluster_ids,
        o.observation_id AS observation_lds_id -- Include for potential tie-breaking
    FROM (
        {{ get_observations("'ISACARER_COD', 'NOTACARER_COD', 'UNPAIDCARER_COD'") }}
    ) o
    JOIN observation_clusters oc
        ON o.observation_id = oc.observation_id
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY o.person_id
            -- Order by date first, then by observation ID as a tie-breaker
            ORDER BY o.clinical_effective_date DESC, o.observation_id DESC
        ) = 1 -- Get only the latest record per person
)

-- Select only persons who have a carer status record
SELECT
    lcsp.person_id,
    lcsp.sk_patient_id,
    lcsp.clinical_effective_date AS latest_carer_status_date,
    lcsp.concept_id,
    lcsp.concept_code,
    lcsp.term,
    lcsp.is_carer,
    lcsp.carer_type,
    lcsp.carer_details,
    lcsp.source_cluster_ids
FROM latest_carer_status_per_person lcsp
