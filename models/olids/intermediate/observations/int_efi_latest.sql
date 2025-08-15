{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest eFI per person, preferring the most recent observation regardless of EFI algorithm.
Exposes both EFI and EFI2 latest scores (if available) and the preferred latest score/type.
*/

WITH all_efi AS (
    SELECT * FROM {{ ref('int_efi_all') }}
),

latest_overall AS (
    SELECT
        person_id,
        observation_id,
        clinical_effective_date,
        efi_value,
        efi_type,
        efi_category,
        ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY clinical_effective_date DESC, observation_id DESC
        ) AS rn
    FROM all_efi
),

latest_efi AS (
    SELECT
        person_id,
        efi_value AS latest_efi_score,
        efi_category AS latest_efi_category
    FROM all_efi
    WHERE efi_type = 'EFI'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY clinical_effective_date DESC, observation_id DESC
    ) = 1
),

latest_efi2 AS (
    SELECT
        person_id,
        efi_value AS latest_efi2_score,
        efi_category AS latest_efi2_category
    FROM all_efi
    WHERE efi_type = 'EFI2'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY clinical_effective_date DESC, observation_id DESC
    ) = 1
)

SELECT
    lo.person_id,
    lo.observation_id AS latest_observation_id,
    lo.clinical_effective_date AS latest_efi_date,
    lo.efi_value AS latest_efi_score_preferred,
    lo.efi_type AS latest_efi_type_preferred,
    lo.efi_category AS latest_efi_category_preferred,
    le.latest_efi_score,
    le.latest_efi_category,
    le2.latest_efi2_score,
    le2.latest_efi2_category
FROM latest_overall lo
LEFT JOIN latest_efi le
    ON lo.person_id = le.person_id
LEFT JOIN latest_efi2 le2
    ON lo.person_id = le2.person_id
WHERE lo.rn = 1

