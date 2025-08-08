{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All eFI measurements (EFI_SCORE and EFI2_SCORE) with correct categorisation per algorithm.
- EFI_SCORE thresholds:
  <0.12 Fit; ≥0.12 and <0.24 Mildly Frail; ≥0.24 and <0.36 Moderately Frail; ≥0.36 Severely Frail
- EFI2_SCORE thresholds:
  <0.09 Fit; 0.09 ≤ score <0.16 Mildly Frail; 0.16 ≤ score <0.24 Moderately Frail; ≥0.24 Severely Frail
*/

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        TRY_CAST(obs.result_value AS FLOAT) AS efi_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value
    FROM ({{ get_observations("'EFI_SCORE', 'EFI2_SCORE'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
),

classified AS (
    SELECT
        bo.*,
        CASE
            WHEN bo.source_cluster_id = 'EFI_SCORE' THEN 'EFI'
            WHEN bo.source_cluster_id = 'EFI2_SCORE' THEN 'EFI2'
            ELSE 'UNKNOWN'
        END AS efi_type,
        CASE
            WHEN bo.source_cluster_id = 'EFI_SCORE' THEN (
                CASE
                    WHEN efi_value < 0.12 THEN 'Fit'
                    WHEN efi_value >= 0.12 AND efi_value < 0.24 THEN 'Mildly Frail'
                    WHEN efi_value >= 0.24 AND efi_value < 0.36 THEN 'Moderately Frail'
                    WHEN efi_value >= 0.36 THEN 'Severely Frail'
                    ELSE 'Unknown'
                END
            )
            WHEN bo.source_cluster_id = 'EFI2_SCORE' THEN (
                CASE
                    WHEN efi_value < 0.09 THEN 'Fit'
                    WHEN efi_value >= 0.09 AND efi_value < 0.16 THEN 'Mildly Frail'
                    WHEN efi_value >= 0.16 AND efi_value < 0.24 THEN 'Moderately Frail'
                    WHEN efi_value >= 0.24 THEN 'Severely Frail'
                    ELSE 'Unknown'
                END
            )
            ELSE 'Unknown'
        END AS efi_category
    FROM base_observations bo
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    efi_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    efi_type,
    efi_category,
    original_result_value
FROM classified
ORDER BY person_id, clinical_effective_date DESC

