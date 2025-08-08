{{ config(
    materialized='table',
    tags=['intermediate', 'lookup', 'observation'],
    cluster_by=['source_code_id']
) }}

-- Mapped concepts lookup: codes → concept → source_code_id (for early filtering)

SELECT DISTINCT
    cm.source_code_id,
    c.id    AS mapped_concept_id,
    c.code  AS mapped_concept_code,
    c.display AS mapped_concept_display,
    cc.cluster_id,
    cc.cluster_description,
    cc.code_description,
    cc.source
FROM {{ ref('stg_codesets_combined_codesets') }} cc
JOIN {{ ref('stg_olids_term_concept') }} c
  ON c.code = cc.code
JOIN {{ ref('stg_olids_term_concept_map') }} cm
  ON cm.target_code_id = c.id

