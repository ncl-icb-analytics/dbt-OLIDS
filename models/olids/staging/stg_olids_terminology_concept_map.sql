-- Staging model for olids_terminology.CONCEPT_MAP
-- Base layer: base_olids_terminology_concept_map (terminology data with unquoted identifiers)
-- Description: OLIDS-specific terminology and code mappings

select
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    concept_map_id,
    source_code_id,
    target_code_id,
    is_primary,
    equivalence,
    lds_start_date_time
from {{ ref('base_olids_terminology_concept_map') }}
