-- Staging model for olids_terminology.CONCEPT
-- Base layer: base_olids_terminology_concept (terminology data with unquoted identifiers)
-- Description: OLIDS-specific terminology and code mappings

select
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    system,
    code,
    display,
    is_mapped,
    use_count,
    lds_start_date_time
from {{ ref('stable_terminology_concept') }}
