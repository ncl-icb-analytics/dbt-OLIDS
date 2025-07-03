{{
  config(
    materialized='table',
    tags=['households', 'geography', 'spatial'],
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: Households Dimension - Geographic and dwelling information for patient household analysis.

Business Purpose:
• Support population health analytics by analysing household-level health outcomes
• Enable social determinants of health reporting using dwelling location and deprivation data
• Support business intelligence teams in understanding patient geographic distribution
• Provide foundation for household-based care planning and resource allocation

Data Granularity:
• One row per unique household (UPRN-based dwelling)
• Includes all dwellings that have had registered patients
• Covers both currently active and historically active households

Key Features:
• Geographic location data including LSOA codes and deprivation indices
• Dwelling activity status and occupancy timeline
• Household-level metadata for population health analysis
• Supports spatial analysis and geographic reporting requirements'"
    ]
  )
}}

with household_base as (
  select
    uprn_hash,

    -- Geographic context (from any resident's address)
    max(lsoa_code_21) as lsoa_code_21,
    max(lsoa_name_21) as lsoa_name_21,
    max(imd_decile_19) as imd_decile_19,
    max(imd_quintile_19) as imd_quintile_19,

    -- Dwelling metadata
    min(registration_start_date) as first_known_occupation_date,
    max(registration_start_date) as last_known_activity_date

  from {{ ref('dim_person_demographics') }}
  where uprn_hash is not null
  group by uprn_hash
),

dwelling_classification as (
  select
    *,
    -- Calculate dwelling age in years
    datediff('year', first_known_occupation_date, current_date()) as years_since_first_occupation,

    -- Classify dwelling activity
    case
      when last_known_activity_date >= dateadd('year', -1, current_date()) then 'Recently active'
      when last_known_activity_date >= dateadd('year', -5, current_date()) then 'Previously active'
      else 'Historically active only'
    end as dwelling_activity_status

  from household_base
)

select
  {{ dbt_utils.generate_surrogate_key(['uprn_hash']) }} as household_id,
  uprn_hash,

  -- Dwelling activity and age
  dwelling_activity_status,
  years_since_first_occupation,
  first_known_occupation_date,
  last_known_activity_date,

  -- Geographic context (dwelling location)
  lsoa_code_21,
  lsoa_name_21,
  imd_decile_19,
  imd_quintile_19,

  -- Metadata
  current_timestamp() as created_at

from dwelling_classification
