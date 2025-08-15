{{
  config(
    materialized='table',
    tags=['households', 'geography', 'spatial'])
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
