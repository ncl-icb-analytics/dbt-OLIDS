/*
LTC LCS Case Finding: CKD_62 - Consecutive High UACR Readings

Purpose:
- Identifies patients with two consecutive UACR readings above 4 (case finding for undiagnosed CKD)

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)
   
2. UACR Criteria:
   - Must have at least 2 UACR readings with values > 0
   - Takes max value per day to handle multiple readings
   - Filters out adjacent day duplicates (same result on consecutive days)
   - Both most recent and previous reading must be > 4
   - Uses 'UINE_ACR' cluster ID

3. Output:
   - Only includes patients who meet all criteria (both readings > 4)
   - Provides latest and previous UACR values and dates
   - Collects all UACR concept codes and displays for traceability

Implementation Notes:
- Materialized as ephemeral to avoid cluttering the database
- Uses LAG functions to identify consecutive readings
- Implements adjacent day filtering logic

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For UACR readings

*/

{{ config(materialized='ephemeral') }}

with base_population as (
    -- Get base population of patients over 17
    -- Base population already excludes those on CKD and Diabetes registers
    select distinct
        person_id,
        age
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age >= 17
),
uacr_readings as (
    -- Get all UACR readings with values > 0
    -- Take max value per day to handle multiple readings
    select
        person_id,
        clinical_effective_date,
        max(cast(result_value as number)) as result_value,
        any_value(mapped_concept_code) as concept_code,
        any_value(mapped_concept_display) as concept_display
    from {{ ref('int_ltc_lcs_ckd_observations') }}
    where cluster_id = 'UACR_TESTING'
        and result_value is not null
        and cast(result_value as number) > 0
    group by 
        person_id,
        clinical_effective_date
),
uacr_with_adjacent_check as (
    -- Check for same results on adjacent days
    select
        *,
                case
            when dateadd(day, 1, clinical_effective_date) = lag(clinical_effective_date) over (partition by person_id order by clinical_effective_date desc)
            and result_value = lag(result_value) over (partition by person_id order by clinical_effective_date desc)
            then 'EXCLUDE'
            else 'INCLUDE'
        end as adjacent_day_check
    from uacr_readings
),
uacr_filtered as (
    -- Remove adjacent day duplicates
    select *
    from uacr_with_adjacent_check
    where adjacent_day_check = 'INCLUDE'
),
uacr_ranked as (
    -- Rank UACR readings by date for each person
    select
        *,
        row_number() over (partition by person_id order by clinical_effective_date desc) as reading_rank
    from uacr_filtered
),
uacr_counts as (
    -- Count readings per person to ensure at least 2
    select
        person_id,
        count(*) as reading_count
    from uacr_filtered
    group by person_id
    having count(*) > 1
),
uacr_with_lags as (
    -- Get the two most recent readings with their lags
    select
        ur.person_id,
        ur.clinical_effective_date as latest_uacr_date,
        lag(ur.clinical_effective_date) over (partition by ur.person_id order by ur.clinical_effective_date desc) as previous_uacr_date,
        ur.result_value as latest_uacr_value,
        lag(ur.result_value) over (partition by ur.person_id order by ur.clinical_effective_date desc) as previous_uacr_value
    from uacr_ranked ur
    join uacr_counts uc using (person_id)
    where ur.reading_rank <= 2
    qualify ur.reading_rank = 1
),
uacr_codes as (
    -- Get all codes and displays for each person
    select
        person_id,
        array_agg(distinct concept_code) within group (order by concept_code) as all_uacr_codes,
        array_agg(distinct concept_display) within group (order by concept_display) as all_uacr_displays
    from uacr_readings
    group by person_id
)
-- Final selection
select
    bp.person_id,
    bp.age,
    case 
        when ceg.latest_uacr_value > 4 and ceg.previous_uacr_value > 4 then true
        else false
    end as has_elevated_uacr,
    ceg.latest_uacr_date,
    ceg.previous_uacr_date,
    ceg.latest_uacr_value,
    ceg.previous_uacr_value,
    codes.all_uacr_codes,
    codes.all_uacr_displays,
    -- Meets criteria flag for mart model
    case 
        when ceg.latest_uacr_value > 4 and ceg.previous_uacr_value > 4 then true
        else false
    end as meets_criteria
from base_population bp
left join uacr_with_lags ceg using (person_id)
left join uacr_codes codes using (person_id)
where ceg.latest_uacr_value > 4 
    and ceg.previous_uacr_value > 4
