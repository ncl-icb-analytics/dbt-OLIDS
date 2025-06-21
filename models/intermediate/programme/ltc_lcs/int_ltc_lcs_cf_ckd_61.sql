/*
LTC LCS Case Finding: CKD_61 - Consecutive Low eGFR Readings

Purpose:
- Identifies patients with two consecutive eGFR readings below 60 (case finding for undiagnosed CKD)

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)
   
2. eGFR Criteria:
   - Must have at least 2 eGFR readings with values > 0
   - Both most recent and previous reading must be < 60
   - Uses 'EGFR_COD_LCS' cluster ID for eGFR testing

3. Output:
   - Only includes patients who meet all criteria (both readings < 60)
   - Provides latest and previous eGFR values and dates
   - Collects all eGFR concept codes and displays for traceability

Implementation Notes:
- Materialized as ephemeral to avoid cluttering the database
- Uses LAG functions to identify consecutive readings

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For eGFR readings

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
egfr_readings as (
    -- Get all eGFR readings with values > 0
    select
        person_id,
        clinical_effective_date,
        cast(result_value as number) as result_value,
        mapped_concept_code as concept_code,
        mapped_concept_display as concept_display
    from {{ ref('int_ltc_lcs_ckd_observations') }}
    where cluster_id = 'EGFR_TESTING'
        and result_value is not null
        and cast(result_value as number) > 0
),
egfr_ranked as (
    -- Rank eGFR readings by date for each person
    select
        *,
        row_number() over (partition by person_id order by clinical_effective_date desc) as reading_rank
    from egfr_readings
),
egfr_counts as (
    -- Count readings per person to ensure at least 2
    select
        person_id,
        count(*) as reading_count
    from egfr_readings
    group by person_id
    having count(*) > 1
),
egfr_with_lags as (
    -- Get the two most recent readings with their lags
    select
        er.person_id,
        er.clinical_effective_date as latest_egfr_date,
        lag(er.clinical_effective_date) over (partition by er.person_id order by er.clinical_effective_date desc) as previous_egfr_date,
        er.result_value as latest_egfr_value,
        lag(er.result_value) over (partition by er.person_id order by er.clinical_effective_date desc) as previous_egfr_value
    from egfr_ranked er
    join egfr_counts ec using (person_id)
    where er.reading_rank <= 2
    qualify er.reading_rank = 1
),
egfr_codes as (
    -- Get all codes and displays for each person
    select
        person_id,
        array_agg(distinct concept_code) within group (order by concept_code) as all_egfr_codes,
        array_agg(distinct concept_display) within group (order by concept_display) as all_egfr_displays
    from egfr_readings
    group by person_id
)
-- Final selection
select
    bp.person_id,
    bp.age,
    case 
        when ceg.latest_egfr_value < 60 and ceg.previous_egfr_value < 60 then true
        else false
    end as has_ckd,
    ceg.latest_egfr_date,
    ceg.previous_egfr_date,
    ceg.latest_egfr_value,
    ceg.previous_egfr_value,
    codes.all_egfr_codes,
    codes.all_egfr_displays,
    -- Meets criteria flag for mart model
    case 
        when ceg.latest_egfr_value < 60 and ceg.previous_egfr_value < 60 then true
        else false
    end as meets_criteria
from base_population bp
left join egfr_with_lags ceg using (person_id)
left join egfr_codes codes using (person_id)
where ceg.latest_egfr_value < 60 
    and ceg.previous_egfr_value < 60
