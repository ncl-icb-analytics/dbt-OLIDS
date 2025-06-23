/*
LTC LCS Case Finding: CKD_63 - Elevated UACR (Single Reading)

Purpose:
- Identifies patients with latest UACR reading above 70 (case finding for undiagnosed CKD)
- Excludes patients already captured in CKD_62 (consecutive high readings)

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)
   - Excludes patients already in CKD_62
   
2. UACR Criteria:
   - Latest UACR reading must be > 70
   - Takes max value per day to handle multiple readings
   - Uses 'UACR_TESTING' cluster ID

3. Output:
   - Only includes patients who meet all criteria (latest reading > 70)
   - Provides latest UACR value and date
   - Collects all UACR concept codes and displays for traceability

Implementation Notes:
- Materialized as ephemeral to avoid cluttering the database
- Excludes patients from CKD_62 to avoid double counting

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For UACR readings
- dim_prog_ltc_lcs_cf_ckd_62: To exclude patients with consecutive high readings

*/

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CKD_63 case finding: Patients with elevated UACR (> 70) indicating significant kidney damage'"
) }}

with base_population as (
    -- Get base population of patients over 17
    -- Excludes those on CKD and Diabetes registers, and those in CKD_62
    select distinct
        bp.person_id,
        bp.age
    from {{ ref('int_ltc_lcs_cf_base_population') }} bp
    left join {{ ref('dim_ltc_lcs_cf_ckd_62') }} ckd62 using (person_id)
    where bp.age >= 17
        and ckd62.person_id is null -- Exclude patients in CKD_62
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
latest_uacr as (
    -- Get the most recent UACR reading for each person
    select
        ur.person_id,
        ur.clinical_effective_date as latest_uacr_date,
        ur.result_value as latest_uacr_value
    from uacr_readings ur
    qualify row_number() over (partition by ur.person_id order by ur.clinical_effective_date desc) = 1
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
        when ceg.latest_uacr_value > 70 then true
        else false
    end as has_elevated_uacr,
    ceg.latest_uacr_date,
    ceg.latest_uacr_value,
    codes.all_uacr_codes,
    codes.all_uacr_displays,
    -- Meets criteria flag for mart model
    case 
        when ceg.latest_uacr_value > 70 then true
        else false
    end as meets_criteria
from base_population bp
left join latest_uacr ceg using (person_id)
left join uacr_codes codes using (person_id)
where ceg.latest_uacr_value > 70
