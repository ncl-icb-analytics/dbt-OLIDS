/*
LTC LCS Case Finding: CKD_64 - Specific Conditions Requiring eGFR Monitoring

Purpose:
- Identifies patients with specific conditions (AKI, BPH/Gout, Lithium, Microhaematuria) 
  who have not had eGFR in last 12 months

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)
   
2. Condition Criteria (must have at least one):
   - AKI in last 3 years ('CKD_ACUTE_KIDNEY_INJURY')
   - BPH or Gout ('CKD_BPH_GOUT')  
   - Lithium/Sulfasalazine/Tacrolimus medications in last 6 months
   - Valid microhaematuria (complex logic with UACR and urine tests)

3. eGFR Exclusion:
   - Must NOT have had eGFR test in last 12 months

4. Microhaematuria Validation:
   - Has microhaematuria ('HAEMATURIA')
   - AND either: no negative urine test after haematuria OR has UACR > 30 after haematuria

Implementation Notes:
- Materialized as ephemeral to avoid cluttering the database
- Complex microhaematuria validation logic from legacy
- Mirrors legacy logic exactly

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For clinical events and lab results

*/

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CKD_64 case finding: Patients with conditions and medications suggesting chronic kidney disease'"
) }}

with base_population as (
    -- Get base population of patients over 17
    -- Base population already excludes those on CKD and Diabetes registers
    select distinct
        person_id,
        age
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age >= 17
),
clinical_events as (
    -- Get all relevant clinical events in one go
    select
        person_id,
        clinical_effective_date,
        cluster_id,
        cast(result_value as number) as result_value,
        mapped_concept_code as concept_code,
        mapped_concept_display as concept_display,
        -- Flag each type of event
        case 
            when cluster_id = 'CKD_ACUTE_KIDNEY_INJURY' 
                and clinical_effective_date >= dateadd(month, -36, current_date()) then true
            else false
        end as is_aki,
        case 
            when cluster_id = 'CKD_BPH_GOUT' then true
            else false
        end as is_bph_gout,
        case 
            when cluster_id in ('LITHIUM_MEDICATIONS', 'SULFASALAZINE_MEDICATIONS', 'TACROLIMUS_MEDICATIONS')
                and clinical_effective_date >= dateadd(month, -6, current_date()) then true
            else false
        end as is_lithium,
        case 
            when cluster_id = 'HAEMATURIA' then true
            else false
        end as is_microhaematuria,
        case 
            when cluster_id = 'UACR_TESTING' and result_value > 30 then true
            else false
        end as is_uacr_high,
        case 
            when cluster_id in ('URINE_BLOOD_NEGATIVE', 'PROTEINURIA_FINDINGS') then true
            else false
        end as is_urine_test
    from {{ ref('int_ltc_lcs_ckd_observations') }}
    where cluster_id in (
        'CKD_ACUTE_KIDNEY_INJURY',
        'CKD_BPH_GOUT',
        'LITHIUM_MEDICATIONS',
        'SULFASALAZINE_MEDICATIONS', 
        'TACROLIMUS_MEDICATIONS',
        'HAEMATURIA',
        'UACR_TESTING',
        'URINE_BLOOD_NEGATIVE',
        'PROTEINURIA_FINDINGS'
    )
),
condition_summary as (
    -- Summarise conditions per person
    select
        person_id,
        -- AKI
        max(case when is_aki then clinical_effective_date end) as latest_aki_date,
        boolor_agg(is_aki) as has_acute_kidney_injury,
        -- BPH/Gout
        max(case when is_bph_gout then clinical_effective_date end) as latest_bph_gout_date,
        boolor_agg(is_bph_gout) as has_bph_gout,
        -- Lithium
        max(case when is_lithium then clinical_effective_date end) as latest_lithium_date,
        boolor_agg(is_lithium) as has_lithium_medication,
        -- Microhaematuria
        max(case when is_microhaematuria then clinical_effective_date end) as latest_microhaematuria_date,
        boolor_agg(is_microhaematuria) as has_microhaematuria,
        -- UACR
        max(case when is_uacr_high then clinical_effective_date end) as latest_uacr_date,
        max(case when is_uacr_high then result_value end) as latest_uacr_value,
        -- Urine tests
        max(case when is_urine_test then clinical_effective_date end) as latest_urine_test_date,
        -- Codes and displays
        array_agg(distinct concept_code) within group (order by concept_code) as all_condition_codes,
        array_agg(distinct concept_display) within group (order by concept_display) as all_condition_displays
    from clinical_events
    group by person_id
),
egfr_in_last_year as (
    -- Get patients with eGFR in last 12 months to exclude
    select distinct person_id
    from {{ ref('int_ltc_lcs_ckd_observations') }}
    where cluster_id = 'EGFR_TESTING'
        and result_value is not null
        and cast(result_value as number) > 0
        and clinical_effective_date >= dateadd(month, -12, current_date())
),
microhaematuria_with_conditions as (
    -- Get patients with microhaematuria and specific conditions
    select
        cs.*,
        case 
            when cs.latest_urine_test_date is null 
                or cs.latest_microhaematuria_date > cs.latest_urine_test_date
                or (cs.latest_uacr_date is not null and cs.latest_uacr_date >= cs.latest_microhaematuria_date)
            then true
            else false
        end as has_valid_microhaematuria
    from condition_summary cs
)
-- Final selection
select
    bp.person_id,
    bp.age,
    coalesce(mh.has_acute_kidney_injury, false) as has_acute_kidney_injury,
    coalesce(mh.has_bph_gout, false) as has_bph_gout,
    coalesce(mh.has_lithium_medication, false) as has_lithium_medication,
    coalesce(mh.has_valid_microhaematuria, false) as has_microhaematuria,
    mh.latest_aki_date,
    mh.latest_bph_gout_date,
    mh.latest_lithium_date,
    mh.latest_microhaematuria_date,
    mh.latest_uacr_date,
    mh.latest_uacr_value,
    mh.all_condition_codes,
    mh.all_condition_displays,
    -- Meets criteria flag for mart model
    case 
        when (coalesce(mh.has_acute_kidney_injury, false)
            or coalesce(mh.has_bph_gout, false)
            or coalesce(mh.has_lithium_medication, false)
            or coalesce(mh.has_valid_microhaematuria, false))
        then true
        else false
    end as meets_criteria
from base_population bp
left join microhaematuria_with_conditions mh using (person_id)
where not exists (
    select 1 from egfr_in_last_year egfr 
    where egfr.person_id = bp.person_id
)
and (
    coalesce(mh.has_acute_kidney_injury, false)
    or coalesce(mh.has_bph_gout, false)
    or coalesce(mh.has_lithium_medication, false)
    or coalesce(mh.has_valid_microhaematuria, false)
)
