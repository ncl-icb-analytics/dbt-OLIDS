-- Test that age calculations are reasonably consistent with birth dates
-- This should return 0 rows if age calculations are accurate (allowing 1 year tolerance)

SELECT 
    person_id,
    effective_start_date,
    birth_date_approx,
    age as calculated_age,
    FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) as expected_age,
    ABS(age - FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12)) as age_difference
FROM {{ ref('dim_person_demographics_historical') }}
WHERE birth_date_approx IS NOT NULL
    AND ABS(age - FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12)) > 1