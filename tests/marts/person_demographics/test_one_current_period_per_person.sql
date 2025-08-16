-- Test that each person has exactly one current period (effective_end_date IS NULL)
-- This should return 0 rows if SCD2 logic is correct

SELECT 
    person_id,
    COUNT(*) as current_periods,
    LISTAGG(effective_start_date, ', ') as current_period_starts
FROM {{ ref('dim_person_demographics_historical') }}
WHERE effective_end_date IS NULL
GROUP BY person_id
HAVING COUNT(*) != 1