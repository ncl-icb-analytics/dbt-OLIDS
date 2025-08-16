-- Test that no overlapping periods exist for the same person in SCD2 table
-- This should return 0 rows if SCD2 temporal logic is correct

SELECT 
    a.person_id,
    a.effective_start_date as period_a_start,
    a.effective_end_date as period_a_end,
    b.effective_start_date as period_b_start,
    b.effective_end_date as period_b_end
FROM {{ ref('dim_person_demographics_historical') }} a
INNER JOIN {{ ref('dim_person_demographics_historical') }} b
    ON a.person_id = b.person_id
    AND a.effective_start_date != b.effective_start_date
WHERE 
    -- Check for overlapping periods using proper temporal logic
    a.effective_start_date < CASE WHEN b.effective_end_date IS NULL THEN CURRENT_DATE + INTERVAL '1 year' ELSE b.effective_end_date END
    AND CASE WHEN a.effective_end_date IS NULL THEN CURRENT_DATE + INTERVAL '1 year' ELSE a.effective_end_date END > b.effective_start_date