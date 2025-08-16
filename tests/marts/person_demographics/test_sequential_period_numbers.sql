-- Test that period_sequence values are sequential starting from 1 for each person
-- This should return 0 rows if period sequencing is correct

WITH expected_sequence AS (
    SELECT 
        person_id,
        effective_start_date,
        period_sequence,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY effective_start_date) as expected_seq
    FROM {{ ref('dim_person_demographics_historical') }}
)
SELECT 
    person_id,
    effective_start_date,
    period_sequence,
    expected_seq,
    'Period sequence mismatch' as error_type
FROM expected_sequence
WHERE period_sequence != expected_seq