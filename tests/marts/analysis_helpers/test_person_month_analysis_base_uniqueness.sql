-- Test that person_id + analysis_month combination is unique
-- This is critical for incremental merge strategy

WITH duplicates AS (
    SELECT 
        person_id,
        analysis_month,
        COUNT(*) as record_count
    FROM {{ ref('person_month_analysis_base') }}
    GROUP BY person_id, analysis_month
    HAVING COUNT(*) > 1
)

SELECT 
    person_id,
    analysis_month,
    record_count,
    'Duplicate person-month record' as issue_type
FROM duplicates