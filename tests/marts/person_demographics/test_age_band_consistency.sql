-- Test that age bands are consistent with calculated age
-- This should return 0 rows if age band logic is correct

SELECT 
    person_id,
    effective_start_date,
    age,
    age_band_5y,
    age_band_10y,
    age_band_nhs,
    'Age band inconsistency' as issue_type
FROM {{ ref('dim_person_age_historical') }}
WHERE 
    -- Check 5-year age band consistency
    (age IS NOT NULL AND age >= 0 AND age < 100 
     AND age_band_5y != (FLOOR(age / 5) * 5)::VARCHAR || '-' || (FLOOR(age / 5) * 5 + 4)::VARCHAR)
    
    OR
    
    -- Check 10-year age band consistency  
    (age IS NOT NULL AND age >= 0 AND age < 100
     AND age_band_10y != (FLOOR(age / 10) * 10)::VARCHAR || '-' || (FLOOR(age / 10) * 10 + 9)::VARCHAR)
    
    OR
    
    -- Check that 100+ ages have correct band
    (age >= 100 AND (age_band_5y != '100+' OR age_band_10y != '100+'))