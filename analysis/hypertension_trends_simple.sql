-- Clinical Episodes Monthly Table - Usage Examples
-- Shows how simple various analyses become with the monthly table

-- 1. BASIC TIME TRENDS
-- Track hypertension prevalence month by month across the entire population
SELECT 
    analysis_month,
    COUNT(DISTINCT person_id) as total_patients,
    SUM(has_htn) as htn_cases,
    ROUND(100.0 * SUM(has_htn) / COUNT(DISTINCT person_id), 1) as htn_prevalence_pct
FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly
GROUP BY analysis_month
ORDER BY analysis_month;

-- 2. COMORBIDITY ANALYSIS
-- Compare multiple conditions over time and identify patients with both hypertension and diabetes
SELECT 
    analysis_month,
    COUNT(DISTINCT person_id) as total_patients,
    SUM(has_htn) as htn_cases,
    SUM(has_dm) as diabetes_cases,
    SUM(CASE WHEN has_htn = 1 AND has_dm = 1 THEN 1 ELSE 0 END) as htn_diabetes_comorbid,
    ROUND(100.0 * SUM(has_htn) / COUNT(DISTINCT person_id), 1) as htn_prevalence_pct,
    ROUND(100.0 * SUM(has_dm) / COUNT(DISTINCT person_id), 1) as dm_prevalence_pct,
    ROUND(100.0 * SUM(CASE WHEN has_htn = 1 AND has_dm = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT person_id), 1) as comorbid_prevalence_pct,
    ROUND(100.0 * SUM(CASE WHEN has_htn = 1 AND has_dm = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(has_htn), 0), 1) as pct_htn_patients_with_diabetes,
    ROUND(100.0 * SUM(CASE WHEN has_htn = 1 AND has_dm = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(has_dm), 0), 1) as pct_dm_patients_with_htn
FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly
GROUP BY analysis_month
ORDER BY analysis_month;

-- 3. PRACTICE COMPARISON
-- Compare hypertension rates across practices using the latest available month (cleaner for practice comparison)
SELECT 
    practice_name,
    COUNT(DISTINCT person_id) as total_patients,
    SUM(has_htn) as htn_cases,
    ROUND(100.0 * SUM(has_htn) / COUNT(DISTINCT person_id), 1) as htn_prevalence_pct,
    SUM(new_htn) as new_htn_episodes_this_month
FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly
WHERE analysis_month = (SELECT MAX(analysis_month) FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly)
GROUP BY practice_name
HAVING COUNT(DISTINCT person_id) >= 1000  -- Practices with substantial patient numbers
ORDER BY htn_prevalence_pct DESC;

-- 4. DEMOGRAPHICS BREAKDOWN
-- Analyse hypertension prevalence by age and gender for the latest available month
SELECT 
    d.age_band_10y,
    d.sex,
    COUNT(DISTINCT m.person_id) as total_patients,
    SUM(m.has_htn) as htn_cases,
    ROUND(100.0 * SUM(m.has_htn) / COUNT(DISTINCT m.person_id), 1) as htn_prevalence_pct
FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly m
LEFT JOIN data_lab_olids_uat.dbt_dev.dim_person_demographics_historical d
    ON m.person_id = d.person_id 
    AND m.analysis_month >= d.effective_start_date 
    AND (d.effective_end_date IS NULL OR m.analysis_month < d.effective_end_date)
WHERE m.analysis_month = (SELECT MAX(analysis_month) FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly)
    AND d.age_band_10y IS NOT NULL
GROUP BY d.age_band_10y, d.sex
ORDER BY MIN(d.age), d.sex;

-- 5. INCIDENCE BY AGE
-- Track new hypertension episodes (incidence) across different age groups over the latest 12 available months
SELECT 
    d.age_band_10y,
    COUNT(DISTINCT m.person_id) as total_patients_observed,
    SUM(m.new_htn) as new_htn_episodes,
    ROUND(1000.0 * SUM(m.new_htn) / COUNT(DISTINCT m.person_id), 1) as new_htn_per_1000_patients
FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly m
LEFT JOIN data_lab_olids_uat.dbt_dev.dim_person_demographics_historical d
    ON m.person_id = d.person_id 
    AND m.analysis_month >= d.effective_start_date 
    AND (d.effective_end_date IS NULL OR m.analysis_month < d.effective_end_date)
WHERE m.analysis_month >= (
    SELECT DATEADD('month', -12, MAX(analysis_month))
    FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly
)
    AND d.age_band_10y IS NOT NULL
GROUP BY d.age_band_10y
ORDER BY MIN(d.age);

-- 6. MULTI-DIMENSIONAL ANALYSIS
-- Combine practice and demographics to see hypertension rates by practice and age group for latest month
SELECT 
    m.practice_name,
    d.age_band_10y,
    COUNT(DISTINCT m.person_id) as total_patients,
    SUM(m.has_htn) as htn_cases,
    ROUND(100.0 * SUM(m.has_htn) / COUNT(DISTINCT m.person_id), 1) as htn_prevalence_pct
FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly m
LEFT JOIN data_lab_olids_uat.dbt_dev.dim_person_demographics_historical d
    ON m.person_id = d.person_id 
    AND m.analysis_month >= d.effective_start_date 
    AND (d.effective_end_date IS NULL OR m.analysis_month < d.effective_end_date)
WHERE m.analysis_month = (SELECT MAX(analysis_month) FROM data_lab_olids_uat.dbt_dev.fct_person_clinical_episodes_monthly)
    AND d.age_band_10y IS NOT NULL
GROUP BY m.practice_name, d.age_band_10y
HAVING COUNT(DISTINCT m.person_id) >= 50  -- Sufficient sample size
ORDER BY m.practice_name, MIN(d.age);