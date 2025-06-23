-- Investigation of accepted values failures
-- Run these queries to understand what values are causing failures

-- 1. Smoking status failures
SELECT 'smoking_status' as test_type, smoking_status, COUNT(*) as count
FROM DATA_LAB_NCL_TRAINING_TEMP.DBT_DEV_test_audit.accepted_values_fct_person_smoking_status_smoking_status__Current_Smoker__Ex_Smoker__Never_Smoked__Unknown
GROUP BY smoking_status
ORDER BY count DESC;

-- 2. Diabetes foot check type failures  
SELECT 'diabetes_foot_check' as test_type, diabetes_type, COUNT(*) as count
FROM DATA_LAB_NCL_TRAINING_TEMP.DBT_DEV_test_audit.accepted_values_fct_person_diabetes_foot_check_diabetes_type__Type_1__Type_2__Other__Unspecified
GROUP BY diabetes_type
ORDER BY count DESC;

-- 3. BMI latest category failures
SELECT 'bmi_latest_category' as test_type, bmi_category, COUNT(*) as count  
FROM DATA_LAB_NCL_TRAINING_TEMP.DBT_DEV_test_audit.accepted_values_int_bmi_latest_bmi_category__Underweight__Normal_Weight__Overweight__Obese_Class_I__Obese_Class_II__Obese_Class_III
GROUP BY bmi_category
ORDER BY count DESC;

-- 4. BMI all category failures
SELECT 'bmi_all_category' as test_type, bmi_category, COUNT(*) as count
FROM DATA_LAB_NCL_TRAINING_TEMP.DBT_DEV_test_audit.accepted_values_int_bmi_all_bmi_category__Invalid__Underweight__Normal_Weight__Overweight__Obese_Class_I__Obese_Class_II__Obese_Class_III__Unknown  
GROUP BY bmi_category
ORDER BY count DESC;

-- 5. Learning disability cluster failures
SELECT 'learning_disability_cluster' as test_type, source_cluster_id, COUNT(*) as count
FROM DATA_LAB_NCL_TRAINING_TEMP.DBT_DEV_test_audit.accepted_values_int_learning_disability_diagnoses_all_source_cluster_id__LD_DIAGNOSIS_COD
GROUP BY source_cluster_id
ORDER BY count DESC;

-- 6. Smoking status latest failures
SELECT 'smoking_status_latest' as test_type, smoking_status, COUNT(*) as count
FROM DATA_LAB_NCL_TRAINING_TEMP.DBT_DEV_test_audit.accepted_values_int_smoking_status_latest_smoking_status__Current_Smoker__Ex_Smoker__Never_Smoked__Unknown
GROUP BY smoking_status
ORDER BY count DESC;

-- 7. Valproate product type failures
SELECT 'valproate_product_type' as test_type, valproate_product_type, COUNT(*) as count
FROM DATA_LAB_NCL_TRAINING_TEMP.DBT_DEV_test_audit.accepted_values_int_valproate__c47d4953fcd5a3b8dc9f8564ddf59f1d
GROUP BY valproate_product_type
ORDER BY count DESC; 