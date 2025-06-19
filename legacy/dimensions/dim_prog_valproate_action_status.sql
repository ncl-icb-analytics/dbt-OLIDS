/*
VALPROATE ACTION STATUS BUSINESS LOGIC DOCUMENTATION
=====================================================

This table implements the clinical decision logic for Valproate safety monitoring,
determining what action should be taken for each patient based on their clinical status.

ACTION PRIORITY ORDER (1 = Highest Priority, 9 = Lowest Priority):
------------------------------------------------------------------

1. PREGNANCY DETECTED (Action: "Review or refer")
   - Patient is currently pregnant
   - URGENT: Immediate review required due to teratogenic risks

2. NO PPP STATUS + NO ARAF + LEARNING DISABILITY (Action: "Review or refer") 
   - Patient has no Pregnancy Prevention Programme record OR 
   - Patient has no Annual Risk Acknowledgement Form AND has learning disability
   - HIGH PRIORITY: Vulnerable population needs immediate attention

3. NO PPP STATUS + NO ARAF + AGE 13-60 (Action: "Review or refer")
   - Patient has no PPP record OR no ARAF and is in high-risk reproductive age
   - HIGH PRIORITY: Prime reproductive age without safety measures

4. NO PPP STATUS + NO ARAF + AGE 7-12 (Action: "Review or refer")
   - Patient has no PPP record OR no ARAF and is approaching reproductive age
   - MEDIUM-HIGH PRIORITY: Early intervention needed

5. PPP NON-ENROLLED STATUS (Action: "Keep under review")
   - Patient has PPP status of: discontinued, not needed, or declined
   - MEDIUM PRIORITY: Regular monitoring sufficient

6. PPP ENROLLED + ARAF CURRENT + LEARNING DISABILITY + AGE 7-60 (Action: "Consider expiry of ARAF")
   - All safety measures in place but vulnerable population
   - MEDIUM-LOW PRIORITY: Check if ARAF needs renewal

7. PPP ENROLLED + ARAF CURRENT + AGE 7-12 (Action: "Consider expiry of ARAF")
   - Safety measures in place, early reproductive age
   - MEDIUM-LOW PRIORITY: Monitor ARAF expiry

8. PPP ENROLLED + ARAF CURRENT + AGE 13-60 (Action: "Consider expiry of ARAF")
   - Safety measures in place, prime reproductive age
   - LOW-MEDIUM PRIORITY: Routine ARAF expiry monitoring

9. LOW RISK PATIENTS (Action: "No action required")
   - Age 0-6 OR permanent absence of pregnancy risk
   - LOWEST PRIORITY: Minimal risk, routine monitoring only

ADDITIONAL CLASSIFICATIONS:
--------------------------
- Risk of Pregnancy: Low Risk (0-6 or permanent absence), Medium Risk (7-12), High Risk (13+)
- Additional Findings: Pregnancy and/or Learning Disability status
- Condition Group: Neurology and/or Psychiatry conditions (indicates why patient is on Valproate)

DATA SOURCES:
------------
- DIM_PROG_VALPROATE_DB_SCOPE: Core patient cohort (non-males, age 0-55, recent Valproate orders)
- DIM_PROG_VALPROATE_PPP_STATUS: Pregnancy Prevention Programme status
- DIM_PROG_VALPROATE_ARAF: Annual Risk Acknowledgement Form completion
- INTERMEDIATE_PERM_ABSENCE_PREG_RISK: Permanent pregnancy risk absence records
- FCT_PERSON_DX_LD: Learning disability diagnoses
- FCT_PERSON_PREGNANT: Current pregnancy status
- DIM_PROG_VALPROATE_NEUROLOGY: Neurology conditions
- DIM_PROG_VALPROATE_PSYCHIATRY: Psychiatry conditions
*/

CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_ACTION_STATUS (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for the person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    AGE NUMBER COMMENT 'Age of the person at time of calculation',
    
    -- PPP Status
    HAS_PPP_STATUS BOOLEAN COMMENT 'TRUE if person has any PPP record in the system',
    IS_PPP_ENROLLED BOOLEAN COMMENT 'TRUE if enrolled in PPP, FALSE otherwise',
    IS_PPP_NON_ENROLLED BOOLEAN COMMENT 'TRUE if PPP status is discontinued/not needed/declined',
    PPP_STATUS_DESCRIPTION VARCHAR COMMENT 'Description of PPP status',
    
    -- ARAF Status  
    HAS_ARAF_EVENT BOOLEAN COMMENT 'TRUE if person has any ARAF record in the system',
    HAS_CURRENT_ARAF BOOLEAN COMMENT 'TRUE if person has ARAF meeting lookback requirements',
    
    -- Risk Assessment Flags
    HAS_PERMANENT_ABSENCE_PREGNANCY_RISK BOOLEAN COMMENT 'TRUE if permanent absence of pregnancy risk recorded',
    HAS_LEARNING_DISABILITY BOOLEAN COMMENT 'TRUE if learning disability recorded',
    HAS_PREGNANCY BOOLEAN COMMENT 'TRUE if currently pregnant',
    
    -- Condition Flags
    HAS_NEUROLOGY BOOLEAN COMMENT 'TRUE if person has neurology-related conditions',
    HAS_PSYCHIATRY BOOLEAN COMMENT 'TRUE if person has psychiatry-related conditions',
    
    -- Risk Categories
    RISK_OF_PREGNANCY VARCHAR COMMENT 'Risk category: Low Risk, Medium Risk, High Risk',
    
    -- Action Determination
    ACTION VARCHAR COMMENT 'Recommended action based on business rules',
    ACTION_ORDER NUMBER COMMENT 'Numeric priority order for action (1=highest priority)',
    
    -- Additional Groupings
    ADDITIONAL_FINDINGS VARCHAR COMMENT 'Summary of pregnancy and learning disability findings',
    CONDITION_GROUP VARCHAR COMMENT 'Summary of neurology and psychiatry condition groups',
    
    -- Processing metadata
    CALCULATION_DATE DATE COMMENT 'Date when this calculation was performed'
)
COMMENT = 'Dimension table implementing Valproate safety monitoring clinical decision logic.

Calculates recommended actions and priority order (1-9) based on:
  - Pregnancy status
  - PPP enrollment  
  - ARAF completion
  - Age and risk factors

Action priorities:
  1 = Pregnancy (urgent)
  2-4 = Missing safety measures by age/vulnerability
  5 = PPP non-enrolled (monitor)
  6-8 = Safety measures present (check ARAF expiry)
  9 = Low risk (no action)'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePatientCohort AS (
    -- Start with the core Valproate patient cohort
    SELECT
        vscope.PERSON_ID,
        vscope.SK_PATIENT_ID,
        vscope.AGE
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_DB_SCOPE vscope
),
PPPStatusData AS (
    -- Get PPP status from dedicated dimension table
    SELECT
        PERSON_ID,
        TRUE AS HAS_PPP_STATUS,
        IS_CURRENTLY_PPP_ENROLLED AS IS_PPP_ENROLLED,
        IS_PPP_NON_ENROLLED,
        CURRENT_PPP_STATUS_DESCRIPTION AS PPP_STATUS_DESCRIPTION
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_PPP_STATUS
),
ARAFStatusData AS (
    -- Get ARAF status for each person
    SELECT
        PERSON_ID,
        TRUE AS HAS_ARAF_EVENT,
        HAS_SPECIFIC_ARAF_FORM_MEETING_LOOKBACK AS HAS_CURRENT_ARAF
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_ARAF
),
PregnancyRiskData AS (
    -- Get permanent absence of pregnancy risk
    SELECT
        PERSON_ID,
        TRUE AS HAS_PERMANENT_ABSENCE_PREGNANCY_RISK
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_PERM_ABSENCE_PREG_RISK
),
LearningDisabilityData AS (
    -- Get learning disability status - assuming there's a learning disability dimension table
    -- (This may need adjustment based on actual table structure)
    SELECT
        dxld.PERSON_ID,
        TRUE AS HAS_LEARNING_DISABILITY
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_LD dxld
    WHERE
        dxld.IS_ON_LD_REGISTER = TRUE
),
PregnancyData AS (
    -- Get current pregnancy status
    SELECT
        preg.PERSON_ID,
        TRUE AS HAS_PREGNANCY
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_PREGNANT preg
    WHERE
        preg.IS_CURRENTLY_PREGNANT = TRUE
),
NeurologyData AS (
    -- Get neurology condition status
    SELECT
        PERSON_ID,
        TRUE AS HAS_NEUROLOGY
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_NEUROLOGY
),
PsychiatryData AS (
    -- Get psychiatry condition status
    SELECT
        PERSON_ID,
        TRUE AS HAS_PSYCHIATRY
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_PSYCHIATRY
),
ConsolidatedData AS (
    SELECT
        bpc.PERSON_ID,
        bpc.SK_PATIENT_ID,
        bpc.AGE,
        
        -- PPP Status
        COALESCE(ppp.HAS_PPP_STATUS, FALSE) AS HAS_PPP_STATUS,
        COALESCE(ppp.IS_PPP_ENROLLED, FALSE) AS IS_PPP_ENROLLED,
        COALESCE(ppp.IS_PPP_NON_ENROLLED, FALSE) AS IS_PPP_NON_ENROLLED,
        COALESCE(ppp.PPP_STATUS_DESCRIPTION, 'No - No entry found') AS PPP_STATUS_DESCRIPTION,
        
        -- ARAF Status
        COALESCE(araf.HAS_ARAF_EVENT, FALSE) AS HAS_ARAF_EVENT,
        COALESCE(araf.HAS_CURRENT_ARAF, FALSE) AS HAS_CURRENT_ARAF,
        
        -- Risk Factors
        COALESCE(prisk.HAS_PERMANENT_ABSENCE_PREGNANCY_RISK, FALSE) AS HAS_PERMANENT_ABSENCE_PREGNANCY_RISK,
        COALESCE(ld.HAS_LEARNING_DISABILITY, FALSE) AS HAS_LEARNING_DISABILITY,
        COALESCE(preg.HAS_PREGNANCY, FALSE) AS HAS_PREGNANCY,
        
        -- Condition Groups
        COALESCE(neuro.HAS_NEUROLOGY, FALSE) AS HAS_NEUROLOGY,
        COALESCE(psych.HAS_PSYCHIATRY, FALSE) AS HAS_PSYCHIATRY
        
    FROM BasePatientCohort bpc
    LEFT JOIN PPPStatusData ppp ON bpc.PERSON_ID = ppp.PERSON_ID
    LEFT JOIN ARAFStatusData araf ON bpc.PERSON_ID = araf.PERSON_ID
    LEFT JOIN PregnancyRiskData prisk ON bpc.PERSON_ID = prisk.PERSON_ID
    LEFT JOIN LearningDisabilityData ld ON bpc.PERSON_ID = ld.PERSON_ID
    LEFT JOIN PregnancyData preg ON bpc.PERSON_ID = preg.PERSON_ID
    LEFT JOIN NeurologyData neuro ON bpc.PERSON_ID = neuro.PERSON_ID
    LEFT JOIN PsychiatryData psych ON bpc.PERSON_ID = psych.PERSON_ID
)
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    
    -- Status flags
    HAS_PPP_STATUS,
    IS_PPP_ENROLLED,
    IS_PPP_NON_ENROLLED,
    PPP_STATUS_DESCRIPTION,
    HAS_ARAF_EVENT,
    HAS_CURRENT_ARAF,
    HAS_PERMANENT_ABSENCE_PREGNANCY_RISK,
    HAS_LEARNING_DISABILITY,
    HAS_PREGNANCY,
    HAS_NEUROLOGY,
    HAS_PSYCHIATRY,
    
    -- Risk of pregnancy classification
    CASE 
        WHEN AGE BETWEEN 0 AND 6 OR HAS_PERMANENT_ABSENCE_PREGNANCY_RISK = TRUE THEN 'Low Risk' 
        WHEN AGE BETWEEN 7 AND 12 THEN 'Medium Risk' 
        ELSE 'High Risk' 
    END AS RISK_OF_PREGNANCY,
    
    -- Action determination matching original Vertica logic exactly
    CASE 
        WHEN HAS_PREGNANCY = TRUE THEN 'Review or refer'
        WHEN AGE BETWEEN 0 AND 6 OR HAS_PERMANENT_ABSENCE_PREGNANCY_RISK = TRUE THEN 'No action required'
        WHEN IS_PPP_NON_ENROLLED = TRUE THEN 'Keep under review'
        WHEN HAS_PPP_STATUS = FALSE OR (HAS_CURRENT_ARAF = FALSE AND HAS_LEARNING_DISABILITY = TRUE) THEN 'Review or refer'
        WHEN HAS_PPP_STATUS = FALSE OR (HAS_CURRENT_ARAF = FALSE AND AGE BETWEEN 13 AND 60) THEN 'Review or refer'
        WHEN HAS_PPP_STATUS = FALSE OR (HAS_CURRENT_ARAF = FALSE AND AGE BETWEEN 7 AND 12) THEN 'Review or refer'
        WHEN IS_PPP_ENROLLED = TRUE AND HAS_CURRENT_ARAF = TRUE AND AGE BETWEEN 7 AND 60 AND HAS_LEARNING_DISABILITY = TRUE THEN 'Consider expiry of ARAF'
        WHEN IS_PPP_ENROLLED = TRUE AND HAS_CURRENT_ARAF = TRUE AND AGE BETWEEN 7 AND 12 THEN 'Consider expiry of ARAF'
        WHEN IS_PPP_ENROLLED = TRUE AND HAS_CURRENT_ARAF = TRUE AND AGE BETWEEN 13 AND 60 THEN 'Consider expiry of ARAF'
        ELSE 'Review or refer'
    END AS ACTION,
    
    -- Action priority order matching original Vertica logic exactly
    CASE 
        WHEN HAS_PREGNANCY = TRUE THEN 1
        WHEN AGE BETWEEN 0 AND 6 OR HAS_PERMANENT_ABSENCE_PREGNANCY_RISK = TRUE THEN 9
        WHEN IS_PPP_NON_ENROLLED = TRUE THEN 5
        WHEN HAS_PPP_STATUS = FALSE OR (HAS_CURRENT_ARAF = FALSE AND HAS_LEARNING_DISABILITY = TRUE) THEN 2
        WHEN HAS_PPP_STATUS = FALSE OR (HAS_CURRENT_ARAF = FALSE AND AGE BETWEEN 13 AND 60) THEN 3
        WHEN HAS_PPP_STATUS = FALSE OR (HAS_CURRENT_ARAF = FALSE AND AGE BETWEEN 7 AND 12) THEN 4
        WHEN IS_PPP_ENROLLED = TRUE AND HAS_CURRENT_ARAF = TRUE AND AGE BETWEEN 7 AND 60 AND HAS_LEARNING_DISABILITY = TRUE THEN 6
        WHEN IS_PPP_ENROLLED = TRUE AND HAS_CURRENT_ARAF = TRUE AND AGE BETWEEN 7 AND 12 THEN 7
        WHEN IS_PPP_ENROLLED = TRUE AND HAS_CURRENT_ARAF = TRUE AND AGE BETWEEN 13 AND 60 THEN 8
        ELSE NULL
    END AS ACTION_ORDER,
    
    -- Additional findings grouping (matching Vertica logic)
    CASE 
        WHEN HAS_PREGNANCY = FALSE AND HAS_LEARNING_DISABILITY = FALSE THEN ''
        WHEN HAS_PREGNANCY = TRUE AND HAS_LEARNING_DISABILITY = TRUE THEN 'Pregnancy, Learning Disability'
        WHEN HAS_PREGNANCY = TRUE THEN 'Pregnancy'
        WHEN HAS_LEARNING_DISABILITY = TRUE THEN 'Learning Disability' 
        ELSE ''
    END AS ADDITIONAL_FINDINGS,
    
    -- Condition group logic (matching Vertica logic)
    CASE 
        WHEN HAS_NEUROLOGY = FALSE AND HAS_PSYCHIATRY = FALSE THEN ''
        WHEN HAS_NEUROLOGY = TRUE AND HAS_PSYCHIATRY = TRUE THEN 'Neurology, Psychiatry'
        WHEN HAS_NEUROLOGY = TRUE THEN 'Neurology'
        WHEN HAS_PSYCHIATRY = TRUE THEN 'Psychiatry'
        ELSE ''
    END AS CONDITION_GROUP,
    
    CURRENT_DATE() AS CALCULATION_DATE
    
FROM ConsolidatedData; 