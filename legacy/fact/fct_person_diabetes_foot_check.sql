CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DIABETES_FOOT_CHECK (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    DIABETES_TYPE VARCHAR, -- Type of diabetes (from diabetes register)
    LATEST_FOOT_CHECK_DATE DATE, -- Date of the latest foot check
    FOOT_CHECK_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if a valid foot check was completed in last 12 months
    IS_PERMANENTLY_EXEMPT BOOLEAN, -- Flag indicating if patient is permanently exempt from foot checks (both feet absent/amputated)
    LATEST_CHECK_STATUS VARCHAR, -- Status of most recent check (Complete, Declined, Unsuitable, Not Done)
    FOOT_CHECK_STATUS VARCHAR, -- Detailed status of the foot check (Complete, Partial, Not Done, Not Appropriate)
    LEFT_FOOT_STATUS VARCHAR, -- Status of left foot (Low Risk, Moderate Risk, High Risk, Ulcerated, Absent, Amputated)
    RIGHT_FOOT_STATUS VARCHAR, -- Status of right foot (Low Risk, Moderate Risk, High Risk, Ulcerated, Absent, Amputated)
    TOWNSON_SCALE_LEVEL VARCHAR, -- Young Townson footskin scale level if used (Level 1-4)
    ALL_CONCEPT_CODES ARRAY, -- Array of all concept codes from latest foot check
    ALL_CONCEPT_DISPLAYS ARRAY, -- Array of all concept displays from latest foot check
    ALL_SOURCE_CLUSTER_IDS ARRAY -- Array of all source cluster IDs from latest foot check
)
COMMENT = 'Fact table for diabetes foot checks. Includes latest foot check status and completion within 12 months for people on the diabetes register. Tracks foot status (risk levels, amputations, absences) and check appropriateness.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

SELECT 
    d.PERSON_ID,
    d.SK_PATIENT_ID,
    d.DIABETES_TYPE,
    fc.CLINICAL_EFFECTIVE_DATE AS LATEST_FOOT_CHECK_DATE,
    -- Check completed in last 12 months if:
    -- 1. We have a check date within 12 months AND
    -- 2. Either both feet were checked OR the unchecked foot is absent/amputated
    -- 3. The check wasn't declined or unsuitable
    CASE
        WHEN fc.CLINICAL_EFFECTIVE_DATE IS NOT NULL 
            AND DATEDIFF(month, fc.CLINICAL_EFFECTIVE_DATE, CURRENT_DATE()) <= 12
            AND (
                fc.BOTH_FEET_CHECKED 
                OR (fc.LEFT_FOOT_CHECKED AND (fc.RIGHT_FOOT_ABSENT OR fc.RIGHT_FOOT_AMPUTATED))
                OR (fc.RIGHT_FOOT_CHECKED AND (fc.LEFT_FOOT_ABSENT OR fc.LEFT_FOOT_AMPUTATED))
            )
            AND NOT (fc.IS_UNSUITABLE OR fc.IS_DECLINED)
        THEN TRUE
        ELSE FALSE
    END AS FOOT_CHECK_COMPLETED_IN_LAST_12M,
    -- Patient is permanently exempt only if both feet are absent/amputated
    CASE
        WHEN (fc.LEFT_FOOT_ABSENT OR fc.LEFT_FOOT_AMPUTATED)
            AND (fc.RIGHT_FOOT_ABSENT OR fc.RIGHT_FOOT_AMPUTATED)
        THEN TRUE
        ELSE FALSE
    END AS IS_PERMANENTLY_EXEMPT,
    -- Status of the most recent check attempt
    CASE
        WHEN fc.CLINICAL_EFFECTIVE_DATE IS NULL THEN 'Not Done'
        WHEN fc.IS_DECLINED THEN 'Declined'
        WHEN fc.IS_UNSUITABLE THEN 'Unsuitable'
        WHEN fc.BOTH_FEET_CHECKED OR (fc.LEFT_FOOT_CHECKED AND fc.RIGHT_FOOT_CHECKED) THEN 'Complete - Both Feet'
        WHEN fc.LEFT_FOOT_CHECKED AND (fc.RIGHT_FOOT_ABSENT OR fc.RIGHT_FOOT_AMPUTATED) THEN 'Complete - Left Only (Right Missing)'
        WHEN fc.RIGHT_FOOT_CHECKED AND (fc.LEFT_FOOT_ABSENT OR fc.LEFT_FOOT_AMPUTATED) THEN 'Complete - Right Only (Left Missing)'
        WHEN fc.LEFT_FOOT_CHECKED THEN 'Partial - Left Only'
        WHEN fc.RIGHT_FOOT_CHECKED THEN 'Partial - Right Only'
        ELSE 'Not Done'
    END AS LATEST_CHECK_STATUS,
    -- Detailed status including permanent exemptions
    CASE
        WHEN (fc.LEFT_FOOT_ABSENT OR fc.LEFT_FOOT_AMPUTATED)
            AND (fc.RIGHT_FOOT_ABSENT OR fc.RIGHT_FOOT_AMPUTATED)
        THEN 'Permanently Exempt - Both Feet Missing'
        WHEN fc.IS_UNSUITABLE THEN 'Not Appropriate - Unsuitable'
        WHEN fc.IS_DECLINED THEN 'Not Appropriate - Declined'
        WHEN fc.BOTH_FEET_CHECKED THEN 'Complete - Both Feet'
        WHEN fc.LEFT_FOOT_CHECKED AND fc.RIGHT_FOOT_CHECKED THEN 'Complete - Both Feet'
        WHEN fc.LEFT_FOOT_CHECKED AND (fc.RIGHT_FOOT_ABSENT OR fc.RIGHT_FOOT_AMPUTATED) THEN 'Complete - Left Only (Right Missing)'
        WHEN fc.RIGHT_FOOT_CHECKED AND (fc.LEFT_FOOT_ABSENT OR fc.LEFT_FOOT_AMPUTATED) THEN 'Complete - Right Only (Left Missing)'
        WHEN fc.LEFT_FOOT_CHECKED THEN 'Partial - Left Only'
        WHEN fc.RIGHT_FOOT_CHECKED THEN 'Partial - Right Only'
        WHEN fc.CLINICAL_EFFECTIVE_DATE IS NULL THEN 'Not Done'
        ELSE 'Not Done'
    END AS FOOT_CHECK_STATUS,
    -- Status of each foot, including risk level
    CASE
        WHEN fc.LEFT_FOOT_ABSENT THEN 'Absent (Congenital)'
        WHEN fc.LEFT_FOOT_AMPUTATED THEN 'Amputated'
        WHEN fc.LEFT_FOOT_RISK_LEVEL IS NOT NULL THEN fc.LEFT_FOOT_RISK_LEVEL || ' Risk'
        WHEN fc.LEFT_FOOT_CHECKED THEN 'Risk Level Not Recorded'
        ELSE 'Not Assessed'
    END AS LEFT_FOOT_STATUS,
    CASE
        WHEN fc.RIGHT_FOOT_ABSENT THEN 'Absent (Congenital)'
        WHEN fc.RIGHT_FOOT_AMPUTATED THEN 'Amputated'
        WHEN fc.RIGHT_FOOT_RISK_LEVEL IS NOT NULL THEN fc.RIGHT_FOOT_RISK_LEVEL || ' Risk'
        WHEN fc.RIGHT_FOOT_CHECKED THEN 'Risk Level Not Recorded'
        ELSE 'Not Assessed'
    END AS RIGHT_FOOT_STATUS,
    -- Include Townson scale level if used
    fc.TOWNSON_SCALE_LEVEL,
    -- Arrays for traceability
    fc.ALL_CONCEPT_CODES,
    fc.ALL_CONCEPT_DISPLAYS,
    fc.ALL_SOURCE_CLUSTER_IDS
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_DIABETES d
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_FOOT_CHECK_LATEST fc
    ON d.PERSON_ID = fc.PERSON_ID; 