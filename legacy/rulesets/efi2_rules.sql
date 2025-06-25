CREATE OR REPLACE TABLE DATA_LAB_NCL_TRAINING_TEMP.RULESETS.EFI2_RULES AS
SELECT
    -- Core identification
    e.SNOMEDCT_CONCEPTID as CODE,
    e.DEFICIT,
    -- Note: All deficits are for patients aged 65+ by default (eFI2 algorithm requirement)
    -- Specific age restrictions are captured in MIN_AGE and MAX_AGE fields
    CASE
        WHEN e.DEFICIT IN ('Alcohol', 'BMI', 'Smoker (current)', 'Smoker (ex)') THEN 'CATEGORICAL'
        WHEN e.DEFICIT = 'Polypharmacy' THEN 'POLYPHARMACY'
        WHEN e.OTHERINSTRUCTIONS LIKE '%greater than%'
             OR e.OTHERINSTRUCTIONS LIKE '%less than%'
             OR e.OTHERINSTRUCTIONS LIKE '%equal to%' THEN 'NUMERIC'
        ELSE 'BINARY'
    END as DEFICIT_TYPE,

    -- Time constraints (from document)
    CASE
        WHEN e.DEFICIT = 'Alcohol' THEN 1825  -- 5 years
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 1825  -- 5 years
        WHEN e.DEFICIT = 'Polypharmacy' THEN 90  -- 90 days
        ELSE NULL
    END as TIME_CONSTRAINT_DAYS,
    CASE
        WHEN e.DEFICIT = 'Alcohol' THEN 'LOOKBACK'
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 'FOLLOWING_DIAGNOSIS'
        WHEN e.DEFICIT = 'Polypharmacy' THEN 'LOOKBACK'
        ELSE NULL
    END as TIME_CONSTRAINT_TYPE,
    CASE
        WHEN e.DEFICIT = 'Alcohol' THEN '5 year lookback for alcohol history'
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN '5 year lookback with resolution check'
        WHEN e.DEFICIT = 'Polypharmacy' THEN '90 day lookback for medication count'
        ELSE NULL
    END as TIME_CONSTRAINT_DESCRIPTION,

    -- Age restrictions (from document)
    CASE
        WHEN e.DEFICIT = 'Asthma' THEN 18
        WHEN e.DEFICIT = 'Urinary incontinence' THEN 18
        WHEN e.DEFICIT = 'Fracture' THEN 55
        ELSE NULL
    END as MIN_AGE,

    CASE
        WHEN e.DEFICIT = 'Asthma' THEN 'Only valid for patients aged 18+'
        WHEN e.DEFICIT = 'Urinary incontinence' THEN 'Only valid for patients aged 18+'
        WHEN e.DEFICIT = 'Fracture' THEN 'Only valid for patients aged 55+'
        ELSE NULL
    END as AGE_RESTRICTION_DESCRIPTION,

    -- Categorical rules (for Alcohol, BMI, Smoking)
    CASE
        WHEN e.DEFICIT = 'Alcohol' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 'ALCOHOL_ZERO'
                WHEN e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN 'ALCOHOL_LOWER_RISK'
                WHEN e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN 'ALCOHOL_HIGHER_RISK'
                WHEN e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 'ALCOHOL_HARMFUL'
                WHEN e.OTHERINSTRUCTIONS = 'Previous higher risk/harmful drinking' THEN 'ALCOHOL_PREVIOUS_HIGHER'
                ELSE NULL
            END
        WHEN e.DEFICIT = 'BMI' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN 'BMI_OBESE'
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 'BMI_UNDERWEIGHT'
                ELSE NULL
            END
        WHEN e.DEFICIT IN ('Smoker (current)', 'Smoker (ex)') THEN
            CASE
                WHEN e.DEFICIT = 'Smoker (current)' THEN 'SMOKER_CURRENT'
                WHEN e.DEFICIT = 'Smoker (ex)' THEN 'SMOKER_EX'
                ELSE NULL
            END
        ELSE NULL
    END as CATEGORY_NAME,

    CASE
        WHEN e.DEFICIT = 'Alcohol' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 1
                WHEN e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN 2
                WHEN e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN 3
                WHEN e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 4
                WHEN e.OTHERINSTRUCTIONS = 'Previous higher risk/harmful drinking' THEN 5
                ELSE NULL
            END
        ELSE NULL
    END as CATEGORY_ORDER,

    CASE
        WHEN e.DEFICIT = 'Alcohol' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0
                WHEN e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN 1
                WHEN e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN 21
                WHEN e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 49
                ELSE NULL
            END
        WHEN e.DEFICIT = 'BMI' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN 30
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 0
                ELSE NULL
            END
        ELSE NULL
    END as CATEGORY_RANGE_START,

    CASE
        WHEN e.DEFICIT = 'Alcohol' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0
                WHEN e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN 20
                WHEN e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN 48
                WHEN e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 999
                ELSE NULL
            END
        WHEN e.DEFICIT = 'BMI' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN 999
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 18.5
                ELSE NULL
            END
        ELSE NULL
    END as CATEGORY_RANGE_END,

    CASE
        WHEN e.DEFICIT = 'Alcohol' THEN e.OTHERINSTRUCTIONS
        WHEN e.DEFICIT = 'BMI' THEN e.OTHERINSTRUCTIONS
        WHEN e.DEFICIT IN ('Smoker (current)', 'Smoker (ex)') THEN e.DEFICIT
        ELSE NULL
    END as CATEGORY_DESCRIPTION,

    -- Numeric thresholds (for BP, lab results, BMI, etc.)
    CASE
        -- Blood pressure thresholds
        WHEN e.DEFICIT = 'Hypertension' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%140%' THEN 140
                WHEN e.OTHERINSTRUCTIONS LIKE '%135%' THEN 135
                WHEN e.OTHERINSTRUCTIONS LIKE '%90%' THEN 90
                WHEN e.OTHERINSTRUCTIONS LIKE '%85%' THEN 85
                ELSE NULL
            END
        WHEN e.DEFICIT = 'Hypotension / syncope' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%90%' THEN 90
                WHEN e.OTHERINSTRUCTIONS LIKE '%60%' THEN 60
                ELSE NULL
            END
        -- Kidney function thresholds
        WHEN e.DEFICIT = 'Chronic kidney disease' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%150%' THEN 150
                WHEN e.OTHERINSTRUCTIONS LIKE '%50%' THEN 50
                WHEN e.OTHERINSTRUCTIONS LIKE '%20%' THEN 20
                WHEN e.OTHERINSTRUCTIONS LIKE '%3%' THEN 3
                WHEN e.OTHERINSTRUCTIONS LIKE '%60%' THEN 60
                ELSE NULL
            END
        -- BMI thresholds
        WHEN e.DEFICIT = 'BMI' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN 30
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 18.5
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI 25+%' THEN 25
                ELSE NULL
            END
        -- Alcohol thresholds
        WHEN e.DEFICIT = 'Alcohol' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN 14
                WHEN e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN 35
                WHEN e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 50
                ELSE NULL
            END
        -- Other numeric thresholds
        WHEN e.OTHERINSTRUCTIONS LIKE '%greater than%' OR
             e.OTHERINSTRUCTIONS LIKE '%less than%' OR
             e.OTHERINSTRUCTIONS LIKE '%equal to%' OR
             e.OTHERINSTRUCTIONS LIKE '%>=%' OR
             e.OTHERINSTRUCTIONS LIKE '%<=%' OR
             e.OTHERINSTRUCTIONS LIKE '%=%' THEN
            -- Extract first number from the instruction
            REGEXP_SUBSTR(e.OTHERINSTRUCTIONS, '[0-9]+(\.[0-9]+)?', 1, 1)::FLOAT
        ELSE NULL
    END as THRESHOLD_VALUE,

    CASE
        -- Standard comparators
        WHEN e.OTHERINSTRUCTIONS LIKE '%less than%' OR e.OTHERINSTRUCTIONS LIKE '%<%' THEN '<'
        WHEN e.OTHERINSTRUCTIONS LIKE '%greater than%' OR e.OTHERINSTRUCTIONS LIKE '%>%' THEN '>'
        WHEN e.OTHERINSTRUCTIONS LIKE '%equal to%' OR e.OTHERINSTRUCTIONS LIKE '%=%' THEN '='
        -- Combined comparators
        WHEN e.OTHERINSTRUCTIONS LIKE '%less than or equal to%' OR e.OTHERINSTRUCTIONS LIKE '%<=%' THEN '≤'
        WHEN e.OTHERINSTRUCTIONS LIKE '%greater than or equal to%' OR e.OTHERINSTRUCTIONS LIKE '%>=%' THEN '≥'
        -- Special cases
        WHEN e.DEFICIT = 'BMI' AND e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN '≥'
        WHEN e.DEFICIT = 'BMI' AND e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN '<'
        WHEN e.DEFICIT = 'BMI' AND e.OTHERINSTRUCTIONS LIKE '%BMI 25+%' THEN '≥'
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN '≤'
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN '>'
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN '>'
        ELSE NULL
    END as THRESHOLD_COMPARATOR,

    -- Add threshold description for clarity
    CASE
        WHEN e.DEFICIT = 'Hypertension' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%systolic%' AND e.OTHERINSTRUCTIONS LIKE '%140%' THEN 'Systolic BP ≥ 140 mmHg'
                WHEN e.OTHERINSTRUCTIONS LIKE '%systolic%' AND e.OTHERINSTRUCTIONS LIKE '%135%' THEN 'Systolic BP ≥ 135 mmHg'
                WHEN e.OTHERINSTRUCTIONS LIKE '%diastolic%' AND e.OTHERINSTRUCTIONS LIKE '%90%' THEN 'Diastolic BP ≥ 90 mmHg'
                WHEN e.OTHERINSTRUCTIONS LIKE '%diastolic%' AND e.OTHERINSTRUCTIONS LIKE '%85%' THEN 'Diastolic BP ≥ 85 mmHg'
                ELSE NULL
            END
        WHEN e.DEFICIT = 'Hypotension / syncope' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%systolic%' AND e.OTHERINSTRUCTIONS LIKE '%90%' THEN 'Systolic BP < 90 mmHg'
                WHEN e.OTHERINSTRUCTIONS LIKE '%diastolic%' AND e.OTHERINSTRUCTIONS LIKE '%60%' THEN 'Diastolic BP < 60 mmHg'
                ELSE NULL
            END
        WHEN e.DEFICIT = 'Chronic kidney disease' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%mg/24hr%' AND e.OTHERINSTRUCTIONS LIKE '%150%' THEN 'eGFR < 150 mg/24hr'
                WHEN e.OTHERINSTRUCTIONS LIKE '%mg/mmol%' AND e.OTHERINSTRUCTIONS LIKE '%50%' THEN 'eGFR < 50 mg/mmol'
                WHEN e.OTHERINSTRUCTIONS LIKE '%mg/mmol%' AND e.OTHERINSTRUCTIONS LIKE '%20%' THEN 'eGFR < 20 mg/mmol'
                WHEN e.OTHERINSTRUCTIONS LIKE '%mg/mmol%' AND e.OTHERINSTRUCTIONS LIKE '%3%' THEN 'eGFR < 3 mg/mmol'
                WHEN e.OTHERINSTRUCTIONS LIKE '%mg/mmol%' AND e.OTHERINSTRUCTIONS LIKE '%60%' THEN 'eGFR < 60 mg/mmol'
                ELSE NULL
            END
        WHEN e.DEFICIT = 'BMI' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN 'BMI ≥ 30 (Obese)'
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 'BMI < 18.5 (Underweight)'
                WHEN e.OTHERINSTRUCTIONS LIKE '%BMI 25+%' THEN 'BMI ≥ 25 (Overweight)'
                ELSE NULL
            END
        WHEN e.DEFICIT = 'Alcohol' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN '≤ 14 units/week'
                WHEN e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN '> 35 units/week'
                WHEN e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN '> 50 units/week'
                ELSE NULL
            END
        ELSE NULL
    END as THRESHOLD_DESCRIPTION,

    CASE
        WHEN e.DEFICIT IN ('Hypertension', 'Hypotension / syncope') THEN 'mmHg'
        WHEN e.DEFICIT = 'Chronic kidney disease' THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%mg/24hr%' THEN 'mg/24hr'
                WHEN e.OTHERINSTRUCTIONS LIKE '%mg/mmol%' THEN 'mg/mmol'
                ELSE NULL
            END
        ELSE NULL
    END as THRESHOLD_UNIT,

    CASE
        WHEN e.DEFICIT IN ('Hypertension', 'Hypotension / syncope') THEN
            CASE
                WHEN e.OTHERINSTRUCTIONS LIKE '%systolic%' THEN 'SYSTOLIC'
                WHEN e.OTHERINSTRUCTIONS LIKE '%diastolic%' THEN 'DIASTOLIC'
                ELSE NULL
            END
        ELSE NULL
    END as THRESHOLD_TYPE,

    CASE
        WHEN e.DEFICIT IN ('Hypertension', 'Hypotension / syncope') AND e.OTHERINSTRUCTIONS LIKE '%3 reading%' THEN 3
        ELSE NULL
    END as READINGS_REQUIRED,

    CASE
        WHEN e.DEFICIT IN ('Hypertension', 'Hypotension / syncope') AND e.OTHERINSTRUCTIONS LIKE '%EVER%' THEN 'EVER'
        ELSE NULL
    END as READINGS_TIMEFRAME,

    -- Hierarchical relationships (for Cognitive/Dementia)
    CASE
        WHEN e.DEFICIT = 'Dementia' THEN 'Cognitive impairment,Memory concerns'
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 'Memory concerns'
        ELSE NULL
    END as SUPERSEDES,

    CASE
        WHEN e.DEFICIT = 'Memory concerns' THEN 'Cognitive impairment,Dementia'
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 'Dementia'
        ELSE NULL
    END as SUPERSEDED_BY,

    CASE
        WHEN e.DEFICIT = 'Dementia' THEN 1
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 2
        WHEN e.DEFICIT = 'Memory concerns' THEN 3
        ELSE NULL
    END as HIERARCHY_LEVEL,

    -- Resolution conditions (for Anaemia)
    CASE
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 'HB_TEST'
        ELSE NULL
    END as RESOLUTION_TEST,

    CASE
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 13.0
        ELSE NULL
    END as RESOLUTION_THRESHOLD,

    CASE
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 'g/L'
        ELSE NULL
    END as RESOLUTION_UNIT,

    CASE
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN TRUE
        ELSE NULL
    END as RESOLUTION_GENDER_SPECIFIC,

    CASE
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 13.0
        ELSE NULL
    END as RESOLUTION_MALE_THRESHOLD,

    CASE
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 11.5
        ELSE NULL
    END as RESOLUTION_FEMALE_THRESHOLD,

    -- Validation rules
    CASE
        WHEN e.DEFICIT IN ('Smoker (current)', 'Smoker (ex)') THEN 'CANNOT_HAVE_BOTH_SMOKING_STATUS'
        WHEN e.DEFICIT IN ('Cognitive impairment', 'Dementia', 'Memory concerns') THEN 'COGNITIVE_HIERARCHY_VALIDATION'
        WHEN e.DEFICIT = 'Alcohol' THEN 'ALCOHOL_CATEGORIES_MUTUALLY_EXCLUSIVE'
        ELSE NULL
    END as VALIDATION_RULE,

    CASE
        WHEN e.DEFICIT IN ('Smoker (current)', 'Smoker (ex)') THEN 'Patient cannot be both current and ex-smoker'
        WHEN e.DEFICIT IN ('Cognitive impairment', 'Dementia', 'Memory concerns') THEN 'Dementia supersedes Cognitive impairment which supersedes Memory concerns'
        WHEN e.DEFICIT = 'Alcohol' THEN 'Alcohol categories are mutually exclusive and ordered by severity'
        ELSE NULL
    END as VALIDATION_DESCRIPTION,

    -- Algorithm coefficients
    CASE
        WHEN e.DEFICIT = 'Activity limitation' THEN 0.15284
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 0.23107
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0.13175
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Previous higher risk/harmful drinking' THEN 1.36434
        WHEN e.DEFICIT = 'Atrial fibrillation' THEN 0.13025
        WHEN e.DEFICIT = 'Cancer' THEN 0.2406
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 0.10985
        WHEN e.DEFICIT = 'COPD' THEN 0.11683
        WHEN e.DEFICIT = 'Dementia' THEN 0.41715
        WHEN e.DEFICIT = 'Dressing & grooming problems' THEN 0.05422
        WHEN e.DEFICIT = 'Environment problems' THEN 0.11886
        WHEN e.DEFICIT = 'Falls' THEN 0.62743
        WHEN e.DEFICIT = 'Fracture' THEN 0.07353
        WHEN e.DEFICIT = 'Fragility fracture' THEN 0.17425
        WHEN e.DEFICIT = 'Heart failure' THEN 0.11086
        WHEN e.DEFICIT = 'Housebound' THEN 0.33254
        WHEN e.DEFICIT = 'Hypotension / syncope' THEN 0.18253
        WHEN e.DEFICIT = 'Liver problems' THEN 0.23787
        WHEN e.DEFICIT = 'Medication management problems' THEN 0.32125
        WHEN e.DEFICIT = 'Memory concerns' THEN 0.11915
        WHEN e.DEFICIT = 'Mobility problems' THEN 0.46836
        WHEN e.DEFICIT = 'Motor neuron disease' THEN 0.35347
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0.25318  -- BMI missing
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 0.4417  -- BMI underweight
        WHEN e.DEFICIT = 'Palliative care' THEN 0.5145
        WHEN e.DEFICIT = 'Parkinsonism & tremor' THEN 0.03537
        WHEN e.DEFICIT = 'Peptic ulcer disease' THEN 0.05427
        WHEN e.DEFICIT = 'Peripheral vascular disease' THEN 0.02672
        WHEN e.DEFICIT = 'Requirement for care' THEN 0.21428
        WHEN e.DEFICIT = 'Respiratory disease' THEN 0.01049
        WHEN e.DEFICIT = 'Seizures' THEN 0.02885
        WHEN e.DEFICIT = 'Self-harm' THEN 0.00900
        WHEN e.DEFICIT = 'Skin ulcer' THEN 0.21935
        WHEN e.DEFICIT = 'Smoker (current)' THEN 0.10291
        WHEN e.DEFICIT = 'Social vulnerability' THEN 0.23585
        WHEN e.DEFICIT = 'Stroke' THEN 0.10565
        WHEN e.DEFICIT = 'Transient ischaemic attack' THEN 0.02305
        WHEN e.DEFICIT = 'Weight loss' THEN 0.19256
        ELSE NULL
    END as EFI2_COEFFICIENT,

    CASE
        WHEN e.DEFICIT = 'Activity limitation' THEN 0.018
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 0.027
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0.016
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Previous higher risk/harmful drinking' THEN 0.162
        WHEN e.DEFICIT = 'Atrial fibrillation' THEN 0.015
        WHEN e.DEFICIT = 'Cancer' THEN 0.029
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 0.013
        WHEN e.DEFICIT = 'COPD' THEN 0.014
        WHEN e.DEFICIT = 'Dementia' THEN 0.049
        WHEN e.DEFICIT = 'Dressing & grooming problems' THEN 0.006
        WHEN e.DEFICIT = 'Environment problems' THEN 0.014
        WHEN e.DEFICIT = 'Falls' THEN 0.074
        WHEN e.DEFICIT = 'Fracture' THEN 0.009
        WHEN e.DEFICIT = 'Fragility fracture' THEN 0.021
        WHEN e.DEFICIT = 'Heart failure' THEN 0.013
        WHEN e.DEFICIT = 'Housebound' THEN 0.039
        WHEN e.DEFICIT = 'Hypotension / syncope' THEN 0.022
        WHEN e.DEFICIT = 'Liver problems' THEN 0.028
        WHEN e.DEFICIT = 'Medication management problems' THEN 0.038
        WHEN e.DEFICIT = 'Memory concerns' THEN 0.014
        WHEN e.DEFICIT = 'Mobility problems' THEN 0.056
        WHEN e.DEFICIT = 'Motor neuron disease' THEN 0.042
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0.030  -- BMI missing
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 0.052  -- BMI underweight
        WHEN e.DEFICIT = 'Palliative care' THEN 0.061
        WHEN e.DEFICIT = 'Parkinsonism & tremor' THEN 0.004
        WHEN e.DEFICIT = 'Peptic ulcer disease' THEN 0.006
        WHEN e.DEFICIT = 'Peripheral vascular disease' THEN 0.003
        WHEN e.DEFICIT = 'Requirement for care' THEN 0.025
        WHEN e.DEFICIT = 'Respiratory disease' THEN 0.001
        WHEN e.DEFICIT = 'Seizures' THEN 0.003
        WHEN e.DEFICIT = 'Self-harm' THEN 0.001
        WHEN e.DEFICIT = 'Skin ulcer' THEN 0.026
        WHEN e.DEFICIT = 'Smoker (current)' THEN 0.012
        WHEN e.DEFICIT = 'Social vulnerability' THEN 0.028
        WHEN e.DEFICIT = 'Stroke' THEN 0.013
        WHEN e.DEFICIT = 'Transient ischaemic attack' THEN 0.003
        WHEN e.DEFICIT = 'Weight loss' THEN 0.023
        ELSE NULL
    END as EFI2_TRANSFORMED_COEFFICIENT,

    -- Falls model coefficients
    CASE
        WHEN e.DEFICIT = 'Abdominal pain' THEN -0.0641861
        WHEN e.DEFICIT = 'Activity limitation' THEN 0.0475092
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 0.1733029
        WHEN e.DEFICIT = 'Asthma' THEN 0.0816046
        WHEN e.DEFICIT = 'Atrial fibrillation' THEN 0.1519654
        WHEN e.DEFICIT = 'Back pain' THEN 0.0498699
        WHEN e.DEFICIT = 'Bone disease' THEN -0.0267845
        WHEN e.DEFICIT = 'Cancer' THEN 0.0301854
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 0.1472251
        WHEN e.DEFICIT = 'COPD' THEN 0.039956
        WHEN e.DEFICIT = 'Dementia' THEN 0.1038111
        WHEN e.DEFICIT = 'Depression' THEN 0.1633415
        WHEN e.DEFICIT = 'Diabetes mellitus' THEN 0.0373911
        WHEN e.DEFICIT = 'Dizziness' THEN 0.0198363
        WHEN e.DEFICIT = 'Dressing & grooming problems' THEN 0.4532777
        WHEN e.DEFICIT = 'Faecal incontinence' THEN -0.071791
        WHEN e.DEFICIT = 'Falls' THEN 0.3009161
        WHEN e.DEFICIT = 'Fatigue' THEN -0.0635057
        WHEN e.DEFICIT = 'Foot problems' THEN 0.0282736
        WHEN e.DEFICIT = 'Fracture' THEN 0.1957923
        WHEN e.DEFICIT = 'Fragility fracture' THEN 0.2031303
        WHEN e.DEFICIT = 'General mental health' THEN 0.0991068
        WHEN e.DEFICIT = 'Headache' THEN -0.0149365
        WHEN e.DEFICIT = 'Hearing impairment' THEN -0.0168728
        WHEN e.DEFICIT = 'Heart failure' THEN -0.0190399
        WHEN e.DEFICIT = 'Housebound' THEN 0.2549983
        WHEN e.DEFICIT = 'Hypertension' THEN -0.0318888
        WHEN e.DEFICIT = 'Hypotension / syncope' THEN 0.0778591
        WHEN e.DEFICIT = 'Inflammatory arthritis' THEN 0.0586785
        WHEN e.DEFICIT = 'Inflammatory bowel disease' THEN 0.0145135
        WHEN e.DEFICIT = 'Liver problems' THEN 0.3803626
        WHEN e.DEFICIT = 'Meal preparation problems' THEN -0.140024
        WHEN e.DEFICIT = 'Medication management problems' THEN 0.8030273
        WHEN e.DEFICIT = 'Memory concerns' THEN 0.2601186
        WHEN e.DEFICIT = 'Mobility problems' THEN -0.1310067
        WHEN e.DEFICIT = 'Mono/hemiparesis' THEN 0.1020457
        WHEN e.DEFICIT = 'Motor neuron disease' THEN -0.1209976
        WHEN e.DEFICIT = 'Musculoskeletal problems' THEN 0.0419361
        WHEN e.DEFICIT = 'Osteoarthritis' THEN 0.0634073
        WHEN e.DEFICIT = 'Osteoporosis' THEN 0.1276254
        WHEN e.DEFICIT = 'Palliative care' THEN -0.2353552
        WHEN e.DEFICIT = 'Parkinsonism & tremor' THEN 0.2312839
        WHEN e.DEFICIT = 'Peptic ulcer disease' THEN 0.0056687
        WHEN e.DEFICIT = 'Peripheral neuropathy' THEN 0.0335789
        WHEN e.DEFICIT = 'Peripheral vascular disease' THEN 0.0173065
        WHEN e.DEFICIT = 'Requirement for care' THEN -0.2177301
        WHEN e.DEFICIT = 'Respiratory disease' THEN 0.0132757
        WHEN e.DEFICIT = 'Seizures' THEN 0.2571899
        WHEN e.DEFICIT = 'Self-harm' THEN 0.1461241
        WHEN e.DEFICIT = 'Severe mental illness' THEN 0.0676153
        WHEN e.DEFICIT = 'Skin ulcer' THEN 0.0746926
        WHEN e.DEFICIT = 'Sleep problems' THEN 0.0056967
        WHEN e.DEFICIT = 'Social vulnerability' THEN 0.0495075
        WHEN e.DEFICIT = 'Stress' THEN -0.0200494
        WHEN e.DEFICIT = 'Stroke' THEN 0.0788542
        WHEN e.DEFICIT = 'Thyroid problems' THEN -0.0273864
        WHEN e.DEFICIT = 'Urinary incontinence' THEN 0.0345173
        WHEN e.DEFICIT = 'Urinary system disease' THEN 0.0119309
        WHEN e.DEFICIT = 'Visual impairment' THEN 0.02332
        WHEN e.DEFICIT = 'Washing & bathing problems' THEN -0.1118824
        WHEN e.DEFICIT = 'Weakness' THEN -0.0589464
        WHEN e.DEFICIT = 'Weight loss' THEN 0.0301414
        -- Special cases for categorical variables
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 0.4164064
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN 0.1549725
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN 0
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Previous higher risk/harmful drinking' THEN 0.0849676
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0.0070124
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 0.4896735  -- BMI underweight
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'BMI normal' THEN 0.2394177
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'BMI overweight' THEN 0
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN -0.0411134  -- BMI obese
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN -0.1451981  -- BMI missing
        WHEN e.DEFICIT = 'Smoker (current)' THEN 0.0684529
        ELSE NULL
    END as EFI_PLUS_FALLS_COEFFICIENT,

    -- Care home admission model coefficients
    CASE
        WHEN e.DEFICIT = 'Abdominal pain' THEN -0.0149019
        WHEN e.DEFICIT = 'Activity limitation' THEN 0.0038632
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 0.0558647
        WHEN e.DEFICIT = 'Asthma' THEN 0.0391181
        WHEN e.DEFICIT = 'Atrial fibrillation' THEN 0.051997
        WHEN e.DEFICIT = 'Back pain' THEN 0.0331701
        WHEN e.DEFICIT = 'Bone disease' THEN -0.0004907
        WHEN e.DEFICIT = 'Cancer' THEN 0.0186758
        WHEN e.DEFICIT = 'Chronic kidney disease' THEN 0.0107882
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 0.0142797
        WHEN e.DEFICIT = 'COPD' THEN 0.0142169
        WHEN e.DEFICIT = 'Dementia' THEN 0.0197444
        WHEN e.DEFICIT = 'Depression' THEN 0.0434872
        WHEN e.DEFICIT = 'Diabetes mellitus' THEN 0.0154494
        WHEN e.DEFICIT = 'Dizziness' THEN 0.0104553
        WHEN e.DEFICIT = 'Dressing & grooming problems' THEN 0.0008617
        WHEN e.DEFICIT = 'Faecal incontinence' THEN -0.0059002
        WHEN e.DEFICIT = 'Falls' THEN 0.1161479
        WHEN e.DEFICIT = 'Fatigue' THEN -0.0100522
        WHEN e.DEFICIT = 'Foot problems' THEN 0.0072954
        WHEN e.DEFICIT = 'Fracture' THEN 0.085402
        WHEN e.DEFICIT = 'Fragility fracture' THEN 0.0691495
        WHEN e.DEFICIT = 'General mental health' THEN 0.0270417
        WHEN e.DEFICIT = 'Housebound' THEN 0.0987896
        WHEN e.DEFICIT = 'Hypertension' THEN 0.0146154
        WHEN e.DEFICIT = 'Hypotension / syncope' THEN 0.0220124
        WHEN e.DEFICIT = 'Inflammatory arthritis' THEN 0.0252711
        WHEN e.DEFICIT = 'Inflammatory bowel disease' THEN 0.002923
        WHEN e.DEFICIT = 'Ischaemic heart disease' THEN 0.0005033
        WHEN e.DEFICIT = 'Liver problems' THEN 0.027092
        WHEN e.DEFICIT = 'Meal preparation problems' THEN -0.0030207
        WHEN e.DEFICIT = 'Medication management problems' THEN 0.0016596
        WHEN e.DEFICIT = 'Memory concerns' THEN 0.0498515
        WHEN e.DEFICIT = 'Mobility problems' THEN -0.0134823
        WHEN e.DEFICIT = 'Mono/hemiparesis' THEN 0.0108169
        WHEN e.DEFICIT = 'Motor neuron disease' THEN -0.00053
        WHEN e.DEFICIT = 'Musculoskeletal problems' THEN 0.0404077
        WHEN e.DEFICIT = 'Osteoarthritis' THEN 0.0378613
        WHEN e.DEFICIT = 'Osteoporosis' THEN 0.0494849
        WHEN e.DEFICIT = 'Palliative care' THEN -0.0200984
        WHEN e.DEFICIT = 'Parkinsonism & tremor' THEN 0.0406329
        WHEN e.DEFICIT = 'Peptic ulcer disease' THEN 0.0010279
        WHEN e.DEFICIT = 'Peripheral neuropathy' THEN 0.0069964
        WHEN e.DEFICIT = 'Peripheral vascular disease' THEN 0.0050192
        WHEN e.DEFICIT = 'Requirement for care' THEN -0.0345862
        WHEN e.DEFICIT = 'Respiratory disease' THEN 0.0030062
        WHEN e.DEFICIT = 'Seizures' THEN 0.0400751
        WHEN e.DEFICIT = 'Self-harm' THEN 0.0058617
        WHEN e.DEFICIT = 'Severe mental illness' THEN 0.0171809
        WHEN e.DEFICIT = 'Skin ulcer' THEN 0.0270553
        WHEN e.DEFICIT = 'Sleep problems' THEN 0.0020218
        WHEN e.DEFICIT = 'Social vulnerability' THEN 0.0060861
        WHEN e.DEFICIT = 'Stress' THEN -0.0007004
        WHEN e.DEFICIT = 'Stroke' THEN 0.0235006
        WHEN e.DEFICIT = 'Transient ischaemic attack' THEN 0.0015024
        WHEN e.DEFICIT = 'Urinary incontinence' THEN 0.0069321
        WHEN e.DEFICIT = 'Urinary system disease' THEN 0.015496
        WHEN e.DEFICIT = 'Visual impairment' THEN 0.021559
        WHEN e.DEFICIT = 'Washing & bathing problems' THEN -0.0021369
        WHEN e.DEFICIT = 'Weakness' THEN -0.0010064
        WHEN e.DEFICIT = 'Weight loss' THEN 0.0071009
        -- Special cases for categorical variables
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 0.0354117
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN 0.0035523
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN 0
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Previous higher risk/harmful drinking' THEN 0.0000913
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 0.0677518  -- BMI underweight
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'BMI normal' THEN 0.0915853
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'BMI overweight' THEN 0
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN -0.01303  -- BMI obese
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN -0.1618026  -- BMI missing
        WHEN e.DEFICIT = 'Smoker (current)' THEN 0.0191217
        ELSE NULL
    END as EFI_PLUS_CARE_HOME_COEFFICIENT,

    -- Mortality model coefficients
    CASE
        WHEN e.DEFICIT = 'Abdominal pain' THEN -0.004378846
        WHEN e.DEFICIT = 'Activity limitation' THEN 0.02321018
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 0.1389734
        WHEN e.DEFICIT = 'Asthma' THEN -0.1287534
        WHEN e.DEFICIT = 'Atrial fibrillation' THEN 0.1931007
        WHEN e.DEFICIT = 'Back pain' THEN -0.1427895
        WHEN e.DEFICIT = 'Cancer' THEN 0.4888576
        WHEN e.DEFICIT = 'Chronic kidney disease' THEN 0.06695185
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 0.02429685
        WHEN e.DEFICIT = 'COPD' THEN 0.3242455
        WHEN e.DEFICIT = 'Dementia' THEN 0.6014349
        WHEN e.DEFICIT = 'Diabetes mellitus' THEN 0.05164558
        WHEN e.DEFICIT = 'Dizziness' THEN -0.1321586
        WHEN e.DEFICIT = 'Dressing & grooming problems' THEN -0.1062569
        WHEN e.DEFICIT = 'Environment problems' THEN 0.2018527
        WHEN e.DEFICIT = 'Faecal incontinence' THEN 0.09718586
        WHEN e.DEFICIT = 'Falls' THEN 0.184553
        WHEN e.DEFICIT = 'Fatigue' THEN 0.01952383
        WHEN e.DEFICIT = 'Fracture' THEN -0.005505913
        WHEN e.DEFICIT = 'General mental health' THEN 0.0244545
        WHEN e.DEFICIT = 'Headache' THEN -0.07203948
        WHEN e.DEFICIT = 'Hearing impairment' THEN -0.06095155
        WHEN e.DEFICIT = 'Heart failure' THEN 0.3872531
        WHEN e.DEFICIT = 'Housebound' THEN 0.4515737
        WHEN e.DEFICIT = 'Hypotension / syncope' THEN 0.1484424
        WHEN e.DEFICIT = 'Inflammatory bowel disease' THEN 0.03343195
        WHEN e.DEFICIT = 'Ischaemic heart disease' THEN -0.0298473
        WHEN e.DEFICIT = 'Liver problems' THEN 0.5506584
        WHEN e.DEFICIT = 'Meal preparation problems' THEN -0.001745687
        WHEN e.DEFICIT = 'Medication management problems' THEN 0.01430133
        WHEN e.DEFICIT = 'Memory concerns' THEN 0.3842805
        WHEN e.DEFICIT = 'Mobility problems' THEN 0.07732175
        WHEN e.DEFICIT = 'Mono/hemiparesis' THEN 0.03344356
        WHEN e.DEFICIT = 'Motor neuron disease' THEN 1.616092
        WHEN e.DEFICIT = 'Musculoskeletal problems' THEN -0.2050594
        WHEN e.DEFICIT = 'Osteoarthritis' THEN -0.09374322
        WHEN e.DEFICIT = 'Palliative care' THEN 1.204347
        WHEN e.DEFICIT = 'Parkinsonism & tremor' THEN 0.2550741
        WHEN e.DEFICIT = 'Peptic ulcer disease' THEN 0.08844284
        WHEN e.DEFICIT = 'Peripheral vascular disease' THEN 0.2195116
        WHEN e.DEFICIT = 'Requirement for care' THEN 0.470767
        WHEN e.DEFICIT = 'Respiratory disease' THEN 0.312179
        WHEN e.DEFICIT = 'Seizures' THEN 0.2870367
        WHEN e.DEFICIT = 'Self-harm' THEN 0.5335219
        WHEN e.DEFICIT = 'Shopping problems' THEN -0.3249093
        WHEN e.DEFICIT = 'Skin ulcer' THEN 0.144899
        WHEN e.DEFICIT = 'Stress' THEN 0.03664157
        WHEN e.DEFICIT = 'Stroke' THEN 0.08363783
        WHEN e.DEFICIT = 'Thyroid problems' THEN -0.02108778
        WHEN e.DEFICIT = 'Transient ischaemic attack' THEN -0.005531419
        WHEN e.DEFICIT = 'Urinary system disease' THEN -0.001120175
        WHEN e.DEFICIT = 'Visual impairment' THEN -0.08362421
        WHEN e.DEFICIT = 'Weakness' THEN 0.2187244
        WHEN e.DEFICIT = 'Weight loss' THEN 0.08370275
        -- Special cases for categorical variables
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 0
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN -0.2483266
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN -0.01403189
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Previous higher risk/harmful drinking' THEN 0.3019435
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN 0
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 0.71753  -- BMI underweight
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'BMI normal' THEN 0
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'BMI overweight' THEN -0.3704303
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN -0.374269  -- BMI obese
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN -0.07006635  -- BMI missing
        WHEN e.DEFICIT = 'Smoker (ex)' THEN 0.4681813
        ELSE NULL
    END as EFI_PLUS_MORTALITY_COEFFICIENT,

    -- Home care package model coefficients
    CASE
        WHEN e.DEFICIT = 'Abdominal pain' THEN -0.2454511
        WHEN e.DEFICIT = 'Activity limitation' THEN 0.192214
        WHEN e.DEFICIT = 'Anaemia & haematinic deficiency' THEN 0.22865
        WHEN e.DEFICIT = 'Anxiety' THEN -0.06632726
        WHEN e.DEFICIT = 'Asthma' THEN -0.0182378
        WHEN e.DEFICIT = 'Atrial fibrillation' THEN -0.000631437
        WHEN e.DEFICIT = 'Back pain' THEN -0.0324407
        WHEN e.DEFICIT = 'Bone disease' THEN -0.03401794
        WHEN e.DEFICIT = 'Cancer' THEN 0.3936962
        WHEN e.DEFICIT = 'Cognitive impairment' THEN 0.2369327
        WHEN e.DEFICIT = 'COPD' THEN 0.1447632
        WHEN e.DEFICIT = 'Dementia' THEN 1.10861
        WHEN e.DEFICIT = 'Depression' THEN 0.32152
        WHEN e.DEFICIT = 'Diabetes mellitus' THEN -0.1264826
        WHEN e.DEFICIT = 'Dizziness' THEN -0.08336997
        WHEN e.DEFICIT = 'Dressing & grooming problems' THEN -0.6674991
        WHEN e.DEFICIT = 'Environment problems' THEN 0.2201757
        WHEN e.DEFICIT = 'Faecal incontinence' THEN 0.2061385
        WHEN e.DEFICIT = 'Falls' THEN 0.3061989
        WHEN e.DEFICIT = 'Fatigue' THEN -0.5746051
        WHEN e.DEFICIT = 'Foot problems' THEN 0.1010256
        WHEN e.DEFICIT = 'Fracture' THEN -0.08463036
        WHEN e.DEFICIT = 'Fragility fracture' THEN 0.2048824
        WHEN e.DEFICIT = 'General mental health' THEN 0.2059454
        WHEN e.DEFICIT = 'Headache' THEN -0.05027238
        WHEN e.DEFICIT = 'Hearing impairment' THEN -0.0269063
        WHEN e.DEFICIT = 'Heart failure' THEN 0.2289291
        WHEN e.DEFICIT = 'Housebound' THEN 0.5438867
        WHEN e.DEFICIT = 'Hypotension / syncope' THEN 0.07192635
        WHEN e.DEFICIT = 'Inflammatory bowel disease' THEN -0.1485968
        WHEN e.DEFICIT = 'Ischaemic heart disease' THEN -0.001198194
        WHEN e.DEFICIT = 'Liver problems' THEN 0.7214212
        WHEN e.DEFICIT = 'Meal preparation problems' THEN -0.1889499
        WHEN e.DEFICIT = 'Medication management problems' THEN 0.7216895
        WHEN e.DEFICIT = 'Memory concerns' THEN 1.354686
        WHEN e.DEFICIT = 'Mobility problems' THEN 0.3783051
        WHEN e.DEFICIT = 'Mono/hemiparesis' THEN 0.2121127
        WHEN e.DEFICIT = 'Motor neuron disease' THEN -1.072472
        WHEN e.DEFICIT = 'Musculoskeletal problems' THEN -0.05186372
        WHEN e.DEFICIT = 'Osteoarthritis' THEN -0.08077273
        WHEN e.DEFICIT = 'Osteoporosis' THEN -0.05361352
        WHEN e.DEFICIT = 'Palliative care' THEN 0.4401383
        WHEN e.DEFICIT = 'Parkinsonism & tremor' THEN 0.365633
        WHEN e.DEFICIT = 'Peptic ulcer disease' THEN 0.3554844
        WHEN e.DEFICIT = 'Peripheral vascular disease' THEN 0.126493
        WHEN e.DEFICIT = 'Requirement for care' THEN -0.3526959
        WHEN e.DEFICIT = 'Respiratory disease' THEN 0.09748338
        WHEN e.DEFICIT = 'Seizures' THEN 0.3657592
        WHEN e.DEFICIT = 'Self-harm' THEN 0.196221
        WHEN e.DEFICIT = 'Severe mental illness' THEN 0.1514199
        WHEN e.DEFICIT = 'Shopping problems' THEN -1.916389
        WHEN e.DEFICIT = 'Skin ulcer' THEN 0.1220291
        WHEN e.DEFICIT = 'Social vulnerability' THEN 0.5829034
        WHEN e.DEFICIT = 'Stress' THEN -0.5146241
        WHEN e.DEFICIT = 'Stroke' THEN 0.1374211
        WHEN e.DEFICIT = 'Thyroid problems' THEN 0.1813636
        WHEN e.DEFICIT = 'Transient ischaemic attack' THEN -0.05872412
        WHEN e.DEFICIT = 'Urinary system disease' THEN -0.05466811
        WHEN e.DEFICIT = 'Visual impairment' THEN -0.02215302
        WHEN e.DEFICIT = 'Washing & bathing problems' THEN -1.158266
        WHEN e.DEFICIT = 'Weakness' THEN 0.3609144
        WHEN e.DEFICIT = 'Weight loss' THEN -0.219571
        -- Special cases for categorical variables
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Harmful drinking' THEN 0
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Higher risk drinking' THEN -0.9968457
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Lower risk drinking' THEN -0.5082808
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Previous higher risk/harmful drinking' THEN 1.293511
        WHEN e.DEFICIT = 'Alcohol' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN -0.2459609
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI <18.5%' THEN 0.504693  -- BMI underweight
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'BMI normal' THEN 0
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'BMI overweight' THEN -0.1788759
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS LIKE '%BMI 30+%' THEN -0.285028  -- BMI obese
        WHEN e.DEFICIT = 'Body mass index' AND e.OTHERINSTRUCTIONS = 'Zero alcohol' THEN -1.079132  -- BMI missing
        WHEN e.DEFICIT = 'Smoker (ex)' THEN -0.2507857
        WHEN e.DEFICIT = 'Smoker (current)' THEN 0.1607583
        ELSE NULL
    END as EFI_PLUS_HOME_CARE_COEFFICIENT,

    -- Additional metadata
    e.OTHERINSTRUCTIONS as SOURCE_INSTRUCTION,

    CASE
        WHEN e.DEFICIT IN ('Cognitive impairment', 'Dementia') THEN 1
        WHEN e.DEFICIT IN ('Smoker (current)', 'Smoker (ex)') THEN 2
        WHEN e.DEFICIT = 'Alcohol' THEN 3
        WHEN e.DEFICIT IN ('Hypertension', 'Hypotension / syncope') THEN 4
        ELSE 5
    END as RULE_PRIORITY

FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.EFI2_SNOMED e;
