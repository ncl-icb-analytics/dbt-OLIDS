# DBT Test Failure Summary - Updated After Model Build

**Total Tests:** 2168  
**Results:** PASS=2087 | WARN=8 | ERROR=73 | SKIP=0

## 🎉 Significant Improvement!
- **Errors reduced from 323 to 73** (77% reduction!)
- **Pass rate improved from 85% to 96%**
- Missing table errors resolved ✅

## Error Categories

### 1. Schema/Column Issues (Invalid Identifier) - 20 Failures

#### Missing Columns in Models
- **CYP Asthma Models** (4 failures): Missing `CLINICAL_EFFECTIVE_DATE` and `SOURCE` columns
- **LTC LCS CF Models** (13 failures): Missing columns like `NEEDS_STATIN_REVIEW`, `HAS_NO_RISK_FACTORS`, `LATEST_BP_CODE`, `LATEST_BP_DISPLAY`, `HAS_STAGE_1_HYPERTENSION_WITH_RF`, `PRACTICE_CODE`
- **Geography Models** (3 failures): Column naming issues with `imd_decile_19`, `imd_quintile_19`, `years_since_first_occupation`, `age`

### 2. SQL Syntax & Logic Issues - 8 Failures

#### Aggregate Function Errors
- **Geography Models** (4 failures): Invalid use of `COUNT(*)` in WHERE clauses
- **Practice Organisation Models** (2 failures): Similar aggregate function issues
- **Custom Test Logic** (2 failures): Syntax errors in cluster ID existence tests

### 3. Data Quality Issues (Not Null Violations) - 19 Failures

#### Person Demographics & Identifiers  
- **Person dimension models**: 5028 null `sk_patient_id` records in multiple demographic tables
- **Active patients**: 823 null records for `latest_record_date` and `sk_patient_id`
- **Practice registrations**: 4 null `person_id` records

#### Clinical Data Quality
- **LTC summary**: 2 null `earliest_diagnosis_date` records
- **NDH diabetes register**: 2 records with null diagnosis status
- **BMI observations**: 76 null `original_result_value` records  
- **Spirometry**: 1 null `original_result_value` record
- **CYP asthma observations**: 19 null `clinical_effective_date` records

#### Pregnancy Risk Assessment
- **Clinical dates**: 4 null `clinical_effective_date` records
- **Concept mapping**: 33 null `concept_display` and `source_cluster_id` records

### 4. Missing Reference Data - 7 Failures

#### Cluster ID Validation
- **Diagnosis models**: Missing cluster IDs for SMI (2), RA (1), Stroke/TIA (2) conditions
- **NHS Health Check**: Missing `NHSHEALTHCHECK_COD` cluster (1)
- **Spirometry**: Missing `UNABLESPI_COD` cluster (1)
- **Urine ACR**: Missing `ACR_COD` cluster (1)

### 5. Uniqueness & Duplicate Issues - 3 Failures

#### Data Duplication
- **Medication orders**: 9 duplicate `medication_order_id` records in inhaled corticosteroids
- **LTC LCS Summary**: 1 duplicate `person_id` 
- **Person combinations**: Duplicate person/medication combinations

### 6. Data Range Issues - 2 Failures

#### Age & BMI Outliers
- **Child-bearing age**: 406 women outside expected age range (15-44)
- **BMI latest**: 1 BMI value outside acceptable range (10-80)

## Warnings (8 Total)

### Data Range Warnings
- **HbA1c Values**: 17 records outside acceptable range (200-20) in diabetes measures
- **BMI Values**: Outlier values in multiple models (13-4 records)
- **Blood Pressure Categories**: 3 unexpected hypertension stage values
- **Creatinine Values**: 28 records outside acceptable range (1500-10)
- **Obesity BMI**: 1 record outside acceptable range (100-10)

## Analysis & Next Steps

### Priority 1: Schema & Column Issues (20 failures)
1. **CYP Asthma Models**: Add missing `CLINICAL_EFFECTIVE_DATE` and `SOURCE` columns
2. **LTC LCS CF Models**: Review model definitions for missing calculated fields
3. **Geography Models**: Fix column naming/expression syntax in tests

### Priority 2: SQL Logic Fixes (8 failures)  
1. **Aggregate Functions**: Rewrite tests using `HAVING` instead of `WHERE` with `COUNT(*)`
2. **Custom Tests**: Fix syntax errors in cluster ID validation logic

### Priority 3: Data Quality Investigation (19 failures)
1. **Person Demographics**: Investigate 5028 null `sk_patient_id` records - potential upstream join issue
2. **Clinical Data**: Review BMI observation quality (76 null values)
3. **Reference Data**: Add missing cluster IDs to `COMBINED_CODESETS` table

### Priority 4: Data Integrity (3 failures)
1. **Medication Duplicates**: Investigate 9 duplicate medication orders
2. **LTC Summary**: Review business logic for person-level summaries

### Quick Wins Available
- ✅ **Model Dependencies**: RESOLVED! All tables now exist
- 🔧 **Test Syntax**: Many failures are SQL syntax issues, easily fixable
- 📊 **Data Validation**: Most data quality issues are in small record counts

### Investigation Queries Available
Test failure data is stored in `DATA_LAB_NCL_TRAINING_TEMP.DBT_DEV_test_audit` schema for detailed investigation.

### Success Metrics
- 🎯 **Target**: Reduce errors from 73 to <20 (97%+ pass rate)
- 📈 **Progress**: Already achieved 77% error reduction after model build 