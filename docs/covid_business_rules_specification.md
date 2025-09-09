# COVID-19 Vaccination Business Rules Specification
## Spring and Autumn Campaigns 2024/25 - 2025/26

Based on UKHSA SARS-CoV2 (COVID-19) Vaccine Uptake Reporting Business Rules v3.5 (27th July 2025)

---

## Campaign Overview

### Reporting Scope
- **Current Year**: 2025/26 (Autumn 2025 + Spring 2026)  
- **Previous Year**: 2024/25 (Autumn 2024 + Spring 2025)
- **Focus**: Eligibility determination and uptake tracking for population health management

### Campaign Periods

#### 2025/26 Campaigns
- **Autumn 2025**: 1 September 2025 - 31 March 2026
- **Spring 2026**: 1 April 2026 - 30 June 2026
- **Age Reference Date**: 31 March 2026 (autumn), 30 June 2026 (spring)

#### 2024/25 Campaigns  
- **Autumn 2024**: 1 September 2024 - 31 March 2025
- **Spring 2025**: 1 April 2025 - 30 June 2025
- **Age Reference Date**: 31 March 2025 (autumn), 30 June 2025 (spring)

---

## Age Rules for Eligibility

### Age-Based Eligibility Criteria (calculated at REF_DAT)
- **Universal Eligibility**: PAT_AGE ≥ 75 at REF_DAT
- **Care Home Eligibility**: PAT_AGE ≥ 65 at REF_DAT (in residential care)
- **Immunosuppressed Eligibility**: PAT_AGE ≥ 6 months AND PAT_AGE < 75 at REF_DAT
- **Morbid Obesity Assessment**: PAT_AGE ≥ 18 at REF_DAT (excluded for under 18s)
- **General Clinical Risk**: PAT_AGE ≥ 5 at REF_DAT (minimum age for vaccination eligibility)

---

## Date Logic and Lookback Periods

### Campaign Start Dates (START_DAT)
```yaml
start_dates:
  autumn_2024: "2024-09-01"
  spring_2025: "2025-04-01" 
  autumn_2025: "2025-09-01"
  spring_2026: "2026-04-01"
```

### Reference Dates (REF_DAT) - for age calculations
```yaml
reference_dates:
  autumn_2024: "2025-03-31"
  spring_2025: "2025-06-30"
  autumn_2025: "2026-03-31" 
  spring_2026: "2026-06-30"
```

### Medication and Treatment Lookback Periods
```yaml
lookback_periods:
  # Immunosuppression
  immunosuppression_medication: "6 months before START_DAT"
  chemotherapy_radiotherapy: "6 months before START_DAT"  
  immunosuppression_admin: "3 years before START_DAT"
  
  # Asthma
  asthma_admission: "2 years before START_DAT (731 days)"
  asthma_inhaled_medication: "1 year before START_DAT (366 days)"
  asthma_oral_steroids: "3 overlapping 2-year windows for multiple prescriptions"
  
  # Vaccination tracking
  vaccination_declined: "1 August before campaign to end of spring period"
  recent_vaccination_exclusion: "84 days before run date"
```

### Asthma Oral Steroid Windows (Complex Rule)
```yaml
steroid_windows:
  window_1:
    earliest: "2023-09-01"  # 2 years before autumn start
    latest: "2025-08-31"    # Up to autumn 2025
    
  window_2:  
    earliest: "2024-04-01"  # 2 years from spring 2024
    latest: "2026-03-31"    # Up to spring 2026
    
  window_3:
    earliest: "2024-07-01"  # Additional window
    latest: "2026-06-30"    # Up to spring 2026 end

# Logic: Patient needs 2+ oral steroid prescriptions within ANY of these windows
# OR prescriptions spanning window boundaries within 731 days
```

---

## Code Cluster Definitions

### Core Vaccination Codes
```yaml
vaccination_codes:
  COVADM_COD:
    description: "COVID vaccine administration codes"
    usage: "Track all COVID vaccinations given"
    
  COVRX_COD: 
    description: "COVID vaccine dm+d codes"
    usage: "Track COVID vaccines via drug codes"
    
  COVDECL_COD:
    description: "COVID vaccination declined codes"
    usage: "Patient declined vaccination"
    
  COVCONTRA_COD:
    description: "COVID vaccination contraindicated codes"  
    usage: "Medical contraindication to vaccination"
```

### Clinical Condition Codes
```yaml
clinical_conditions:
  # Respiratory
  AST_COD:
    description: "Asthma diagnosis codes"
    date_logic: "Earliest ≤ RUN_DAT (ever recorded)"
    
  ASTRXM1_COD:
    description: "Asthma inhaled medication codes (dm+d)"
    date_logic: "Latest ≥ (START_DAT - 366 days)"
    
  ASTRXM2_COD: 
    description: "Asthma oral steroid prescription codes (dm+d)"
    date_logic: "Complex 2-year windows for repeated use"
    
  ASTADM_COD:
    description: "Asthma admission codes"
    date_logic: "Latest ≥ (START_DAT - 731 days)"
    
  RESP_COV_COD:
    description: "Chronic respiratory disease diagnosis codes"
    date_logic: "Earliest ≤ RUN_DAT (ever recorded)"

  # Cardiovascular  
  CHD_COV_COD:
    description: "Chronic heart disease diagnosis codes"
    date_logic: "Earliest ≤ RUN_DAT (ever recorded)"

  # Kidney
  CKD_COV_COD:
    description: "Chronic kidney disease diagnosis codes" 
    date_logic: "Earliest ≤ RUN_DAT (ever recorded)"
    
  CKD15_COD:
    description: "Chronic kidney disease codes – all stages"
    date_logic: "Latest ≤ RUN_DAT"
    
  CKD35_COD:
    description: "Chronic kidney disease codes – stages 3-5"
    date_logic: "Latest ≤ RUN_DAT"

  # Liver
  CLD_COD:
    description: "Chronic liver disease diagnosis codes"
    date_logic: "Earliest ≤ RUN_DAT (ever recorded)"

  # Diabetes
  DIAB_COD:
    description: "Diabetes diagnosis codes"
    date_logic: "Latest ≤ RUN_DAT"
    
  DMRES_COD:
    description: "Diabetes resolved codes"
    date_logic: "Latest ≤ RUN_DAT"
    
  GDIAB_COD:
    description: "Gestational diabetes codes"
    date_logic: "Latest ≥ 14/01/2025 AND ≤ RUN_DAT"
    
  ADDIS_COD:
    description: "Addison's disease & pan-hypopituitarism diagnosis codes"
    date_logic: "Latest ≤ RUN_DAT (ever recorded)"

  # Immunosuppression
  IMMDX_COV_COD:
    description: "Immunosuppression diagnosis codes"
    date_logic: "Latest ≤ RUN_DAT (ever recorded)"
    
  IMMADM_COD:
    description: "Immunosuppression admin codes"
    date_logic: "Latest ≥ (START_DAT - 3 years)"
    
  IMMRX_COD:
    description: "Immunosuppression medication codes (dm+d)"
    date_logic: "Latest ≥ (START_DAT - 6 months)"
    
  DXT_CHEMO_COD:
    description: "Chemotherapy or radiotherapy codes"
    date_logic: "Latest ≥ (START_DAT - 6 months)"

  # Neurological
  CNS_COV_COD:
    description: "Chronic neurological disease diagnosis codes"
    date_logic: "Earliest ≤ RUN_DAT (ever recorded)"
    notes: "Includes significant learning disorder & epilepsy"

  # Other conditions
  SPLN_COV_COD:
    description: "Asplenia or dysfunction of the spleen codes"
    date_logic: "Earliest ≤ RUN_DAT (ever recorded)"
    
  LEARNDIS_COD:
    description: "Learning disability codes"
    date_logic: "Latest ≤ RUN_DAT (ever recorded)"
    
  SEV_MENTAL_COD:
    description: "Severe mental illness codes"
    date_logic: "Latest ≤ RUN_DAT"
    
  SMHRES_COD:
    description: "Resolved severe mental health codes"
    date_logic: "Latest ≤ RUN_DAT"

  # Obesity
  BMI_COD:
    description: "BMI codes"
    date_logic: "Latest ≤ RUN_DAT WHERE BMI_VAL <> NULL"
    
  BMI_STAGE_COD:
    description: "BMI stage codes"
    date_logic: "Latest ≤ RUN_DAT"
    
  SEV_OBESITY_COD:
    description: "Severe obesity codes"
    date_logic: "Most recent of BMI_STAGE_DAT"
```

### Population and Residence Codes
```yaml
population_codes:
  # Pregnancy
  PREG_COD:
    description: "Pregnancy (only) codes"
    date_logic: "Campaign period specific"
    
  PREGDEL_COD:
    description: "Pregnancy or delivery codes"  
    date_logic: "Campaign period specific"

  # Residence
  LONGRES_COD:
    description: "Long term residential care codes"
    date_logic: "Latest ≤ RUN_DAT"
    
  RESIDE_COD:
    description: "Residence codes"
    date_logic: "Latest ≤ RUN_DAT"
    
  HOMELESS_COD:
    description: "Homeless codes"
    date_logic: "Latest ≤ RUN_DAT"
```

---

## Eligibility Group Logic

### 1. Universal Age-Based Eligibility
```yaml
age_75_plus_group:
  logic: "PAT_AGE ≥ 75 at REF_DAT"
  campaigns: ["autumn", "spring"]
  priority: "highest"
  notes: "REF_DAT = 31/03/2026 (autumn) or 30/06/2026 (spring)"
```

### 2. Care Home Residents
```yaml
longres_group:
  logic: |
    IF LONGRES_DAT <> NULL
      Next
    IF LONGRES_DAT ≥ RESIDE_DAT  
      Select
    Reject
  additional_criteria: "PAT_AGE ≥ 65 at REF_DAT for care home specific reporting"
  priority: "highest"
```

### 3. Immunosuppressed Group (Current Campaign)
```yaml
immuno_group:
  logic: |
    IF IMMDX_COV_DAT <> NULL (ever)
      Select
      Next  
    IF IMMRX_DAT <> NULL (6 months before START_DAT)
      Select
      Next
    IF IMMADM_DAT <> NULL (3 years before START_DAT) 
      Select
      Next
    IF DXT_CHEMO_DAT <> NULL (6 months before START_DAT)
      Select
      Reject
  age_criteria: "PAT_AGE ≥ 6 months AND PAT_AGE < 75 at REF_DAT"
  priority: "highest"
```

### 4. Core Clinical At-Risk Group
```yaml
atrisk_group:
  logic: |
    IF IMMUNOGROUP <> NULL: Select, Next
    IF CKD_GROUP <> NULL: Select, Next  
    IF RESP_GROUP <> NULL: Select, Next
    IF DIAB_GROUP <> NULL: Select, Next
    IF CLD_DAT <> NULL: Select, Next
    IF CNS_GROUP <> NULL: Select, Next
    IF CHD_COV_DAT <> NULL: Select, Next
    IF SPLN_COV_DAT <> NULL: Select, Next
    IF LEARNDIS_DAT <> NULL: Select, Next
    IF SEVMENT_GROUP <> NULL: Select, Reject
  notes: "BMI_GROUP handled separately to avoid circular logic"
```

### 5. Chronic Kidney Disease Group
```yaml
ckd_group:
  logic: |
    IF CKD_COV_DAT <> NULL (diagnoses): Select, Next
    IF CKD15_DAT = NULL (no stages): Reject, Next  
    IF CKD35_DAT ≥ CKD15_DAT (latest stage is 3-5): Select, Reject
  notes: "Must have diagnosis OR stage 3-5 codes"
```

### 6. Asthma Group (Complex Logic)
```yaml
ast_group:
  logic: |
    IF ASTADM_DAT <> NULL (admission in last 2 years): Select, Next
    IF AST_DAT <> NULL (diagnosis ever): Next, Reject
    IF ASTRXM1_DAT <> NULL (inhaled medication last 12 months): Next, Reject
    
    # Oral steroid logic - 2 prescriptions in any 2-year window
    IF (ASTRXM2E1_DAT ≠ ASTRXM2L1_DAT) AND (ASTRXM2E1_DAT + 731 DAYS > ASTRXM2L1_DAT): Select, Next
    IF (ASTRXM2E2_DAT ≠ ASTRXM2L2_DAT) AND (ASTRXM2E2_DAT + 731 DAYS > ASTRXM2L2_DAT): Select, Next  
    IF (ASTRXM2E3_DAT ≠ ASTRXM2L3_DAT) AND (ASTRXM2E3_DAT + 731 DAYS > ASTRXM2L3_DAT): Select, Next
    IF (ASTRXM2L1_DAT ≠ ASTRXM2E2_DAT) AND (ASTRXM2L1_DAT + 731 DAYS > ASTRXM2E2_DAT): Select, Next
    IF (ASTRXM2L2_DAT ≠ ASTRXM2E3_DAT) AND (ASTRXM2L2_DAT + 731 DAYS > ASTRXM2E3_DAT): Select, Reject
  
  steroid_windows:
    window_1: "1/9/2023 to 1/9/2025" 
    window_2: "1/4/2024 to 1/4/2026"
    window_3: "1/7/2024 to 1/7/2026"
```

### 7. Respiratory Group  
```yaml
resp_group:
  logic: |
    IF AST_GROUP <> NULL: Select, Next
    IF RESP_COV_DAT <> NULL: Select, Reject
```

### 8. Diabetes Group
```yaml
diab_group:
  logic: |
    IF ADDIS_DAT <> NULL: Select, Next
    IF GDIAB_GROUP <> NULL: Select, Next  
    IF DIAB_DAT = NULL: Reject, Next
    IF DIAB_DAT > DMRES_DAT: Select, Reject
  notes: "Resolved diabetes excluded unless more recent diagnosis"
```

### 9. Severe Mental Health Group
```yaml
sevment_group:
  logic: |
    IF SEV_MENTAL_DAT > SMHRES_DAT: Select, Reject
  notes: "Resolved conditions excluded"
```

### 10. Morbid Obesity Group
```yaml
bmi_group:
  logic: |
    IF PAT_AGE < 18 at REF_DAT: Reject, Next
    IF SEV_OBESITY_DAT > BMI_DAT OR (SEV_OBESITY_DAT <> NULL AND BMI_DAT = NULL): Select, Next
    IF BMI_DAT ≥ BMI_STAGE_DAT AND BMI_VAL ≥ 40: Select, Next  
    IF BMI_STAGE_DAT = NULL AND BMI_VAL ≥ 40: Select, Reject
  notes: "Adults 18+ only, BMI ≥40 threshold"
```

### 11. Pregnancy Groups
```yaml
# Current pregnancy determination
preg25_group:
  logic: |
    IF PREG25B_DAT <> NULL (pregnancy/delivery 1/9/25-30/6/26): Select, Next
    IF PREGDEL25_DAT <> NULL (pregnancy/delivery 1/1/25-31/8/25) AND 
       PREG25A_DAT <> NULL AND PREG25A_DAT ≥ PREGDEL25_DAT: Select, Reject
  notes: "Determines if currently pregnant during campaign period"

# Gestational diabetes  
gdiab_group:
  logic: |
    IF GDIAB_DAT <> NULL: Next, Reject
    IF PREG25_GROUP <> NULL: Select, Reject
  notes: "Must have gestational diabetes AND be currently pregnant"
```

### 12. Homeless Group
```yaml
homeless_group:
  logic: |
    IF HOMELESS_DAT <> NULL: Next, Reject
    IF HOMELESS_DAT ≥ RESIDE_DAT: Select, Reject
  notes: "Latest residence status must be homeless"
```

---

## Vaccination Status Logic

### Vaccinated in Campaign Period
```yaml
vaccinated_autumn:
  logic: "COVADM1_DAT <> NULL OR COVRX1_DAT <> NULL"
  date_range: "01/09/2025 to 31/03/2026"
  
vaccinated_spring:  
  logic: "COVADM2_DAT <> NULL OR COVRX2_DAT <> NULL"
  date_range: "01/04/2026 to 30/06/2026"
```

### Vaccination Exclusions
```yaml
declined_vaccination:
  logic: "COVDECL_DAT <> NULL"
  date_range: "01/08/2025 to 30/06/2026"
  
contraindicated:
  logic: "COVCONTRA_DAT <> NULL"  
  date_range: "Any time ≤ RUN_DAT"
  
recently_vaccinated:
  logic: "L_DOSE_DAT > (RUN_DAT - 84 DAYS)"
  usage: "Exclude from recall/outreach"
```

---

## Target Outputs

### COVID Eligibility Table
**Columns:**
- `person_id`
- `campaign_id` (covid_2024_autumn, covid_2025_spring, covid_2025_autumn, covid_2026_spring)  
- `campaign_year` (2024/25, 2025/26)
- `campaign_period` (autumn, spring)
- `eligible` (boolean)
- `eligibility_reason` (age_75_plus, care_home, immunosuppressed, clinical_risk)
- `age_at_reference_date`
- Individual eligibility flags for each condition (boolean columns)

### COVID Status Table  
**Columns:**
- `person_id`
- `campaign_id`
- `eligible` (boolean)
- `vaccinated` (boolean)  
- `vaccination_date`
- `declined` (boolean)
- `contraindicated` (boolean)
- `status` (eligible_vaccinated, eligible_unvaccinated, eligible_declined, etc.)

### Key Benefits
- Single row per person per campaign for easy PowerBI consumption
- All eligibility logic pre-calculated
- Clean vaccination status determination
- Year-on-year comparison ready (2024/25 vs 2025/26)
- Campaign period comparison (autumn vs spring)

---

## Implementation Validation Rules

### Data Quality Checks
1. **Age calculations**: Verify using correct REF_DAT for each campaign
2. **Date logic**: Ensure START_DAT used for medication lookbacks
3. **Lookback periods**: Validate 366 days = 1 year, 731 days = 2 years
4. **Code hierarchies**: Check ATRISK_GROUP excludes BMI_GROUP to avoid circular logic
5. **Pregnancy logic**: Verify current pregnancy determination using delivery dates

### Business Rule Validation
1. **Asthma logic**: Test complex oral steroid window calculations
2. **CKD staging**: Ensure stage 3-5 requirement when only stage codes present  
3. **Resolved conditions**: Check diabetes and mental health resolution logic
4. **Immunosuppression**: Validate different lookback periods for different code types
5. **Residence status**: Confirm latest residence code determines care home/homeless status

This specification provides the complete business logic foundation for implementing COVID vaccination eligibility and uptake reporting focused on spring and autumn campaigns for this year and last year.