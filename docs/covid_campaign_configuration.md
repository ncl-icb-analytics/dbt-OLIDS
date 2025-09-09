# COVID-19 Vaccination Campaign Configuration
## Business Rules Documentation 2025/26

This document defines the configuration variables and eligibility criteria for COVID-19 vaccination campaigns, extracted from UKHSA SARS-CoV2 Business Rules v3.5 (27th July 2025).

---

## Campaign Configuration

### Supported Campaigns

#### COVID Autumn 2025 Campaign
- **Campaign ID**: `covid_2025_autumn`
- **Period**: 1st September 2025 - 31st March 2026
- **Reference Date**: 31st March 2026
- **Target Groups**: Age 75+, Care home residents 65+, Immunosuppressed 6 months-74 years

#### COVID Spring 2026 Campaign  
- **Campaign ID**: `covid_2026_spring`
- **Period**: 1st April 2026 - 30th June 2026
- **Reference Date**: 30th June 2026
- **Target Groups**: Same as autumn campaign

---

## Configuration Variables

### Core Campaign Dates
```yaml
# In dbt_project.yml
vars:
  covid_current_campaign: "covid_2025_autumn"
  covid_previous_campaign: "covid_2024_spring"  # For comparison
```

### Date Configuration Details

#### Autumn 2025 Campaign
- `campaign_start_date`: 2025-09-01
- `campaign_end_date`: 2026-03-31
- `campaign_reference_date`: 2026-03-31 (for age calculations)
- `vaccination_tracking_start`: 2025-09-01
- `vaccination_tracking_end`: 2026-03-31

#### Spring 2026 Campaign
- `campaign_start_date`: 2026-04-01
- `campaign_end_date`: 2026-06-30
- `campaign_reference_date`: 2026-06-30 (for age calculations)
- `vaccination_tracking_start`: 2026-04-01
- `vaccination_tracking_end`: 2026-06-30

### Lookback Period Configuration
```yaml
# Medication and treatment lookback periods
immunosuppression_medication_months: 6
chemotherapy_radiotherapy_months: 6
immunosuppression_admin_years: 3

# Condition-specific lookbacks
asthma_admission_years: 2
asthma_inhaled_medication_months: 12
asthma_oral_steroid_window_years: 2

# Pregnancy tracking
pregnancy_tracking_start: 2025-09-01
pregnancy_tracking_end: 2026-06-30
gestational_diabetes_start: 2025-01-14

# Vaccination decline tracking
decline_tracking_start: 2025-08-01
decline_tracking_end: 2026-06-30
recent_vaccination_exclusion_days: 84
```

---

## Eligibility Group Configuration

### Primary Eligibility (Autumn 2025)

#### 1. Age-Based Universal Eligibility
```yaml
age_75_plus:
  min_age: 75
  reference_date_field: "campaign_reference_date"
  universal_eligibility: true
```

#### 2. Care Home Residents
```yaml
care_home_residents:
  min_age: 65
  reference_date_field: "campaign_reference_date"
  requires_residence_code: "LONGRES_COD"
  priority_level: "high"
```

#### 3. Immunosuppressed Patients
```yaml
immunosuppressed:
  min_age_months: 6
  max_age: 74
  reference_date_field: "campaign_reference_date"
  criteria:
    - diagnosis_codes: "IMMDX_COV_COD" # Any time
    - medication_codes: "IMMRX_COD" # Last 6 months
    - treatment_codes: "DXT_CHEMO_COD" # Last 6 months  
    - admin_codes: "IMMADM_COD" # Last 3 years
  priority_level: "highest"
```

### Clinical Risk Groups (At-Risk Eligibility)

#### 4. Chronic Kidney Disease
```yaml
chronic_kidney_disease:
  diagnosis_codes: "CKD_COV_COD"
  stage_codes: "CKD15_COD"
  qualifying_stages: "CKD35_COD" # Stages 3-5 only
  logic: "diagnosis_any_time OR (stage_codes AND latest_stage_3_to_5)"
```

#### 5. Chronic Respiratory Disease
```yaml
chronic_respiratory_disease:
  includes_asthma: true
  asthma_criteria:
    emergency_admission:
      codes: "ASTADM_COD"
      lookback_years: 2
      sufficient_alone: true
    active_asthma:
      diagnosis_codes: "AST_COD" # Ever
      inhaled_medication: "ASTRXM1_COD" # Last 12 months
      oral_steroids: "ASTRXM2_COD" # 2+ prescriptions in any 2-year window
  other_respiratory:
    diagnosis_codes: "RESP_COV_COD"
```

#### 6. Diabetes
```yaml
diabetes:
  type_1_2_codes: "DIAB_COD"
  resolved_codes: "DMRES_COD"
  gestational_codes: "GDIAB_COD"
  addisons_codes: "ADDIS_COD"
  logic: "latest_diabetes > latest_resolved OR gestational_current OR addisons_any"
```

#### 7. Other Chronic Conditions
```yaml
chronic_heart_disease:
  codes: "CHD_COV_COD"
  any_time: true

chronic_liver_disease:
  codes: "CLD_COD"
  any_time: true

chronic_neurological_disease:
  codes: "CNS_COV_COD"
  includes: ["stroke", "tia", "cerebral_palsy", "ms", "epilepsy", "learning_disabilities"]
  any_time: true

asplenia:
  codes: "SPLN_COV_COD"
  any_time: true

learning_disabilities:
  codes: "LEARNDIS_COD" 
  any_time: true

severe_mental_illness:
  codes: "SEV_MENTAL_COD"
  resolved_codes: "SMHRES_COD"
  logic: "latest_diagnosis > latest_resolved"
```

#### 8. Morbid Obesity
```yaml
morbid_obesity:
  min_age: 18
  bmi_threshold: 40
  bmi_codes: "BMI_COD"
  stage_codes: "SEV_OBESITY_COD"
  logic: "(latest_bmi_value >= 40) OR (latest_stage_code = severe_obesity AND stage_date >= bmi_date)"
```

### Special Populations

#### 9. Pregnancy
```yaml
pregnancy:
  pregnancy_codes: "PREG_COD"
  delivery_codes: "PREGDEL_COD"
  tracking_period: 
    start: "pregnancy_tracking_start"
    end: "pregnancy_tracking_end"
  current_pregnancy_logic: "pregnancy_or_delivery_in_period AND (pregnancy_date >= delivery_date OR no_delivery_after_pregnancy)"
  
gestational_diabetes:
  codes: "GDIAB_COD"
  requires_current_pregnancy: true
  start_date: "gestational_diabetes_start"
```

#### 10. Vulnerable Populations
```yaml
long_term_residential_care:
  codes: "LONGRES_COD"
  residence_codes: "RESIDE_COD"
  logic: "latest_residence_code IN longres_codes"

homelessness:
  codes: "HOMELESS_COD" 
  residence_codes: "RESIDE_COD"
  logic: "latest_residence_code IN homeless_codes"
```

---

## Vaccination Status Configuration

### Vaccination Tracking
```yaml
vaccination_administration:
  codes: "COVADM_COD"
  dm_d_codes: "COVRX_COD"
  autumn_period: ["2025-09-01", "2026-03-31"]
  spring_period: ["2026-04-01", "2026-06-30"]

vaccination_declined:
  codes: "COVDECL_COD"
  tracking_period: ["2025-08-01", "2026-06-30"]

vaccination_contraindicated:
  codes: "COVCONTRA_COD"
  any_time: true

recent_vaccination_exclusion:
  lookback_days: 84
  from_date: "run_date"
```

---

## Priority Stratification

### Risk-Based Prioritisation
```yaml
priority_levels:
  highest:
    - age_75_plus
    - immunosuppressed_current
    - care_home_residents_65_plus
  
  high:  
    - chronic_kidney_disease_stage_3_5
    - active_immunosuppression
    - severe_respiratory_disease_with_admission
    - multiple_chronic_conditions
  
  standard:
    - single_chronic_condition
    - controlled_chronic_conditions  
    - pregnancy_any_risk_status
  
  exclusions:
    - recent_vaccination_within_84_days
    - documented_contraindication
    - declined_current_campaign_period
```

### Outreach Configuration
```yaml
call_recall_logic:
  eligible_for_recall:
    - meets_eligibility_criteria
    - not_recently_vaccinated
    - not_contraindicated
    - not_declined_current_period
  
  review_for_vaccination:
    - has_contraindication_code
    - not_recently_vaccinated
    - meets_eligibility_criteria
  
  exclude_from_outreach:
    - vaccinated_current_campaign
    - recent_vaccination_within_84_days
    - declined_current_campaign
```

---

## Implementation Notes

### Age Calculations
- Use `campaign_reference_date` for all age-based eligibility
- Age 75+ calculated as age >= 75 years on reference date
- Immunosuppressed group: 6 months to 74 years inclusive

### Date Logic Patterns  
- **"Ever" conditions**: No date restriction (diabetes, heart disease)
- **Recent medications**: 6-month lookback from campaign start
- **Historical admissions**: 2-year lookback for asthma
- **Admin codes**: 3-year lookback for immunosuppression status

### Multi-Campaign Support
- All models should support both autumn and spring campaigns
- Use campaign variables to switch between periods
- Maintain consistent eligibility logic across campaigns

### Hierarchy Rules
1. Check universal age eligibility first (75+)
2. Check special populations (care homes, immunosuppressed)  
3. Check clinical risk groups for remaining patients
4. Apply exclusions (recent vaccination, contraindications, declined)

This configuration provides the foundation for implementing COVID vaccination eligibility and population health management in the dbt models.