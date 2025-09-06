# Indicator Definition Field Requirements

This document outlines the complete field requirements for defining indicators in the dbt OLIDS project. These fields are used by the `extract_indicator_metadata()` macro to generate the indicator definitions tables.

## Core Indicator Fields

### Mandatory Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `id` | String | Unique identifier for the indicator | `"BRF_BMI_ETHNIC"`, `"DM017"`, `"NG136_BP"` |
| `type` | String | High-level indicator classification | `"BRF"`, `"CONDITION"`, `"MEASURE"` |
| `category` | String | Indicator category for grouping | `"BRF"`, `"LTC"`, `"CARDIOVASCULAR"` |
| `name_short` | String | Brief display name | `"BMI with Ethnicity Adjustment"` |
| `description_short` | String | One-line description | `"BMI categories with ethnicity-adjusted thresholds"` |
| `description_long` | String | Detailed definition (use `>` for multi-line) | Full clinical definition with context |
| `source_column` | String | Column name in source model containing the indicator | `"bmi_category"`, `"is_on_register"` |

### Optional Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `clinical_domain` | String | Medical specialty or clinical area | `"Lifestyle"`, `"Metabolic"`, `"Hypertension"` |
| `is_qof` | Boolean | Whether this is a QOF indicator | `true`, `false` (default) |
| `qof_indicator` | String | QOF code if `is_qof: true` | `"DM017"` |
| `sort_order` | String | Custom sort key (auto-generated if not provided) | `"BRF_1_BMI"`, `"COND_DIABET_017"` |

## Related Data Arrays

### Usage Contexts
Array of where the indicator is used:

```yaml
usage_contexts:
  - "POPULATION_HEALTH_NEEDS_DASHBOARD"
  - "QOF"
  - "NICE_GUIDANCE"
```

### Code Clusters
Array of SNOMED code clusters used by the indicator:

```yaml
code_clusters:
  - cluster_id: "BMI_COD"
    category: "INCLUSION"
  - cluster_id: "ETH2016AI_COD"
    category: "REFINEMENT"
```

**Code Categories:**
- `INCLUSION` - Codes that identify patients for the indicator
- `EXCLUSION` - Codes that exclude patients from the indicator  
- `RESOLUTION` - Codes that indicate condition resolution
- `REFINEMENT` - Codes that modify how the indicator is applied
- `CALCULATION` - Codes used as inputs for calculating the indicator value

### Thresholds
Array of clinical thresholds for the indicator:

```yaml
thresholds:
  - population_group: "STANDARD"
    threshold_type: "OVERWEIGHT"
    threshold_value: "25"
    threshold_operator: "AT_OR_ABOVE"
    threshold_unit: "kg/m²"
    description: "Overweight threshold for standard populations"
    sort_order: 1
```

**Threshold Fields:**
- `population_group` - Target population (e.g., `"ALL"`, `"AGE_LT_80"`, `"T2DM"`)
- `threshold_type` - Type of threshold (e.g., `"OVERWEIGHT"`, `"TARGET_UPPER"`, `"HIGH_RISK"`)
- `threshold_value` - Threshold value as string
- `threshold_operator` - Comparison operator (`"AT_OR_ABOVE"`, `"BELOW"`, `"EQUALS"`, `"BETWEEN"`)
- `threshold_unit` - Unit of measurement (e.g., `"kg/m²"`, `"mmHg"`, `"score"`, `"status"`)
- `description` - Human-readable description
- `sort_order` - Numeric sort key

## Complete Example

```yaml
config:
  meta:
    indicator:
      id: "BRF_BMI_ETHNIC"
      type: "BRF"
      category: "BRF"
      clinical_domain: "Lifestyle"
      name_short: "BMI with Ethnicity Adjustment"
      description_short: "BMI categories with ethnicity-adjusted thresholds for cardiometabolic risk"
      description_long: >
        BMI categorisation using ethnicity-adjusted thresholds per NICE guidance.
        Standard populations use conventional BMI categories (overweight ≥25, obese ≥30).
        Populations with increased cardiometabolic risk use lower thresholds 
        (overweight ≥23, obese ≥27.5) to reflect increased risk at lower BMI levels.
      is_qof: false
      source_column: "bmi_category"
      sort_order: "BRF_1_BMI"
      usage_contexts:
        - "POPULATION_HEALTH_NEEDS_DASHBOARD"
      code_clusters:
        - cluster_id: "BMI_COD"
          category: "INCLUSION"
        - cluster_id: "ETH2016AI_COD"
          category: "REFINEMENT"
      thresholds:
        - population_group: "STANDARD"
          threshold_type: "OVERWEIGHT"
          threshold_value: "25"
          threshold_operator: "AT_OR_ABOVE"
          threshold_unit: "kg/m²"
          description: "Overweight threshold for standard populations"
          sort_order: 1
```

## Validation Notes

1. **ID Patterns**: Use consistent naming patterns:
   - BRF indicators: `BRF_[FACTOR]_[VARIANT]`
   - Conditions: `[QOF_CODE]` or `COND_[DOMAIN]_[ID]`
   - Measures: `[GUIDELINE]_[MEASURE]`

2. **Sort Order**: Auto-generated patterns:
   - BRF: `BRF_1_BMI`, `BRF_2_SMOKING`, `BRF_3_ALCOHOL`
   - Conditions: `COND_[DOMAIN]_[ID]`
   - Others: `[TYPE]_[NAME]`

3. **Required Relationships**: All indicators must have:
   - At least one usage context
   - At least one code cluster with INCLUSION category
   - Source column that exists in the model

4. **QOF Indicators**: If `is_qof: true`, must also provide `qof_indicator` field

## Generated Tables

This metadata feeds into these reference tables:
- `def_indicator` - Core indicator definitions
- `def_indicator_usage` - Usage contexts
- `def_indicator_codes` - Code cluster mappings  
- `def_indicator_thresholds` - Clinical thresholds

With corresponding history tables for change tracking.