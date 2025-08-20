# Analysis Files: get_observations Macro vs Native SQL Comparison

This directory contains analysis files comparing the `get_observations` macro with equivalent native SQL joins using the `combined_codesets` table.

## Files Overview

### 1. `macro_vs_native_comparison_ast_cod.sql`
**Purpose**: Compare results for asthma codes (AST_COD)
- Tests practice-level aggregation
- Validates person counts match between approaches
- Provides difference analysis and status indicators

### 2. `macro_vs_native_comparison_diab_cod.sql` 
**Purpose**: Compare results for diabetes codes (DIAB_COD)
- Includes date range validation (earliest/latest dates)
- Tests both count and temporal consistency
- More comprehensive validation than AST_COD test

### 3. `macro_vs_native_performance_test.sql`
**Purpose**: Performance and scalability comparison
- Tests multiple clusters simultaneously (AST_COD, DIAB_COD, RESP_COD)
- Compares execution approaches for bulk analysis
- Measures observations per person ratios

### 4. `code_mapping_validation.sql`
**Purpose**: Validate code mapping consistency
- Compares actual SNOMED codes used by both approaches
- Identifies missing or extra codes in either method
- Validates concept displays and descriptions match

### 5. `deduplication_analysis.sql`
**Purpose**: Understand deduplication behavior
- Analyzes how the macro's QUALIFY clause works
- Shows before/after deduplication statistics
- Identifies observations with multiple code mappings

## Usage Instructions

### Running the Analysis
```sql
-- Run individual comparisons
dbt compile --select analysis.macro_vs_native_comparison_ast_cod
-- Then execute the compiled SQL in your query tool

-- Or run all analysis files
dbt compile --select analysis.*
```

### Expected Outcomes

**Perfect Match Scenarios:**
- Person counts should be identical between macro and native approaches
- Date ranges should match exactly
- Code mappings should be consistent

**Acceptable Differences:**
- Minor timing differences due to materialized view refresh
- Small count variations (±1-2) due to data processing timing

**Concerning Differences:**
- Large person count discrepancies (>5%)
- Missing codes in either approach
- Significant date range differences

## Key Validation Points

### 1. **Data Accuracy**
```sql
-- Both methods should return same person counts
WHERE macro_count = native_count
```

### 2. **Code Coverage**
```sql
-- No codes should be missing from either approach
WHERE status = 'Missing from macro' OR status = 'Missing from native'
```

### 3. **Deduplication Effectiveness**
```sql
-- Duplicate percentage should be reasonable (typically <5%)
WHERE duplicate_percentage > 5
```

### 4. **Performance Characteristics**
- Macro approach: Better for single clusters, leverages materialized views
- Native approach: More efficient for multi-cluster queries

## Database Dependencies

These analyses require access to:
- **OLIDS tables**: `observation`, `patient_person`, `concept_map`, `concept`
- **Reference tables**: `combined_codesets` 
- **dbt models**: `dim_person_demographics`, `int_mapped_concepts`, etc.

## Interpretation Guide

### Status Indicators
- `✓ Perfect match`: Identical results
- `⚠ Minor difference`: Small acceptable variance (≤5 people)
- `❌ Significant difference`: Requires investigation

### Performance Metrics
- **Total observations**: Raw observation count
- **Unique persons**: Distinct people with the condition
- **Avg obs per person**: Indicates data richness/duplication

### Common Issues to Investigate
1. **Materialized view lag**: Macro uses `_mv` tables that may be stale
2. **Patient linking differences**: Different patient-person mapping approaches
3. **Date filtering**: Subtle differences in NULL handling or date ranges
4. **Concept mapping**: Changes in code mappings between reference tables

## Next Steps

After running these analyses:
1. **Review discrepancies** in the comparison files
2. **Investigate performance** differences for your use cases
3. **Validate code mappings** match your expectations
4. **Document any systematic differences** found
5. **Consider using findings** to optimize the macro or native approaches