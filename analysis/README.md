# dbt Analysis Documentation

This directory contains dbt analysis examples showing how to use the `person_month_analysis_base` helper view for common population health analysis patterns. These examples teach users simple patterns for temporal analytics without complex SQL.

## Core Infrastructure

### `int_date_spine` 
A centralised date dimension table providing:
- 10-year monthly spine with all date calculations pre-computed
- UK financial year support (April-March)
- Date components for filtering (year, month, quarter numbers)
- Multiple date formats for reporting
- Eliminates repetitive DATE_TRUNC and DATEADD calculations

### `person_month_analysis_base`
A comprehensive view that combines:
- Active person-months using the date spine
- Complete demographics (with SCD2 temporal logic pre-applied)
- All condition flags (has_* and new_* for all conditions)
- Practice and geographic information
- All date dimensions from `int_date_spine`
- Simple boolean filters for common time periods

This architecture eliminates the need for:
- Complex temporal joins to SCD2 tables
- Date arithmetic in queries
- Monthly snapshot tables
- Subqueries to find current month data

## Analysis Examples

### 1. `practice_analysis_examples.sql`
**Practice Population Patterns**

Simple examples showing how to analyze practice populations:
- Current practice patient counts
- Practice size trends over time
- Demographic breakdowns by practice
- Borough-level comparisons

### 2. `condition_analysis_examples.sql`
**Condition Prevalence Patterns**

Common patterns for analyzing health conditions:
- Current prevalence rates
- Prevalence trends over time
- New cases (incidence) tracking
- Conditions by demographics
- Multi-morbidity analysis
- Comorbidity patterns

### 3. `demographic_analysis_examples.sql`
**Population Demographics Patterns**

Standard demographic analysis patterns:
- Overall population demographics
- Age distribution (population pyramid)
- Ethnicity composition
- Geographic distribution
- Language and interpreter needs
- Population changes over time

### 4. `practice_list_size_trends.sql`
**Extended Practice Analysis**

More comprehensive practice analysis showing advanced patterns using the helper view.

### 5. `ltc_prevalence_simplified_example.sql`
**Long-Term Condition Analysis**

Simplified prevalence analysis for chronic conditions using the helper view.

### 6. `financial_year_reporting_examples.sql`
**UK Financial Year Reporting**

Demonstrates financial year reporting patterns using pre-calculated FY dimensions.

### 7. `simplified_date_filtering_examples.sql`
**Date Filtering Patterns**

Shows all available date filtering options and how they simplify temporal queries.

### 8. `year_over_year_analysis_examples.sql`
**Year-over-Year Analysis**

Comprehensive patterns for comparing metrics across all available years of data, including same-month comparisons, financial year trends, and rolling averages.

## Simple Query Patterns

All examples follow simple patterns using the helper view:

```sql
-- Pattern: Current state analysis (most recent month)
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})

-- Pattern: Trend analysis over time (no date math needed!)
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month >= DATEADD('month', -12, (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }}))
GROUP BY month_year_label, analysis_month

-- Pattern: Financial year reporting
FROM {{ ref('person_month_analysis_base') }}
WHERE financial_year_start = 2023  -- Specific FY
GROUP BY financial_quarter

-- Pattern: Demographic filtering
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }}) 
    AND age_band_nhs = '65-74' 
    AND sex = 'Female' 
    AND practice_borough = 'Camden'

-- Pattern: Year-over-year comparison
SELECT year_number, metric,
       LAG(metric) OVER (ORDER BY year_number) as previous_year_metric
FROM {{ ref('person_month_analysis_base') }}
GROUP BY year_number
```

## Running dbt Analyses

dbt analyses are compiled but not executed by default. To run these analyses:

```bash
# Compile analyses to check syntax
dbt compile --models analysis.*

# Execute a specific analysis in your database client
# Copy the compiled SQL from target/compiled/dbt_olids/analysis/
```

## Key Technical Patterns

### SCD2 Temporal Joins
All modern analyses use proper temporal join patterns:
```sql
FROM fact_table f
INNER JOIN dim_person_demographics_historical d
    ON f.person_id = d.person_id
    AND f.analysis_date >= d.effective_start_date
    AND (d.effective_end_date IS NULL OR f.analysis_date < d.effective_end_date)
```

### Point-in-Time Demographics
Demographics are attributed correctly for the analysis time period, capturing:
- Age at the specific analysis date
- Practice registration valid during that period  
- Ethnicity records active at that time
- Address information current for that period

### Statistical Validity Filters
All analyses include appropriate filters for statistical validity:
- Minimum population sizes for group comparisons
- Exclusion of unknown/declined demographic categories where appropriate
- Sample size thresholds for rate calculations

## Benefits of SCD2 Approach

1. **Accurate Historical Analysis**: Demographics reflect actual values at analysis time
2. **Temporal Integrity**: No anachronistic attribution of changed demographics
3. **Precise Population Counting**: Point-in-time populations without double-counting
4. **Audit Trail**: Full history of demographic changes with effective dates
5. **Performance**: Efficient temporal joins using clustered date columns

## Analytics Best Practices

1. **Always use temporal joins** for historical analysis
2. **Apply statistical validity filters** for meaningful comparisons
3. **Consider denominator populations** carefully for rate calculations
4. **Account for demographic confounders** when comparing groups
5. **Use appropriate time aggregations** (monthly/quarterly) for stability

These analyses demonstrate the analytical power unlocked by proper implementation of slowly changing dimensions for healthcare population analytics.