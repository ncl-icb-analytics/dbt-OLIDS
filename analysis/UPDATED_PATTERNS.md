# Updated Analysis Patterns

After simplifying the `person_month_analysis_base` view, all analysis examples now use clear, explicit date filtering instead of confusing boolean flags. The view also now only includes months where patients were actually registered, preventing empty months from appearing in results.

## Key Changes Made

### Removed Confusing Boolean Flags
- ❌ `is_current_month = TRUE`
- ❌ `is_last_12_months = TRUE`  
- ❌ `is_current_financial_year = TRUE`

### Replaced With Clear Date Filtering
- ✅ `analysis_month = (SELECT MAX(analysis_month) FROM person_month_analysis_base)` for latest month with data
- ✅ `analysis_month >= DATEADD('month', -12, CURRENT_DATE)` for date ranges
- ✅ `financial_year_start = 2023` for specific financial year
- ✅ `year_number = 2024 AND month_number >= 4` for year/month filtering

### Improved Data Quality
- ✅ Only includes months where patients were actually registered
- ✅ No empty months in results when filtering to recent periods
- ✅ Prevents future months from appearing in the data

## Available Date Columns

| Column | Type | Purpose | Example |
|--------|------|---------|---------|
| `analysis_month` | DATE | Direct date comparisons | `>= '2023-04-01'` |
| `year_number` | INTEGER | Year filtering | `= 2024` |
| `month_number` | INTEGER | Month filtering | `BETWEEN 4 AND 9` |
| `quarter_number` | INTEGER | Quarter filtering | `IN (1, 2)` |
| `month_year_label` | VARCHAR | Display | 'MAR 2024' |
| `financial_year` | VARCHAR | FY display | '2023/24' |
| `financial_year_start` | INTEGER | FY filtering | `= 2023` |
| `financial_quarter` | VARCHAR | FY quarter display | 'Q1' |
| `financial_quarter_number` | INTEGER | FY quarter filtering | `IN (3, 4)` |

## Common Filtering Patterns

```sql
-- Latest month with data (more reliable than CURRENT_DATE)
WHERE analysis_month = (SELECT MAX(analysis_month) FROM person_month_analysis_base)

-- Last 12 months trend
WHERE analysis_month >= DATEADD('month', -12, CURRENT_DATE)

-- Specific financial year  
WHERE financial_year_start = 2023

-- Current financial year
WHERE financial_year_start = CASE 
    WHEN MONTH(CURRENT_DATE) >= 4 THEN YEAR(CURRENT_DATE) 
    ELSE YEAR(CURRENT_DATE) - 1 
END

-- Specific months in a year
WHERE year_number = 2024 AND month_number BETWEEN 4 AND 9

-- Financial year quarters
WHERE financial_year_start = 2023 
    AND financial_quarter_number IN (3, 4)
```

## Updated Analysis Files

All analysis files have been updated to use these clear patterns:

1. **demographic_analysis_examples.sql** - Population demographics
2. **condition_analysis_examples.sql** - Disease prevalence  
3. **practice_analysis_examples.sql** - Practice populations
4. **practice_list_size_trends.sql** - Practice trends
5. **financial_year_reporting_examples.sql** - FY reporting
6. **ltc_prevalence_simplified_example.sql** - LTC analysis
7. **simplified_date_filtering_examples.sql** - Date filtering guide

The result is much clearer, more explicit SQL that analysts can easily understand and modify.