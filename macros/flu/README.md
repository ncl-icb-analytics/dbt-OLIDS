# Flu Campaign Macros

## Overview
This directory contains macros for managing flu vaccination campaign eligibility rules. The macros provide a flexible, campaign-specific approach to determining patient eligibility based on various clinical criteria.

## Unsafe Introspection Fix (December 2024)

### Problem
The original implementation used `run_query()` within `if execute` blocks to dynamically fetch configuration data from staging tables during compilation. This caused dbt1000 warnings about unsafe introspection because:
- Models queried the database during the parse phase
- SQL generation depended on the current state of staging tables
- This violated dbt's principle of deterministic SQL generation

### Solution
Replaced dynamic database queries with static configuration mappings in the macros. The configuration data from the seed files is now hardcoded as Jinja dictionaries within the macros.

### Implementation Details

1. **`get_flu_campaign_config()`** - New macro containing all campaign configurations as a static dictionary
   - Dates for each campaign and rule group
   - Cluster mappings for each rule group
   - Eliminates need for database queries during compilation

2. **`get_flu_clusters_for_rule_group()`** - Refactored to use static config instead of `run_query()`
   - Reads from the static configuration dictionary
   - Returns cluster IDs based on campaign, rule group, and data source type

3. **`get_flu_campaign_date()`** - Refactored to use static config instead of `run_query()`
   - Reads dates from the static configuration
   - Supports fallback to 'ALL' rule group dates

4. **`get_flu_rule_config()`** - Refactored to use static config instead of `run_query()`
   - Contains all rule configurations (type, logic, age limits, etc.)
   - Returns configuration for specific campaign and rule group

### Maintenance
When adding new campaigns or updating configurations:
1. Update the seed CSV files as usual
2. Update the static configurations in `get_flu_campaign_config()` and `get_flu_rule_config()`
3. The dual maintenance ensures data consistency while avoiding unsafe introspection

### Benefits
- Eliminates dbt1000 warnings
- Improves compilation performance (no database queries)
- Enables static analysis of models
- Maintains the same functionality as before

## Main Macros

### `get_flu_clusters_for_rule_group(campaign_id, rule_group_id, data_source_type=none)`
Returns a comma-separated list of cluster IDs for a specific rule group and campaign.

### `get_flu_campaign_date(campaign_id, rule_group_id, date_type)`
Returns a specific date for a campaign and rule group (e.g., start_dat, ref_dat, latest_since_date).

### `get_flu_audit_date(campaign_id=none)`
Returns the audit end date, either from a variable or the default CURRENT_DATE.

### `get_flu_observations_for_rule_group(campaign_id, rule_group_id, source='UKHSA_FLU')`
Returns observations for a specific rule group, using the appropriate cluster IDs.

### `get_flu_medications_for_rule_group(campaign_id, rule_group_id, source='UKHSA_FLU')`
Returns medication orders for a specific rule group, using the appropriate cluster IDs.

### `get_flu_rule_config(campaign_id, rule_group_id)`
Returns the complete rule configuration including type, logic expression, age limits, and description.