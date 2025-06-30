/*
Flu Campaign Utility Macros

Helper macros for dynamic flu campaign configuration and data retrieval.
These macros support the campaign-specific model approach while maintaining DRY principles.
*/

-- Get cluster IDs for a specific rule group and campaign
{% macro get_flu_clusters_for_rule_group(campaign_id, rule_group_id, data_source_type=none) %}
    {%- set cluster_query -%}
        SELECT DISTINCT cluster_id 
        FROM {{ ref('stg_flu_code_clusters') }} clusters
        JOIN {{ ref('stg_flu_programme_logic') }} logic
            ON clusters.rule_group_id = logic.rule_group_id
        WHERE logic.campaign_id = '{{ campaign_id }}'
          AND logic.rule_group_id = '{{ rule_group_id }}'
          {%- if data_source_type %}
          AND clusters.data_source_type = '{{ data_source_type }}'
          {%- endif %}
        ORDER BY cluster_id
    {%- endset -%}
    
    {%- if execute -%}
        {%- set cluster_results = run_query(cluster_query) -%}
        {%- set cluster_list = [] -%}
        {%- for row in cluster_results.rows -%}
            {%- do cluster_list.append(row[0]) -%}
        {%- endfor -%}
        {{- "'" ~ cluster_list|join("','") ~ "'" -}}
    {%- else -%}
        {{- "'PLACEHOLDER_CLUSTER'" -}}
    {%- endif -%}
{% endmacro %}

-- Get campaign dates for a specific campaign and rule group
{% macro get_flu_campaign_date(campaign_id, rule_group_id, date_type) %}
    {%- set date_query -%}
        SELECT date_value
        FROM {{ ref('stg_flu_campaign_dates') }}
        WHERE campaign_id = '{{ campaign_id }}'
          AND rule_group_id = '{{ rule_group_id }}'
          AND date_type = '{{ date_type }}'
        LIMIT 1
    {%- endset -%}
    
    {%- if execute -%}
        {%- set date_results = run_query(date_query) -%}
        {%- if date_results.rows|length > 0 -%}
            {{- "'" ~ date_results.rows[0][0] ~ "'" -}}
        {%- else -%}
            -- Try fallback to 'ALL' rule group
            {%- set fallback_query -%}
                SELECT date_value
                FROM {{ ref('stg_flu_campaign_dates') }}
                WHERE campaign_id = '{{ campaign_id }}'
                  AND rule_group_id = 'ALL'
                  AND date_type = '{{ date_type }}'
                LIMIT 1
            {%- endset -%}
            {%- set fallback_results = run_query(fallback_query) -%}
            {%- if fallback_results.rows|length > 0 -%}
                {{- "'" ~ fallback_results.rows[0][0] ~ "'" -}}
            {%- else -%}
                {{- "NULL" -}}
            {%- endif -%}
        {%- endif -%}
    {%- else -%}
        {{- "'2024-09-01'" -}}
    {%- endif -%}
{% endmacro %}

-- Get audit end date (from variable or campaign config)
{% macro get_flu_audit_date(campaign_id=none) %}
    {%- set audit_date = var('flu_audit_end_date', 'CURRENT_DATE') -%}
    {%- if audit_date == 'CURRENT_DATE' -%}
        CURRENT_DATE
    {%- else -%}
        '{{ audit_date }}'
    {%- endif -%}
{% endmacro %}

-- Get observations for a rule group (replaces hardcoded cluster lists)
{% macro get_flu_observations_for_rule_group(campaign_id, rule_group_id, source='UKHSA_FLU') %}
    {%- set clusters = get_flu_clusters_for_rule_group(campaign_id, rule_group_id, 'observation') -%}
    SELECT person_id, clinical_effective_date, cluster_id
    FROM ({{ get_observations(clusters, source) }})
    WHERE clinical_effective_date IS NOT NULL
{% endmacro %}

-- Get medications for a rule group (replaces hardcoded cluster lists)
{% macro get_flu_medications_for_rule_group(campaign_id, rule_group_id, source='UKHSA_FLU') %}
    {%- set clusters = get_flu_clusters_for_rule_group(campaign_id, rule_group_id, 'medication') -%}
    SELECT person_id, order_date, cluster_id
    FROM ({{ get_medication_orders(cluster_id=clusters, source=source) }})
    WHERE order_date IS NOT NULL
{% endmacro %}

-- Get rule configuration for a specific rule group
{% macro get_flu_rule_config(campaign_id, rule_group_id) %}
    {%- set config_query -%}
        SELECT 
            rule_type,
            logic_expression,
            exclusion_groups,
            age_min_months,
            age_max_years,
            description
        FROM {{ ref('stg_flu_programme_logic') }}
        WHERE campaign_id = '{{ campaign_id }}'
          AND rule_group_id = '{{ rule_group_id }}'
        LIMIT 1
    {%- endset -%}
    
    {%- if execute -%}
        {%- set config_results = run_query(config_query) -%}
        {%- if config_results.rows|length > 0 -%}
            {%- set config = config_results.rows[0] -%}
            {{- {
                'rule_type': config[0],
                'logic_expression': config[1], 
                'exclusion_groups': config[2],
                'age_min_months': config[3],
                'age_max_years': config[4],
                'description': config[5]
            } -}}
        {%- else -%}
            {{- {} -}}
        {%- endif -%}
    {%- else -%}
        {{- {'rule_type': 'SIMPLE', 'description': 'Placeholder'} -}}
    {%- endif -%}
{% endmacro %}