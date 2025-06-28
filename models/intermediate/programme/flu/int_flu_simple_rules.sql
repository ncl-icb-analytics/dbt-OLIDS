/*
Flu Simple Rules Intermediate Model

Handles single cluster eligibility rules for flu vaccination programme.
These rules evaluate one clinical condition using a single code cluster.

Simple rule examples:
- CHD_GROUP: Chronic heart disease diagnosis
- LEARNDIS_GROUP: Learning disability diagnosis  
- CLD_GROUP: Chronic liver disease diagnosis

This model replaces the apply_simple_rule macro functionality.
Uses dynamic cluster queries from configuration data.
*/

{{ config(materialized='table') }}

{%- set current_campaign = var('flu_current_campaign') -%}

WITH simple_rules AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        cluster_id,
        data_source_type,
        date_qualifier,
        resolved_reference_date,
        age_min_months,
        age_max_years,
        description
    FROM {{ ref('stg_flu_programme_rules') }}
    WHERE rule_type = 'SIMPLE'
        AND campaign_id = '{{ current_campaign }}'
        AND cluster_id IS NOT NULL
),

-- Get all observation-based events for SIMPLE rules
all_observation_events AS (
    {%- set obs_rules_query -%}
        SELECT DISTINCT rule_group_id 
        FROM {{ ref('stg_flu_programme_logic') }}
        WHERE rule_type = 'SIMPLE' 
          AND campaign_id = '{{ current_campaign }}'
    {%- endset -%}
    
    {%- if execute -%}
        {%- set obs_rule_results = run_query(obs_rules_query) -%}
        {%- for row in obs_rule_results.rows -%}
            {%- set rule_group = row[0] -%}
            
            SELECT 
                '{{ current_campaign }}' AS campaign_id,
                '{{ rule_group }}' AS rule_group_id,
                obs.person_id,
                obs.clinical_effective_date AS event_date,
                obs.cluster_id
            FROM ({{ get_flu_observations_for_rule_group(current_campaign, rule_group) }}) obs
            
            {%- if not loop.last -%}
            UNION ALL
            {%- endif -%}
        {%- endfor -%}
    {%- else -%}
        -- Compile-time placeholder
        SELECT 
            '{{ current_campaign }}' AS campaign_id,
            'PLACEHOLDER' AS rule_group_id,
            'placeholder_person' AS person_id,
            CURRENT_DATE AS event_date,
            'PLACEHOLDER_CLUSTER' AS cluster_id
        WHERE FALSE
    {%- endif -%}
),

-- Join with rule configuration
observation_events AS (
    SELECT 
        r.campaign_id,
        r.rule_group_id,
        r.rule_group_name,
        r.cluster_id,
        r.date_qualifier,
        r.resolved_reference_date,
        r.age_min_months,
        r.age_max_years,
        r.description,
        obs.person_id,
        obs.event_date
    FROM simple_rules r
    JOIN all_observation_events obs
        ON r.campaign_id = obs.campaign_id
        AND r.rule_group_id = obs.rule_group_id
        AND r.cluster_id = obs.cluster_id
    WHERE r.data_source_type = 'observation'
),

-- Apply date filtering based on date_qualifier
filtered_events AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        event_date,
        date_qualifier,
        resolved_reference_date,
        age_min_months,
        age_max_years,
        description
    FROM observation_events
    WHERE 1=1
        -- Apply date filters based on qualifier
        AND CASE 
            WHEN date_qualifier = 'EARLIEST' THEN event_date <= {{ get_flu_audit_date(current_campaign) }}
            WHEN date_qualifier = 'LATEST' THEN event_date <= {{ get_flu_audit_date(current_campaign) }}
            WHEN date_qualifier = 'LATEST_SINCE' AND resolved_reference_date != 'PARAMETER' 
                THEN event_date >= resolved_reference_date
            WHEN date_qualifier = 'LATEST_AFTER' AND resolved_reference_date != 'PARAMETER' 
                THEN event_date > resolved_reference_date
            ELSE TRUE
        END
),

-- Get the qualifying event per person (earliest or latest based on qualifier)
qualified_events AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        description,
        resolved_reference_date,
        age_min_months,
        age_max_years,
        CASE 
            WHEN date_qualifier = 'EARLIEST' THEN MIN(event_date)
            ELSE MAX(event_date)
        END AS qualifying_event_date
    FROM filtered_events
    GROUP BY 
        campaign_id, rule_group_id, rule_group_name, person_id, 
        description, resolved_reference_date, age_min_months, 
        age_max_years, date_qualifier
)

-- Apply age restrictions if specified
SELECT 
    qe.campaign_id,
    qe.rule_group_id,
    qe.rule_group_name,
    qe.person_id,
    qe.qualifying_event_date,
    qe.resolved_reference_date AS reference_date,
    qe.description,
    demo.birth_date,
    DATEDIFF('month', demo.birth_date, {{ get_flu_audit_date(current_campaign) }}) AS age_months,
    DATEDIFF('year', demo.birth_date, {{ get_flu_audit_date(current_campaign) }}) AS age_years,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM qualified_events qe
LEFT JOIN {{ ref('dim_person_demographics') }} demo
    ON qe.person_id = demo.person_id
WHERE 1=1
    -- Apply age restrictions if specified
    AND (qe.age_min_months IS NULL OR DATEDIFF('month', demo.birth_date, {{ get_flu_audit_date(current_campaign) }}) >= qe.age_min_months)
    AND (qe.age_max_years IS NULL OR DATEDIFF('year', demo.birth_date, {{ get_flu_audit_date(current_campaign) }}) < qe.age_max_years)

ORDER BY rule_group_id, person_id