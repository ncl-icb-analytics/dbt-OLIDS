-- depends_on: {{ ref('stg_flu_code_clusters') }}
-- depends_on: {{ ref('stg_olids_observation') }}
-- depends_on: {{ ref('stg_olids_patient') }}
-- depends_on: {{ ref('stg_olids_patient_person') }}
-- depends_on: {{ ref('stg_olids_term_concept') }}
-- depends_on: {{ ref('stg_codesets_mapped_concepts') }}

/*
Flu Simple Rules Intermediate Model

Handles single cluster eligibility rules for flu vaccination programme.
These rules evaluate one clinical condition using a single code cluster.

Simple rule examples:
- CHD_GROUP: Chronic heart disease diagnosis
- LEARNDIS_GROUP: Learning disability diagnosis  
- CLD_GROUP: Chronic liver disease diagnosis

This model replaces the apply_simple_rule macro functionality.
Uses static configuration to avoid unsafe introspection.
*/

{{ config(
    materialized='table') }}

{%- set current_campaign = var('flu_current_campaign') -%}

{%- set simple_rule_groups = [
    'CHD_GROUP',
    'CLD_GROUP', 
    'CNS_GROUP',
    'ASPLENIA_GROUP',
    'LEARNDIS_GROUP',
    'HHLD_IMDEF_GROUP',
    'AST_ADM_GROUP'
] -%}

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
    {%- for rule_group in simple_rule_groups -%}
        {%- if not loop.first %}
        
        UNION ALL
        {%- endif %}
        
        SELECT 
            '{{ current_campaign }}' AS campaign_id,
            '{{ rule_group }}' AS rule_group_id,
            obs.person_id,
            obs.clinical_effective_date AS event_date,
            obs.cluster_id
        FROM ({{ get_flu_observations_for_rule_group(current_campaign, rule_group) }}) obs
    {%- endfor -%}
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
    SELECT *
    FROM observation_events
    WHERE 
        -- For EARLIEST/LATEST, check event is before the resolved reference date
        (date_qualifier IN ('EARLIEST', 'LATEST') 
         AND event_date <= resolved_reference_date::DATE)
        
        -- For LATEST_SINCE, use the reference_date from config
        OR (date_qualifier = 'LATEST_SINCE' 
            AND event_date >= resolved_reference_date::DATE
            AND event_date <= {{ get_flu_audit_date(current_campaign) }})
        
        -- For LATEST_AFTER, use the reference_date from config  
        OR (date_qualifier = 'LATEST_AFTER'
            AND event_date > resolved_reference_date::DATE
            AND event_date <= {{ get_flu_audit_date(current_campaign) }})
            
        -- Handle any other date qualifiers
        OR (date_qualifier NOT IN ('EARLIEST', 'LATEST', 'LATEST_SINCE', 'LATEST_AFTER'))
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
    demo.birth_date_approx,
    DATEDIFF('month', demo.birth_date_approx, {{ get_flu_audit_date(current_campaign) }}) AS age_months,
    DATEDIFF('year', demo.birth_date_approx, {{ get_flu_audit_date(current_campaign) }}) AS age_years,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM qualified_events qe
LEFT JOIN {{ ref('dim_person_demographics') }} demo
    ON qe.person_id = demo.person_id
WHERE 1=1
    -- Apply age restrictions if specified
    AND (qe.age_min_months IS NULL OR DATEDIFF('month', demo.birth_date_approx, {{ get_flu_audit_date(current_campaign) }}) >= qe.age_min_months)
    AND (qe.age_max_years IS NULL OR DATEDIFF('year', demo.birth_date_approx, {{ get_flu_audit_date(current_campaign) }}) < qe.age_max_years)

ORDER BY rule_group_id, person_id