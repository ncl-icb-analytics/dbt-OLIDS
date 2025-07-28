/*
Flu Rule Components - Composable Building Blocks

This file contains atomic, reusable components for building flu eligibility rules.
Each component represents a single, testable piece of business logic.

Components use the core get_observations and get_medication_orders macros directly,
working with the existing code clusters (e.g., 'CKD_COD', 'CKD15_COD').
*/

-- Component: Check if person has a diagnosis code
{% macro flu_has_diagnosis(cluster_id, date_qualifier='EARLIEST', reference_date=none, source='UKHSA_FLU') %}
    SELECT 
        person_id,
        clinical_effective_date AS event_date,
        '{{ cluster_id }}' AS evidence_type
    FROM ({{ get_observations(cluster_id, source) }})
    WHERE clinical_effective_date IS NOT NULL
    {%- if date_qualifier == 'EARLIEST' %}
        -- Keep all records, will take MIN later
    {%- elif date_qualifier == 'LATEST' %}
        -- Keep all records, will take MAX later
    {%- elif date_qualifier == 'LATEST_SINCE' and reference_date %}
        AND clinical_effective_date >= {{ reference_date }}
    {%- elif date_qualifier == 'LATEST_AFTER' and reference_date %}
        AND clinical_effective_date > {{ reference_date }}
    {%- endif %}
{% endmacro %}

-- Component: Check if person has medication orders
{% macro flu_has_medication(cluster_id, since_date=none, after_date=none, source='UKHSA_FLU') %}
    SELECT 
        person_id,
        order_date AS event_date,
        '{{ cluster_id }}' AS evidence_type
    FROM ({{ get_medication_orders(cluster_id=cluster_id, source=source) }})
    WHERE order_date IS NOT NULL
    {%- if since_date %}
        AND order_date >= {{ since_date }}
    {%- elif after_date %}
        AND order_date > {{ after_date }}
    {%- endif %}
{% endmacro %}

-- Component: Check if person is within age range
{% macro flu_age_between(min_months=none, max_years=none, reference_date='CURRENT_DATE') %}
    SELECT 
        person_id,
        birth_date_approx,
        DATEDIFF('month', birth_date_approx, {{ reference_date }}) AS age_months,
        DATEDIFF('year', birth_date_approx, {{ reference_date }}) AS age_years
    FROM {{ ref('dim_person_demographics') }}
    WHERE 1=1
    {%- if min_months %}
        AND DATEDIFF('month', birth_date_approx, {{ reference_date }}) >= {{ min_months }}
    {%- endif %}
    {%- if max_years %}
        AND DATEDIFF('year', birth_date_approx, {{ reference_date }}) < {{ max_years }}
    {%- endif %}
{% endmacro %}

-- Component: Check if birth date is within range
{% macro flu_birth_date_between(start_date, end_date) %}
    SELECT 
        person_id,
        birth_date_approx
    FROM {{ ref('dim_person_demographics') }}
    WHERE birth_date_approx BETWEEN {{ start_date }} AND {{ end_date }}
{% endmacro %}

-- Component: Get latest code from a set of observation codes
{% macro flu_latest_code_from_set(cluster_ids, reference_date='CURRENT_DATE', source='UKHSA_FLU') %}
    WITH all_codes AS (
        {%- for cluster_id in cluster_ids %}
        {%- if not loop.first %}UNION ALL{% endif %}
        SELECT 
            person_id,
            clinical_effective_date,
            '{{ cluster_id }}' AS cluster_id
        FROM ({{ get_observations(cluster_id, source) }})
        WHERE clinical_effective_date IS NOT NULL
            AND clinical_effective_date <= {{ reference_date }}
        {%- endfor %}
    )
    SELECT 
        person_id,
        cluster_id AS latest_cluster_id,
        clinical_effective_date AS latest_date
    FROM (
        SELECT 
            person_id,
            cluster_id,
            clinical_effective_date,
            ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) AS rn
        FROM all_codes
    )
    WHERE rn = 1
{% endmacro %}

-- Component: Check if one code is more recent than another
{% macro flu_code_more_recent_than(code_a_cluster, code_b_cluster, reference_date='CURRENT_DATE', source='UKHSA_FLU') %}
    WITH code_a_dates AS (
        SELECT 
            person_id,
            MAX(clinical_effective_date) AS latest_date
        FROM ({{ get_observations(code_a_cluster, source) }})
        WHERE clinical_effective_date IS NOT NULL
            AND clinical_effective_date <= {{ reference_date }}
        GROUP BY person_id
    ),
    code_b_dates AS (
        SELECT 
            person_id,
            MAX(clinical_effective_date) AS latest_date
        FROM ({{ get_observations(code_b_cluster, source) }})
        WHERE clinical_effective_date IS NOT NULL
            AND clinical_effective_date <= {{ reference_date }}
        GROUP BY person_id
    )
    SELECT 
        COALESCE(a.person_id, b.person_id) AS person_id,
        a.latest_date AS code_a_date,
        b.latest_date AS code_b_date,
        CASE 
            WHEN a.latest_date IS NOT NULL AND (b.latest_date IS NULL OR a.latest_date >= b.latest_date)
            THEN TRUE
            ELSE FALSE
        END AS code_a_more_recent
    FROM code_a_dates a
    FULL OUTER JOIN code_b_dates b
        ON a.person_id = b.person_id
{% endmacro %}

-- Component: Check BMI value
{% macro flu_has_bmi_over(threshold, reference_date='CURRENT_DATE', source='UKHSA_FLU') %}
    SELECT 
        person_id,
        clinical_effective_date,
        result_value AS bmi_value
    FROM ({{ get_observations('BMI_COD', source) }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ reference_date }}
        AND result_value IS NOT NULL
        AND CAST(result_value AS FLOAT) >= {{ threshold }}
{% endmacro %}

-- Component: Combine evidence from multiple sources
{% macro flu_combine_evidence(ctes, combination_logic='OR') %}
    {%- if combination_logic == 'OR' %}
    SELECT DISTINCT person_id, event_date, evidence_type
    FROM (
        {%- for cte in ctes %}
        {%- if not loop.first %}UNION ALL{% endif %}
        SELECT person_id, event_date, evidence_type FROM {{ cte }}
        {%- endfor %}
    )
    {%- elif combination_logic == 'AND' %}
    -- For AND logic, person must appear in all CTEs
    WITH person_counts AS (
        SELECT person_id, COUNT(DISTINCT evidence_type) AS evidence_count
        FROM (
            {%- for cte in ctes %}
            {%- if not loop.first %}UNION ALL{% endif %}
            SELECT DISTINCT person_id, evidence_type FROM {{ cte }}
            {%- endfor %}
        )
        GROUP BY person_id
    )
    SELECT DISTINCT 
        e.person_id,
        e.event_date,
        e.evidence_type
    FROM (
        {%- for cte in ctes %}
        {%- if not loop.first %}UNION ALL{% endif %}
        SELECT person_id, event_date, evidence_type FROM {{ cte }}
        {%- endfor %}
    ) e
    JOIN person_counts pc ON e.person_id = pc.person_id
    WHERE pc.evidence_count = {{ ctes|length }}
    {%- endif %}
{% endmacro %}

-- Component: Apply date qualifier logic
{% macro flu_apply_date_qualifier(cte_name, date_qualifier='LATEST') %}
    SELECT 
        person_id,
        {%- if date_qualifier == 'EARLIEST' %}
        MIN(event_date) AS qualifying_date
        {%- else %}
        MAX(event_date) AS qualifying_date
        {%- endif %},
        evidence_type
    FROM {{ cte_name }}
    GROUP BY person_id, evidence_type
{% endmacro %}