{% test cluster_ids_exist(model, cluster_ids=none, arguments=none) %}
    -- Generic test to verify that specified cluster IDs exist in codesets
    {%- if arguments and arguments.cluster_ids -%}
        {%- set cluster_ids = arguments.cluster_ids -%}
    {%- endif -%}
    
    WITH required_clusters AS (
        SELECT TRIM(value) AS cluster_id
        FROM TABLE(SPLIT_TO_TABLE(
            '{{ cluster_ids }}',
            ','
        ))
    )
    SELECT
        rc.cluster_id,
        'Cluster ID not found in codesets_combined_codesets' AS failure_reason
    FROM required_clusters rc
    WHERE rc.cluster_id NOT IN (
        SELECT DISTINCT cluster_id
        FROM {{ ref('stg_codesets_combined_codesets') }}
    )
{% endtest %}
