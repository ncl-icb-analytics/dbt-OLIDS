{% macro get_latest_events(from_table, partition_by='person_id', order_by='clinical_effective_date', direction='DESC') %}
    -- Get latest events for each person from a set of events
    -- Uses Snowflake's QUALIFY statement for efficient filtering of window functions
    -- 
    -- Args:
    --   from_table: The CTE or table name to get events from
    --   partition_by: Column(s) to partition by (default: person_id)
    --   order_by: Column(s) to order by (default: clinical_effective_date)
    --   direction: Sort direction, ASC or DESC (default: DESC)
    
    SELECT *
    FROM {{ from_table }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }} {{ direction }}
    ) = 1

{% endmacro %} 