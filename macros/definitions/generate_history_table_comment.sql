{% macro generate_history_table_comment(table_name, description) %}
  {%- if execute -%}
    {%- set current_user = target.user | default('SYSTEM') if (target.user and target.user.strip()) else 'SYSTEM' -%}
    {%- set day = run_started_at.strftime('%d')|int -%}
    {%- set day_suffix = 'th' -%}
    {%- if day % 10 == 1 and day != 11 -%}{%- set day_suffix = 'st' -%}{%- endif -%}
    {%- if day % 10 == 2 and day != 12 -%}{%- set day_suffix = 'nd' -%}{%- endif -%}
    {%- if day % 10 == 3 and day != 13 -%}{%- set day_suffix = 'rd' -%}{%- endif -%}
    {%- set run_timestamp = day|string + day_suffix + run_started_at.strftime(' %B %Y at %H:%M:%S UTC') -%}
    {%- set target_name = target.name | default('unknown') -%}
    
    {#- Reuse same format as generate_table_comment but adapt for history tables -#}
    {%- set clean_description = description | replace("'", "''") -%}
    {%- set footer = "

🤖 Last ran on " + run_timestamp + " by " + current_user + " (target: " + target_name + ")
📄 History table for: " + table_name + "
📖 Documentation: https://github.com/ncl-icb-analytics/dbt-olids" -%}
    {{- clean_description + footer | replace("'", "''") -}}
  {%- endif -%}
{% endmacro %}