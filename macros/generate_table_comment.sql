{% macro generate_table_comment() %}
  {%- if execute -%}
    {%- set model_description = model.description or "" -%}
    {%- set current_user = target.user | default('SYSTEM') if (target.user and target.user.strip()) else 'SYSTEM' -%}
    {%- set day = run_started_at.strftime('%d')|int -%}
    {%- set day_suffix = 'th' -%}
    {%- if day % 10 == 1 and day != 11 -%}{%- set day_suffix = 'st' -%}{%- endif -%}
    {%- if day % 10 == 2 and day != 12 -%}{%- set day_suffix = 'nd' -%}{%- endif -%}
    {%- if day % 10 == 3 and day != 13 -%}{%- set day_suffix = 'rd' -%}{%- endif -%}
    {%- set run_timestamp = day|string + day_suffix + run_started_at.strftime(' %B %Y at %H:%M:%S UTC') -%}
    {%- set target_name = target.name | default('unknown') -%}
    
    
    {%- set github_base_url = "https://github.com/ncl-icb-analytics/dbt-olids/blob/main/" -%}
    {%- set model_file_path = model.original_file_path | replace("\\", "/") -%}
    {%- set github_file_url = github_base_url + model_file_path -%}
    
    {%- if model_description -%}
      {%- set clean_description = model.description | replace("'", "''") -%}
      {%- set footer = "

🤖 Last ran on " + run_timestamp + " by " + current_user + " (target: " + target_name + ")
📄 Model source: " + github_file_url + "
📖 Documentation: https://github.com/ncl-icb-analytics/dbt-olids" -%}
      {{- clean_description + footer | replace("'", "''") -}}
    {%- else -%}
🤖 Last ran on {{ run_timestamp }} by {{ current_user }} (target: {{ target_name }})
📄 Model source: {{ github_file_url }}
📖 Documentation: https://github.com/ncl-icb-analytics/dbt-olids
    {%- endif -%}
  {%- endif -%}
{% endmacro %}