{% macro filter_by_date(date_column, months_back=none, years_back=none, start_date=none, end_date=none) %}



    -- Apply date filtering based on provided parameters
    -- At least one parameter must be provided
    {% if months_back is none and years_back is none and start_date is none and end_date is none %}
    {{ exceptions.raise_compiler_error("Must provide at least one date filter parameter to filter_by_date macro (months_back, years_back, start_date, or end_date)") }}
    {% endif %}

    {% if months_back is not none %}
        {{ date_column }} >= DATEADD(month, -{{ months_back }}, CURRENT_DATE())
    {% elif years_back is not none %}
        {{ date_column }} >= DATEADD(year, -{{ years_back }}, CURRENT_DATE())
    {% elif start_date is not none and end_date is not none %}
        {{ date_column }} BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    {% elif start_date is not none %}
        {{ date_column }} >= '{{ start_date }}'
    {% elif end_date is not none %}
    {{ date_column }} <= '{{ end_date }}'
    {% endif %}
{% endmacro %}
