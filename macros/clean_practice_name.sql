{% macro clean_practice_name(organisation_name_field, fallback_field) %}
    {% set medical_acronyms = [
        'GP', 'NHS', 'PMS', 'HESA', 'PHGH', 'H/C', 'CCG', 'ICB', 'PCN', 'LP'
    ] %}
    
    -- Use organisation_name (preserve existing casing) or convert fallback to proper case
    {% set base_name = 'COALESCE(' + organisation_name_field + ', INITCAP(' + fallback_field + '))' %}
    {% set cleaned_name = 'TRIM(' + base_name + ", '.')" %}
    
    {% for acronym in medical_acronyms %}
        {% set cleaned_name = "REGEXP_REPLACE(" + cleaned_name + ", '\\\\b" + acronym + "\\\\b', '" + acronym + "', 'gi')" %}
    {% endfor %}
    
    {{ cleaned_name }}
{% endmacro %}