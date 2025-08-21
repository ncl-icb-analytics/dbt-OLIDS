{% macro calculate_school_age_flags(birth_date_field, reference_date_field) %}
    -- Primary school age flag (Reception to Year 6): born Sept 2013 to Aug 2020 for 2024-25 academic year
    CASE
        WHEN {{ birth_date_field }} IS NOT NULL 
             AND DATEDIFF(year, {{ birth_date_field }}, {{ reference_date_field }}) BETWEEN 3 AND 12 THEN
            CASE
                WHEN {{ birth_date_field }} >= DATE_FROM_PARTS(
                        CASE 
                            WHEN EXTRACT(MONTH FROM {{ reference_date_field }}) >= 9 
                            THEN EXTRACT(YEAR FROM {{ reference_date_field }}) - 11
                            ELSE EXTRACT(YEAR FROM {{ reference_date_field }}) - 12
                        END, 9, 1
                    )
                    AND {{ birth_date_field }} < DATE_FROM_PARTS(
                        CASE 
                            WHEN EXTRACT(MONTH FROM {{ reference_date_field }}) >= 9 
                            THEN EXTRACT(YEAR FROM {{ reference_date_field }}) - 4  
                            ELSE EXTRACT(YEAR FROM {{ reference_date_field }}) - 5
                        END, 9, 1
                    )
                THEN TRUE
                ELSE FALSE
            END
        ELSE FALSE
    END AS is_primary_school_age,

    -- Secondary school age flag (Year 7 to Year 13): born Sept 2006 to Aug 2013 for 2024-25 academic year  
    CASE
        WHEN {{ birth_date_field }} IS NOT NULL 
             AND DATEDIFF(year, {{ birth_date_field }}, {{ reference_date_field }}) BETWEEN 10 AND 19 THEN
            CASE
                WHEN {{ birth_date_field }} >= DATE_FROM_PARTS(
                        CASE 
                            WHEN EXTRACT(MONTH FROM {{ reference_date_field }}) >= 9 
                            THEN EXTRACT(YEAR FROM {{ reference_date_field }}) - 18
                            ELSE EXTRACT(YEAR FROM {{ reference_date_field }}) - 19
                        END, 9, 1
                    )
                    AND {{ birth_date_field }} < DATE_FROM_PARTS(
                        CASE 
                            WHEN EXTRACT(MONTH FROM {{ reference_date_field }}) >= 9 
                            THEN EXTRACT(YEAR FROM {{ reference_date_field }}) - 11
                            ELSE EXTRACT(YEAR FROM {{ reference_date_field }}) - 12
                        END, 9, 1
                    )
                THEN TRUE
                ELSE FALSE
            END
        ELSE FALSE
    END AS is_secondary_school_age
{% endmacro %}