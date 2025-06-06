{% macro get_person_ids(cte_name) %}
    -- Add person_id and sk_patient_id through patient mapping
    -- Takes a CTE name as input and expects it to have a patient_id column
    SELECT 
        base.*,
        pp.person_id,
        p.sk_patient_id
    FROM {{ cte_name }} base
    JOIN {{ ref('stg_olids_patient') }} p
        ON base.patient_id = p.id
    JOIN {{ ref('stg_olids_patient_person') }} pp
        ON p.id = pp.patient_id
{% endmacro %} 