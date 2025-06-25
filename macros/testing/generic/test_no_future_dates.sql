{% test test_no_future_dates(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} > CURRENT_DATE
{% endtest %}
