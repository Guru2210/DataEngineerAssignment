{% test assert_date_in_range(model, column_name, min_date='2000-01-01', max_date='2099-12-31') %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < '{{ min_date }}' OR {{ column_name }} > '{{ max_date }}'
{% endtest %}
