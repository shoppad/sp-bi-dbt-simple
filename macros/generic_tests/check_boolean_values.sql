-- tests/check_boolean_values.sql
{# Checks that a column has both True and False values. #}
{% macro test_check_boolean_values(model, column_name) %}
    with validation as (
        select
            count(distinct {{ column_name }}) as distinct_boolean_count
        from {{ model }}
        WHERE {{ column_name }} in (True, False) or {{ column_name }} is null
    )
    select *
    from validation
    where distinct_boolean_count < 2
{% endmacro %}
