{% test relationships_proportion(model, column_name, field, to) %}

    {{ config(fail_calc = 'missing_proportion') }}

    WITH
    table_size_calc AS (
        SELECT COUNT(*) as total_size
        FROM {{ model }}
        WHERE {{ column_name }} IS NOT NULL
    ),

    child as (
        SELECT {{ column_name }} as from_field
        FROM {{ model }}
        WHERE {{ column_name }} is not null
    ),

    parent as (
        SELECT {{ field }} AS to_field
        FROM {{ to }}
    ),

    missing_parents AS (
        select
            ROUND(COUNT(*) / (SELECT * FROM table_size_calc) * 100) AS missing_proportion
        FROM child
        LEFT JOIN parent
            ON child.from_field = parent.to_field
        WHERE parent.to_field IS NULL
    )

    SELECT *
    FROM missing_parents


{% endtest %}
