{% macro groomed_column_list(relation_object, except=[]) %}
    {{ return(dbt_utils.get_filtered_columns_in_relation(from=relation_object, except=(except + ['group'] + var('etl_fields')))) }}
{% endmacro %}
