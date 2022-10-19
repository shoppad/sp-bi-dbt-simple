{% macro groomed_column_list(relation_object, columns_to_skip=[]) %}
    {% set hevo_columns = ['__HEVO__DATABASE_NAME', '__HEVO__INGESTED_AT', '__HEVO__LOADED_AT', '__HEVO__MARKED_DELETED', '__HEVO_ID'] %}
    {{ dbt_utils.get_filtered_columns_in_relation(from=relation_object, except=columns_to_skip + hevo_columns) | join(",\n")  }}
{% endmacro %}