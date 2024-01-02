{% macro get_prefixed_columns(relation_ref, prefix, exclude=[]) %}

    {%- set column_names = dbt_utils.get_filtered_columns_in_relation(
            from=relation_ref,
            except=['shop_subdomain', 'user_pseudo_id'] + exclude
        )
    -%}

    {%- for column_name in column_names -%}
        {%- if column_name|lower == 'event_timestamp_pt' or column_name|lower == 'created_at' -%}
            {{ column_name }} AS  {{ prefix }}_at_pt
        {%- else -%}
            iff(
                {{ column_name }}::varchar = '', null, {{ column_name }}
            ) AS {{ prefix }}_{{ column_name|lower }}
        {%- endif -%}
        {%- if not loop.last %},{% endif %}
    {% endfor -%}

{% endmacro %}
