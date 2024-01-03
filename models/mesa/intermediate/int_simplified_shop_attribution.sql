with unified_columns AS (
    {% set excluded_prefixes = ['ga_', 'segment_', 'mesa_install_', 'app_store_'] %}
    {% set table_name = ref("int_shop_install_sources") %}
    {% set column_names = dbt_utils.get_filtered_columns_in_relation(table_name, except=['shop_subdomain']) %}
    {# {{ log("Debug: column_names = " ~ column_names, info=True) }} #}
    SELECT
        shop_subdomain,
        {% for column_name in column_names %}
            {% set column_name = column_name|lower %}
            {% set result = [] %}
            {{ should_exclude_column_by_prefix(column_name, excluded_prefixes, result) }}
            {% if result|length == 0 %}
                {{ column_name }} AS acq_{{ column_name }}
                {%- if not loop.last %},{% endif %}
            {% endif %}
        {% endfor %},
        app_store_has_ad_click AS acq_app_store_has_ad_click,
        app_store_has_organic_click AS acq_app_store_has_organic_click,
        app_store_click_type AS acq_app_store_click_type
    FROM {{ table_name }}
)

SELECT
    unified_columns.*
FROM unified_columns
