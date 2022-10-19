{# FROM https://dm03514.medium.com/beware-of-dbt-incremental-updates-against-snowflake-external-tables-beeda513e748 #}
{% macro get_max_updated_at() %}
    {% if execute and is_incremental() %}
        {% set query %}
            SELECT max(updated_at) FROM {{ this }};
        {% endset %}
        {% set max_updated_at = run_query(query).columns[0][0] %}
        {% do return(max_updated_at) %}
    {% endif %}
{% endmacro %}