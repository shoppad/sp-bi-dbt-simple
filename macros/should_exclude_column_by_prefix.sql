{% macro should_exclude_column_by_prefix(column_name, excluded_prefixes, result) %}
    {% for prefix in excluded_prefixes %}
        {% if column_name.startswith(prefix) %}
            {# {{ log('Debug: excluding column ' ~ column_name ~ ' because it starts with ' ~ prefix, info=True) }} #}
            {% set _ = result.append(true) %}
            {% break %}
        {% endif %}
    {% endfor %}
{% endmacro %}
