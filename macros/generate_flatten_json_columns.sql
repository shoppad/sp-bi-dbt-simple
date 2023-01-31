{%- macro generate_flatten_json_columns(model_name, json_column) -%}

{%- set get_json_path -%}

{# /* get json keys and paths with the FLATTEN function supported by Snowflake */ #}
WITH low_level_flatten AS (
	SELECT f.key AS json_key, f.path AS json_path,
	f.value AS json_value
	FROM {{ model_name }},
	LATERAL FLATTEN(INPUT => {{ json_column }}, RECURSIVE => TRUE ) f
)

	{# /* get the unique and flattest paths  */ #}
	{# /* you could modify the function to determine the level of nested JSON  */ #}
	SELECT DISTINCT lower(json_path)
	FROM low_level_flatten
	WHERE NOT contains(json_value, '{')

{%- endset -%}

{# /* the value in the column will be the attributes of you exploded result  */ #}
{%- set res = run_query(get_json_path) -%}
{%- if execute -%}
    {%- set res_list = res.columns[0].values() -%}
{%- else -%}
    {%- set res_list = [] -%}
{%- endif -%}

{# /* explode JSON columns and format the column names  */ #}
{% for json_path in res_list %}
	{{ json_column }}:{{ json_path }} AS {{ json_path | replace("-","_") | replace(".", "_") | replace("[", "_") | replace("]", "") | replace("'", "") }}{{ ", " if not loop.last else "" }}
{% endfor %}

{% endmacro %}
