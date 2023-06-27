{%- set source_table = ref('stg_step_runs') -%}
SELECT
    {{ groomed_column_list(source_table, except=['is_test_run']) | join(',\n        ') }},
    ROW_NUMBER() OVER (PARTITION BY workflow_run_id ORDER BY step_run_at_pt) AS position_in_workflow_run
FROM
    {{ source_table }}
WHERE NOT is_test_run

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    AND updated_at > '{{ get_max_updated_at() }}'
{% endif %}
