{%- set source_table = ref('stg_workflow_runs') -%}
{%- set columns_to_skip = ['workflow_run_id', 'workflow_run_at_utc', 'workflow_run_at_pt', 'workflow_run_on_pt', 'is_test_run'] %}
SELECT
    workflow_run_id AS test_run_id,
    workflow_run_at_utc AS test_run_at_utc,
    workflow_run_at_pt AS test_run_at_pt,
    workflow_run_on_pt AS test_run_on_pt,
    {{ groomed_column_list(source_table, except=columns_to_skip)  | join(",\n       ") }},
    run_status = 'success' AND child_failure_count = 0 AS is_successful
FROM {{ source_table }}
WHERE is_test_run

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    AND updated_at > '{{ get_max_updated_at() }}'
{% endif %}
