{%- set source_table = ref('stg_step_runs') -%}
{%- set columns_to_skip = ['workflow_run_id', 'step_run_id', 'workflow_run_at_utc', 'workflow_run_at_pt', 'workflow_run_on_pt', 'is_test_run'] %}
WITH test_runs AS (
    SELECT
        workflow_run_id AS test_run_id,
        step_run_id AS test_step_run_id,
        step_run_at_utc AS test_step_run_at_utc,
        step_run_at_pt AS test_step_run_at_pt,
        step_run_on_pt AS test_step_run_on_pt,
        {{ groomed_column_list(source_table, except=columns_to_skip) | join(',\n        ') }}
    FROM {{ source_table }}
    WHERE is_test_run
)

SELECT *
FROM test_runs

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    WHERE updated_at > '{{ get_max_updated_at() }}'
{% endif %}
