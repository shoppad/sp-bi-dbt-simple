SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY workflow_run_id ORDER BY step_run_at_pt) AS position_in_workflow_run
FROM
    {{ ref('stg_step_runs') }}
WHERE NOT(is_test_run)

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    AND updated_at > '{{ get_max_updated_at() }}'
{% endif %}
