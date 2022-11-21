SELECT
    *,
    run_status = 'success' AS is_successful
FROM {{ ref('stg_workflow_runs') }}
WHERE is_test_run

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    AND updated_at > '{{ get_max_updated_at() }}'
{% endif %}
