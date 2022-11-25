SELECT *
FROM
    {{ ref('stg_step_runs') }}
WHERE NOT(is_test_run)

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    AND updated_at > '{{ get_max_updated_at() }}'
{% endif %}
