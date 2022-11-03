WITH
first_step_runs AS (
    SELECT
        workflow_run_id,
        workflow_id,
        shop_id,
        shop_subdomain,
        run_at_utc,
        run_at_pt,
        run_on_pt,
        is_billable,
        unbillable_reason,
        integration_app AS source_app,
        run_status AS run_status,
        updated_at
    FROM {{ ref('stg_step_runs') }}
    WHERE
        trigger_type = 'input'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workflow_run_id
        ORDER BY run_at_utc ASC
    ) = 1
)

SELECT *
FROM first_step_runs

{% if is_incremental() -%}
    WHERE
        updated_at > '{{ get_max_updated_at() }}'
{% endif %}
