WITH first_steps AS (
    SELECT
        workflow_run_id,
        workflow_id,
        shop_id,
        shop_subdomain,
        run_at_utc,
        run_at_pt,
        is_billable,
        unbillable_reason,
        step_run_status AS run_status
    FROM {{ ref('step_runs') }}
    WHERE
        trigger_type = 'input'
),

action_run_stats AS (
    SELECT
        workflow_run_id,
        SUM(child_failure_count) AS child_failure_count, {# TODO: Does this only get set on the first step? #}
        COUNT(*) AS executed_step_count
    FROM {{ ref('step_runs') }}
    GROUP BY 1
),

shops AS (
    SELECT shop_id
    FROM {{ ref('stg_shops') }}
),

final AS (
    SELECT *
    FROM first_steps
    INNER JOIN shops USING (shop_id) -- Filter out workflow runs that don't have a Shop.
    LEFT JOIN action_run_stats USING (workflow_run_id)
)

SELECT * FROM final
