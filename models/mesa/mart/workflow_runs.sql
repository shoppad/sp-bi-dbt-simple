WITH run_starts AS (
    SELECT *
    FROM {{ ref('stg_trigger_runs') }}
),

action_run_stats AS (
    SELECT
        workflow_run_id,
        SUM(child_failure_count) AS child_failure_count, {# TODO: Does this only get set on the first step? #}
        COUNT(*) AS executed_step_count
    FROM {{ ref('step_runs') }}
    WHERE trigger_type = 'input' {# TODO: Would prefer to remove this and include all steps in workflow run. But it currently returns a lot of step runs without true "Parent" associations. #}
    GROUP BY 1
),

shops AS (
    SELECT shop_id
    FROM {{ ref('stg_shops') }}
),

final AS (
    SELECT
        *
    FROM run_starts
    INNER JOIN shops USING (shop_id) -- Filter out workflow runs that don't have a Shop.
    LEFT JOIN action_run_stats USING (workflow_run_id)
)

SELECT * FROM final
