WITH shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}
),

step_run_counts AS (
    SELECT
        workflow_step_id,
        COUNT(*) AS run_count,
        COUNT_IF(run_status = 'success') AS run_count_success,
        COUNT_IF(run_status = 'fail') AS run_count_failure,
        run_count_success / run_count AS run_success_rate
    FROM {{ ref('int_step_runs') }}
    WHERE is_time_travel = FALSE
    GROUP BY workflow_step_id
)

SELECT
    *
FROM {{ ref('stg_workflow_steps') }}
INNER JOIN shops USING (shop_subdomain)
LEFT JOIN step_run_counts USING (workflow_step_id)
