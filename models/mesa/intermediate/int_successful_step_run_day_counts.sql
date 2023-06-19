WITH
shop_calendar AS (
    SELECT *
    FROM {{ ref('int_shop_calendar') }}
),

successful_workflow_steps AS (
    SELECT
        shop_subdomain,
        step_run_on_pt AS dt,
        step_type,
        integration_key,
        workflow_run_id
    FROM {{ ref('int_step_runs') }}
    WHERE run_status = 'success'
),

final AS (
    SELECT
        shop_subdomain,
        dt,
        COALESCE(COUNT(*), 0) AS total_workflow_steps_count,
        COALESCE(COUNT_IF(successful_workflow_steps.step_type = 'input'), 0) AS input_step_count,
        COALESCE(COUNT_IF(successful_workflow_steps.step_type = 'output'), 0) AS output_step_count
    FROM shop_calendar
    LEFT JOIN successful_workflow_steps USING (shop_subdomain, dt)
    GROUP BY
        1,
        2
)

SELECT * FROM final
