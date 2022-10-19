{{ config(materialized = 'table') }}

WITH triggers AS (
    SELECT
        *,
        triggers."TYPE" AS integration_app,
        automation AS workflow_id
    FROM {{ source('mesa_mongo', 'mesa_triggers') }} AS triggers
),

first_steps as (
    SELECT *
    FROM triggers
    WHERE trigger_type = 'input' AND weight = 0
    ORDER BY type ASC
),

last_steps as (
    SELECT *
    FROM triggers
    WHERE trigger_type = 'output'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_id ORDER BY weight DESC) = 1
    ORDER BY type ASC
),

final AS (
    {# TODO: Missing workflows that have never been run before. Will need to join on the table that houses raw workflows. #}
    SELECT
        workflow_id,
        workflow_runs.shop_id,
		MIN(IFF(workflow_runs.is_billable, workflow_runs.run_at_pt, NULL)) AS first_run_at_pt,
		LISTAGG(DISTINCT first_steps.integration_app, ', ') AS first_step_app, {# TODO: Add ` WITHIN GROUP (ORDER BY integration_app ASC)::varchar` to alphabetize items. #}
		LISTAGG(DISTINCT last_steps.integration_app, ', ') AS last_step_app,
        COUNT(triggers.*) AS step_count,
        MIN(IFF((workflow_runs.is_billable AND workflow_runs.run_status = 'success'), workflow_runs.run_at_pt, NULL)) AS first_successful_run_at_pt,
        COUNT(IFF(workflow_runs.is_billable, workflow_runs.workflow_run_id, NULL)) AS run_start_count,
        COUNT(IFF((workflow_runs.is_billable AND workflow_runs.run_status = 'success'), workflow_runs.workflow_run_id, NULL)) AS run_success_count
    FROM {{ ref('workflow_runs') }} AS workflow_runs
    LEFT JOIN first_steps USING (workflow_id)
    LEFT JOIN last_steps USING (workflow_id)
    LEFT JOIN triggers USING (workflow_id)
    GROUP BY
        1,
        2
    ORDER BY 1 DESC
)

SELECT * FROM final
