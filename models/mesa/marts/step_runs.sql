WITH
step_runs AS (
    SELECT *
    FROM {{ ref('int_step_runs') }}
),

workflow_steps AS (
    SELECT *
    FROM {{ ref('stg_workflow_steps') }}
),

workflows AS (
    SELECT *
    FROM {{ ref('stg_workflows') }}
),

final AS (
    SELECT step_runs.*
    FROM step_runs
    INNER JOIN workflows USING (workflow_id)
    LEFT JOIN workflow_steps USING (workflow_step_id)

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
    WHERE  updated_at > '{{ get_max_updated_at() }}'
{% endif %}

)

SELECT *
FROM final
