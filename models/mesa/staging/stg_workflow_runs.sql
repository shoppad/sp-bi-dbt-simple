WITH
shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}
),

workflows AS (
    SELECT workflow_id
    FROM {{ ref('stg_workflows') }}
),

first_step_runs AS (
    SELECT
        _id AS workflow_run_id,
        metadata:automation:_id::VARCHAR AS workflow_id,
        uuid AS shop_subdomain,
        _created_at AS workflow_run_at_utc,
        {{ pacific_timestamp("_created_at") }} AS workflow_run_at_pt,
        DATE_TRUNC('day', workflow_run_at_pt)::DATE AS workflow_run_on_pt,
        status AS run_status,
        NULLIF(metadata:unbillable_reason::STRING, '') AS unbillable_reason,
        unbillable_reason IS NOT NULL AS is_free_workflow,
        unbillable_reason IS NULL AS is_billable,
        metadata:automation:automation_name::STRING AS workflow_name,
        metadata:trigger:trigger_name::STRING AS source_app,
        metadata:trigger:trigger_key::STRING AS integration_key,
        COALESCE(metadata:child_fails, 0) AS child_failure_count,
        updated_at,
        metadata:is_test AS is_test_run
    FROM {{ source('mesa_mongo', 'tasks') }}
    WHERE
        NOT(__hevo__marked_deleted)
        AND metadata:trigger:trigger_type = 'input'
        AND workflow_id IS NOT NULL -- ~3,000 triggers don't have automation:id's
        AND run_status IN ('fail', 'success', 'replayed', 'stop')
        AND NOT(integration_key ILIKE '%delay%' OR integration_key ILIKE '%-vo%')
),

final AS (

    SELECT
        *,
        workflows.workflow_id IS NULL AS is_workflow_hard_deleted
    FROM first_step_runs
    INNER JOIN shops USING (shop_subdomain)
    LEFT JOIN workflows USING (workflow_id)

    {% if is_incremental() -%}
        WHERE
            updated_at > '{{ get_max_updated_at() }}'
    {% endif %}

)

SELECT * FROM final
