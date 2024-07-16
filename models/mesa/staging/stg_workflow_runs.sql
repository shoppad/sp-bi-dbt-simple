WITH
shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}
),

workflows AS (
    SELECT workflow_id
    FROM {{ ref('stg_workflows') }}
),

trigger_runs AS (
    SELECT *
        RENAME (_id AS workflow_run_id, uuid AS shop_subdomain, _created_at AS workflow_run_at_utc, status AS run_status)
    FROM {{ source('mongo_sync', 'tasks') }}
    WHERE
        NOT (__hevo__marked_deleted)
        AND metadata:trigger:trigger_type = 'input'
        AND status IN ('fail', 'success', 'replayed', 'stop', 'error')

),

formatted_trigger_runs AS (
    SELECT
        workflow_run_id,
        metadata:automation:_id::VARCHAR AS workflow_id,
        shop_subdomain,
        workflow_run_at_utc,
        {{ pacific_timestamp("workflow_run_at_utc") }} AS workflow_run_at_pt,
        DATE_TRUNC('day', workflow_run_at_pt)::DATE AS workflow_run_on_pt,
        NULLIF(metadata:unbillable_reason::STRING, '') AS unbillable_reason,
        unbillable_reason IS NOT NULL AS is_free_workflow,
        unbillable_reason IS NULL AS is_billable,
        metadata:automation:automation_name::STRING AS workflow_name,
        metadata:trigger:trigger_name::STRING AS source_app,
        metadata:trigger:trigger_key::STRING AS integration_key,
        metadata:child_fails::NUMERIC AS child_failure_count,
        metadata:child_stops::NUMERIC AS child_stop_count,
        metadata:child_completes::NUMERIC AS child_complete_count,
        case
            WHEN child_failure_count > 0
                THEN 'fail'
            WHEN child_stop_count > 0
                THEN 'stop'
            ELSE
                run_status
            end AS run_status,
        updated_at,
        metadata:is_test AS is_test_run,
        COALESCE(NOT IS_NULL_VALUE(metadata:backfill_id), FALSE) AS is_time_travel
    FROM trigger_runs
    WHERE
        workflow_id IS NOT NULL -- ~3,000 triggers don't have automation:id's which is workflow_id
        AND NOT (integration_key ILIKE '%delay%' OR integration_key ILIKE '%-vo%')


),

final AS (

    SELECT
        *,
        workflows.workflow_id IS NULL AS is_workflow_hard_deleted
    FROM formatted_trigger_runs
    INNER JOIN shops USING (shop_subdomain)
    LEFT JOIN workflows USING (workflow_id)

    {% if is_incremental() -%}
        WHERE
            updated_at > '{{ get_max_updated_at() }}'
    {% endif %}

)

SELECT * FROM final
