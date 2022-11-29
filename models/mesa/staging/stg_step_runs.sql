WITH
workflow_runs AS (
    SELECT
        workflow_run_id,
        workflow_id,
        workflow_name,
        shop_subdomain,
        is_workflow_hard_deleted
    FROM {{ ref('stg_workflow_runs') }}
),

decorated_step_runs AS (
    {%- set columns_to_skip = [
        '_id',
        'created_at',
        '_created_at',
        'status',
        'group',
        'handle',
        'method',
        'metadata',
        'payload',
        'response',
        'request',
        'uuid',
    ] %}
    SELECT
        _id AS step_run_id,
        IFF(
            IS_NULL_VALUE(metadata:parents[0]:task_id) OR metadata:parents[0]:task_id IS NULL,
            step_run_id,
            metadata:parents[0]:task_id
        ) AS workflow_run_id,
        metadata:trigger:_id::VARCHAR AS workflow_step_id,
        _created_at AS step_run_at_utc,
        {{ pacific_timestamp("_created_at") }} AS step_run_at_pt,
        DATE_TRUNC('day', step_run_at_pt)::DATE AS step_run_on_pt,
        metadata:trigger:trigger_type::STRING AS step_type,
        metadata:trigger:trigger_name::STRING AS integration_name,
        metadata:trigger:trigger_key::STRING AS integration_key,
        status AS run_status,
        metadata:trigger:trigger_name::VARCHAR AS workflow_step_name,
        metadata:trigger:trigger_key::VARCHAR AS workflow_step_key,
        NOT(metadata:is_test = TRUE) AS is_test_run,
        {{ groomed_column_list(source('mesa_mongo', 'tasks'), except=columns_to_skip)  | join(",\n      ") }}
    FROM
        {{ source('mesa_mongo', 'tasks') }}
    WHERE
        NOT(__hevo__marked_deleted)
        AND status NOT IN ('ready', 'skip', 'skip-ignore', 'skip-retry')
        AND NOT(metadata:parents[0].trigger_key ILIKE '%delay%' OR metadata:parents[0].trigger_key ILIKE '%-vo%')

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        AND updated_at > '{{ get_max_updated_at() }}'
    {% endif %}
),

final AS (
    SELECT *
    FROM decorated_step_runs
    INNER JOIN workflow_runs USING (workflow_run_id)
)

SELECT * FROM final
