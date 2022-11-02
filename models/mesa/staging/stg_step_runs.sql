WITH
workflows AS (
    SELECT
        workflow_id,
        shop_id,
        shop_subdomain
    FROM {{ ref('stg_workflows') }}
),

decorated_step_runs AS (
    {% set columns_to_skip = [
        '_id',
        'created_at',
        'group',
        'handle',
        'method',
        'payload',
        'response',
        'request',
        'uuid',
    ] %}
    SELECT
        _id AS step_run_id,
        IFF(IS_NULL_VALUE(metadata:parents[0]:task_id) OR metadata:parents[0]:task_id IS NULL, step_run_id, metadata:parents[0]:task_id) AS workflow_run_id,
        metadata:automation:_id::varchar AS workflow_id,
        metadata:trigger:_id::varchar AS workflow_step_id,
        _created_at AS run_at_utc,
        {{ pacific_timestamp("_created_at") }} AS run_at_pt,
        metadata:trigger:trigger_type::string AS trigger_type,
        NULLIF(metadata:unbillable_reason::string, '') AS unbillable_reason,
        unbillable_reason IS NOT NULL AS is_free_workflow,
        (
            trigger_type = 'input'
            AND unbillable_reason IS NULL
        ) AS is_billable,
        COALESCE(metadata:child_fails, 0) AS child_failure_count,
        {{ groomed_column_list(source('mesa_mongo', 'tasks'), except=columns_to_skip)  | join(",\n      ") }}
    FROM
        {{ source('mesa_mongo', 'tasks') }}
    WHERE
        workflow_id IS NOT NULL -- Eliminates about ~1,700 records without a workflow ID
        AND status NOT IN ('ready', 'skip', 'skip-ignore', 'skip-retry')
        AND NOT(COALESCE(metadata:is_test, FALSE))

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        AND updated_at > '{{ get_max_updated_at() }}'
    {% endif %}
),

final AS (
    SELECT *
    FROM decorated_step_runs
    INNER JOIN workflows USING (workflow_id)
)

SELECT * FROM final
