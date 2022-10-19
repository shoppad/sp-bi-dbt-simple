WITH
shops AS (
    SELECT
        shop_id,
        shop_subdomain
    FROM {{ ref('stg_shops') }}
),

decorated_step_runs AS (
    {% set columns_to_skip = [
        '_id',
        '_created_at',
        'group',
        'uuid'
    ]%}
    SELECT
        _id AS step_run_id,
        COALESCE(metadata:"parents"[0]:"task_id", step_run_id) AS workflow_run_id,
        metadata:automation:_id::varchar AS workflow_id,
        uuid AS shop_subdomain,
        created_at AS run_at_utc,
        {{ pacific_timestamp("_created_at") }} AS run_at_pt,
        metadata:trigger:trigger_type::string AS trigger_type,
        NULLIF(metadata:unbillable_reason::string,'') AS unbillable_reason,
        unbillable_reason IS NOT NULL AS is_free_workflow,
        (
            trigger_type = 'input'
                AND unbillable_reason IS NULL
        ) AS is_billable,
        COALESCE(metadata:child_fails, 0) AS child_failure_count,
        __hevo__ingested_at AS synced_at,
        {{ groomed_column_list(source('mesa_mongo', 'tasks'), columns_to_skip=columns_to_skip) }}
    FROM
        {{ source('mesa_mongo', 'tasks') }}
    WHERE NOT(__hevo__marked_deleted)
        AND workflow_id IS NOT NULL -- Eliminates about ~1,700 records without a workflow ID
        AND status NOT IN ('ready', 'skip', 'skip-ignore', 'skip-retry')

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        AND updated_at > '{{ get_max_updated_at() }}'
    {% endif %}
),

final AS (
    SELECT *
    FROM decorated_step_runs
    INNER JOIN shops USING (shop_subdomain)
)

SELECT * FROM final
