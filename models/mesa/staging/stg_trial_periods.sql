WITH final AS (
    SELECT *
    FROM {{ ref('stg_plan_changes_from_usage') }}
    WHERE plan_type ILIKE '%trial%'
)

SELECT *
FROM final
