WITH uninstall_events AS (
    SELECT *
    FROM {{ ref('int_uninstall_app_events') }}
),

grouped_stated_reasons AS (
    SELECT
        shop_subdomain,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT reason), ', ') AS uninstall_reasons,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT description), ', ') AS uninstall_reason_details
    FROM uninstall_events
    GROUP BY 1
)

SELECT *
FROM grouped_stated_reasons
