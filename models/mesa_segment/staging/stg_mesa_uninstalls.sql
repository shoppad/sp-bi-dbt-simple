WITH
raw_mesa_uninstalls AS (
    SELECT
        * EXCLUDE (timestamp, handle, {{ var('ugly_segment_fields') | join(', ') }})
            RENAME (id AS uninstall_event_id, user_id AS shop_subdomain),
        {{ pacific_timestamp('timestamp') }} AS uninstalled_at_pt,
        uninstalled_at_pt::DATE AS uninstall_on_pt
    FROM {{ source('php_segment', 'app_uninstalls') }}
    WHERE handle = 'mesa'
)
SELECT *
FROM raw_mesa_uninstalls
