WITH

    session_starts AS (
        SELECT * EXCLUDE event_name
        FROM {{ ref("int_ga4_events") }}
        WHERE event_name = 'session_start'
    )

SELECT
    *
FROM session_starts
