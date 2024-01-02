WITH
    first_install_events AS (
        SELECT
            * RENAME (
                event_timestamp_pt AS first_install_timestamp_pt
            )
        FROM {{ ref("int_ga_install_events") }}
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY shop_subdomain
                ORDER BY event_timestamp_pt
            )
            = 1
    ),

    session_starts AS (SELECT * FROM {{ ref("int_ga_session_starts") }}),

    last_touch_sessions AS (
        SELECT * EXCLUDE (rn)
        FROM
            (
                SELECT
                    session_starts.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY first_install_events.shop_subdomain
                        ORDER BY session_starts.event_timestamp_pt DESC
                    ) AS rn
                FROM session_starts
                INNER JOIN first_install_events USING (shop_subdomain)
                WHERE
                    session_starts.event_timestamp_pt
                    <= first_install_events.first_install_timestamp_pt
                QUALIFY rn = 1
            ) AS t
    ),

    formatted_last_touch_session_starts AS (

        SELECT
            shop_subdomain,
            {{ get_prefixed_columns(ref('int_ga_session_starts'), 'last_touch') }}
        FROM last_touch_sessions
    )

SELECT *
FROM formatted_last_touch_session_starts
