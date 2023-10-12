with usage_periods AS (
    SELECT
        uuid AS shop_subdomain,
        usg.value:plan_type::VARCHAR AS plan_type,
        usg.value:label::VARCHAR AS label,
        (usg.value:start / 1000)::VARCHAR AS started_at_unix_timestamp,
        CASE
            WHEN usg.value:end IS NULL THEN
                CASE WHEN usg.value:plan_type ILIKE '%trial%' THEN
                    (billing:plan:trial_ends / 1000)::VARCHAR
                ELSE
                    NULL
                END
            ELSE (usg.value:end / 1000)::VARCHAR
        END AS intended_ended_at_unix_timestamp
    FROM {{ source('mongo_sync', 'shops') }}, table(flatten(usage)) usg
),

usage_periods_formatted AS (

    SELECT
        *,
        COALESCE(LEAST(LEAD(started_at_unix_timestamp) OVER (PARTITION BY shop_subdomain ORDER BY started_at_unix_timestamp ASC) - 1, intended_ended_at_unix_timestamp),
            DATE_PART('epoch', CURRENT_TIMESTAMP)) AS ended_at_unix_timestamp,
        TO_TIMESTAMP_TZ(started_at_unix_timestamp) AS started_at_utc,
        TO_TIMESTAMP_TZ(ended_at_unix_timestamp) AS ended_at_utc,
        {{ pacific_timestamp('started_at_utc') }} AS started_at_pt,
        started_at_pt::DATE AS started_on_pt,
        {{ pacific_timestamp('ended_at_utc') }}  AS ended_at_pt,
        ended_at_pt::DATE - INTERVAL '1day' AS ended_on_pt,
        TIMEDIFF(day, started_at_pt, ended_at_pt) + 1 AS duration_in_days
    FROM usage_periods
)

SELECT *
FROM usage_periods_formatted
