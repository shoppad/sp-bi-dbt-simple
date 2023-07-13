with plan_events AS (
    SELECT
        uuid AS shop_subdomain,
        usg.value:plan_type::VARCHAR AS plan_type,
        usg.value:label::VARCHAR AS label,
        {{ pacific_timestamp('TO_TIMESTAMP(usg.value:start::VARCHAR)') }}::DATE AS started_on

    FROM {{ source('mongo_sync', 'shops') }}, table(flatten(usage)) usg
),

shop_events_formatted AS (

    SELECT
        *,
        COALESCE(LEAD(started_on) OVER (PARTITION BY shop_subdomain ORDER BY started_on ASC) - INTERVAL '1 day',
            LEAST(current_date(), started_on + INTERVAL '14 days')) AS ended_on,
        ended_on - started_on + 1 AS duration_in_days
    FROM plan_events
)

{# grouped_trials AS (

    SELECT
        shop_subdomain,
        plan_type,
        label,
        MIN(started_at) AS first_trial_ended_at,
        MAX(started_at) AS last_trial_ended_at,
        SUM(duration_in_days) AS total_trial_days_with_overlaps,
        TIMESTAMPDIFF(day,first_trial_ended_at, last_trial_ended_at) AS total_trial_span,
        COUNT(*) AS trials_count
    FROM shop_events_formatted
    WHERE plan_type ILIKE '%trial%'
    GROUP BY 1, 2, 3
    ORDER BY 7 DESC

) #}

SELECT *
FROM shop_events_formatted
WHERE plan_type ILIKE '%trial%'
