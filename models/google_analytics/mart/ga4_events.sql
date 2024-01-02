WITH
ga4_events AS (
    SELECT * FROM {{ ref("int_ga4_events") }}
),

final AS (
    SELECT *
    FROM ga4_events
)

SELECT *
FROM final
