WITH
    first_visits AS (
        SELECT *
        FROM {{ ref("int_ga4_events") }}
        WHERE (event_name IN ('first_visit', 'session_start')) {# Sometimes there is no first_visit. #}
    ),

    formatted_first_visits AS (
        SELECT
           shop_subdomain,
           {{ get_prefixed_columns(ref('int_ga4_events'), 'first_touch', exclude=['event_name']) }}
        FROM first_visits
    )

SELECT *
FROM formatted_first_visits
QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY first_touch_at_pt) = 1
