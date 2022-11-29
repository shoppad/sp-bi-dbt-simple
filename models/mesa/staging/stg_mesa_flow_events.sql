WITH
flow_events AS (
    SELECT
        *,
        user_id AS shop_subdomain
    FROM {{ source('mesa_segment', 'flow_events') }}
    WHERE shop_subdomain NOT IN (SELECT * FROM {{ ref('staff_subdomains') }})
)

SELECT
    *
FROM flow_events
