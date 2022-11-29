WITH shops AS (
    SELECT
        shop_subdomain,
        activation_date_pt
    FROM {{ ref('int_shops') }}
),

flow_events AS (
    SELECT
        *,
        user_id AS shop_subdomain
    FROM {{ source('mesa_segment', 'flow_events') }}
)

SELECT
    *,
    IFF(activation_date_pt >= timestamp, 'activated', 'onboarding') AS funnel_phase
FROM flow_events
INNER JOIN shops USING (shop_subdomain)
