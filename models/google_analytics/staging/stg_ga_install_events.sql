WITH installation_events AS (

    SELECT
        user_pseudo_id,
        user_id AS shop_subdomain,
        event_timestamp,
        PARSE_URL(page_location) AS page_params,
        page_params:parameters:utm_content::STRING AS utm_content,
        page_params:parameters:utm_campaign::STRING AS utm_campaign,
        page_params:parameters:utm_medium::STRING AS utm_medium,
        page_params:parameters:utm_source::STRING AS utm_source,
        page_params:parameters:page_referrer::STRING AS referrer,
        page_params:host::STRING AS referrer_host,
        page_params:parameters:referrer_source::STRING AS referrer_source,
        page_params:parameters:referrer_medium::STRING AS referrer_medium,
        shop_id
    FROM {{ source('mesa_ga4', 'events') }}
    WHERE event_name = 'getmesa_install_convert'
)

SELECT *
FROM installation_events
