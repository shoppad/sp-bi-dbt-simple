WITH first_visits AS (

    SELECT
        user_pseudo_id,
        page_location,
        event_name,
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
        page_params:parameters:referrer_campaign::STRING AS referrer_campaign,
        page_params:parameters:surface_detail::STRING AS app_store_search_term,
        page_params:parameters:surface_type::STRING AS app_store_surface_type,
        page_params:parameters:surface_intra_position::STRING AS app_store_surface_intra_position,
        page_params:parameters:surface_inter_position::STRING AS app_store_surface_inter_position,
        page_params:parameters:locale::STRING AS app_store_locale
    FROM {{ source('mesa_ga4', 'events') }}
    WHERE event_name = 'first_visit'
)

SELECT *
FROM first_visits
