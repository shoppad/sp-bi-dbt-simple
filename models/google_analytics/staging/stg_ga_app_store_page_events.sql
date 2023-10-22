WITH source AS (
  SELECT
      user_pseudo_id,
      user_id,
      event_name,
      PARSE_URL(page_location) AS page_params,
      PARSE_URL(page_location):parameters:surface_detail::STRING AS app_store_search_term,
      PARSE_URL(page_location):parameters:surface_type::STRING AS app_store_surface_type,
      PARSE_URL(page_location):parameters:surface_intra_position::STRING AS app_store_surface_intra_position,
      PARSE_URL(page_location):parameters:surface_inter_position::STRING AS app_store_surface_inter_position,
      PARSE_URL(page_location):parameters:locale::STRING AS app_store_locale,
      page_params:parameters:utm_content::STRING AS utm_content,
          page_params:parameters:utm_campaign::STRING AS utm_campaign,
          page_params:parameters:utm_medium::STRING AS utm_medium,
          page_params:parameters:utm_source::STRING AS utm_source,
          page_params:parameters:utm_term::STRING AS utm_term,
          page_params:parameters:page_referrer::STRING AS referrer,
          page_params:host::STRING AS referrer_host,
          page_params:parameters:referrer_source::STRING AS referrer_source,
          page_params:parameters:referrer_medium::STRING AS referrer_medium,
          page_params:parameters:referrer_term::STRING AS referrer_term,
          page_params:parameters:shop_id::STRING AS shopify_id
  FROM {{ source('mesa_ga4', 'events') }}
  WHERE
      (page_location ILIKE '%apps.shopify.com%' AND event_name = 'page_view')
      OR
      event_name ILIKE 'shopify%'

)

SELECT
    user_pseudo_id,
    shopify_id,
    CASE
      WHEN app_store_surface_type IS NOT NULL THEN 'Shopify App Store'
      ELSE utm_source
      END AS utm_source,
    app_store_locale,
    APP_STORE_SEARCH_TERM,
    event_name,
    CASE
      WHEN app_store_surface_intra_position IS NOT NULL
      THEN CONCAT('Intra pos:', app_store_surface_intra_position, ' / Inter pos:', app_store_surface_inter_position)
      ELSE utm_campaign
      END AS utm_campaign,
    CASE
      WHEN app_store_surface_type = 'search_ad' THEN 'CPC'
      ELSE COALESCE(app_store_surface_type, utm_medium)
      END AS app_store_surface_type
FROM source
{# WHERE shopify_id IS NOT NULL #}