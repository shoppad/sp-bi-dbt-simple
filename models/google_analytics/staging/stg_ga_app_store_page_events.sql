with
    user_matching AS (SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}),

    source AS (
        SELECT
            user_pseudo_id,
            shop_subdomain,
            event_name,
            shopify_id,
            event_timestamp_pt,
            page_location,

            {# Attribution #}
            COALESCE(traffic_source_name, param_campaign) AS utm_campaign,
            COALESCE(traffic_source_medium, param_medium) AS utm_medium,
            COALESCE(traffic_source_source, param_source) AS utm_source,
            device_category,
            param_content AS utm_content,
            param_term AS utm_term,
            * ilike 'referrer%',

            {# App Store #}
            * ilike 'app_store%'
        FROM {{ ref("stg_ga4_events") }}
        WHERE
            page_location ilike '%apps.shopify.com%'
            OR event_name ilike 'shopify%'
            OR page_location ilike '%surface_%'

    ),

    final AS (

        SELECT
            source.* exclude (utm_source, utm_campaign, shop_subdomain),
            user_matching.shop_subdomain,
            CASE
                WHEN app_store_surface_type IS NOT NULL
                THEN 'Shopify App Store'
                ELSE utm_source
            END AS utm_source,

            CASE
                WHEN app_store_surface_intra_position IS NOT NULL
                THEN
                    CONCAT(
                        'Intra pos:',
                        app_store_surface_intra_position,
                        ' / Inter pos:',
                        app_store_surface_inter_position
                    )
                ELSE utm_campaign
            END AS utm_campaign
        FROM source
        INNER JOIN
            user_matching
            ON (
                source.user_pseudo_id = user_matching.user_pseudo_id
                OR source.shopify_id = user_matching.shopify_id
                OR source.shop_subdomain = user_matching.shop_subdomain
            )
    )

SELECT *
FROM final
