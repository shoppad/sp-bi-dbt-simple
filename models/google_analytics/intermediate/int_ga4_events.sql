WITH
user_matching AS (SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}),

staged_ga4_events AS (
    SELECT *
    FROM {{ ref("stg_ga4_events") }}
),

reformatted AS (

    SELECT
        user_matching.shop_subdomain,
        COALESCE(user_matching.shopify_id, staged_ga4_events.shopify_id) AS shopify_id,
        staged_ga4_events.*
            EXCLUDE (shopify_id, shop_subdomain, traffic_source_medium, traffic_source_source),

        {# Manual reformatting based on experience. #}

        CASE
            {# If the traffic_source_medium is '(none)', then it's direct.
                But first check if param_medium is set, and if so, use that instead. #}
            WHEN lower(traffic_source_medium) = '(none)'
                THEN
                    CASE
                        WHEN lower(referrer_host) = 'apps.shopify.com'
                            THEN 'app store'
                        ELSE COALESCE(
                            param_medium,
                            IFF(
                                referrer_full IS NOT NULL,
                                nullif(PARSE_URL('https://' || referrer_full):parameters:utm_medium, ''),
                                NULL
                            ),
                            IFF(
                                referrer_full ILIKE '%apps.shopify.com%', 'app store', 'direct'
                            ) {# Do a bunch of stuff to override direct because of the way the
                                App Store works. #}
                        )
                    END

            {# Rename Shopify App Store to [medium:app store] #}
            WHEN
                lower(traffic_source_medium) = 'referral' AND traffic_source_source ILIKE '%apps.shopify%'
                    THEN 'app store'

            {# Fallback to original #}
            ELSE traffic_source_medium
            END AS traffic_source_medium,

        CASE
            WHEN app_store_surface_type = 'search_ad'
                THEN 'App Store - CPC'
            WHEN app_store_surface_type = 'search'
                THEN 'App Store - Organic Search'
            WHEN
                traffic_source_source ILIKE '%direct%'
                    THEN COALESCE(
                            param_source,
                            IFF(
                                referrer_full IS NOT NULL,
                                nullif(PARSE_URL('https://' || referrer_full):parameters:utm_source, ''),
                                NULL
                            ),
                            IFF(
                                referrer_full ILIKE '%apps.shopify.com%', 'shopify', 'direct'
                            ) {# Do a bunch of stuff to override direct because of the way
                                the App Store works. #}
                    )
            WHEN
                {# Rename Shopify App to [source:shopify]  #}
                traffic_source_source ILIKE '%apps.shopify%'
                    THEN 'shopify'
            ELSE traffic_source_source
            END AS traffic_source_source,
            NULLIF(
                CASE
                    WHEN page_location ILIKE '%getmesa.com/blog%' OR page_location_host = 'blog.getmesa.com' THEN 'Blog'
                    WHEN page_location ILIKE '%apps.shopify.com/mesa%' THEN 'Shopify App Store'
                    WHEN page_location ILIKE '%docs.getmesa%' THEN 'Support Site'
                    WHEN page_location ILIKE '%getmesa.com/' THEN 'Homepage'
                    WHEN page_location ILIKE '%app.getmesa%' THEN 'Inside App (Untrackable)'
                    WHEN page_location ILIKE '%getmesa.com%' THEN initcap(SPLIT_PART(page_location_path, '/', 2))
                    WHEN page_location IS NULL THEN '(Untrackable)'
                    ELSE page_location
                END,
                ''
                ) AS page_location_page_type

    FROM staged_ga4_events
    LEFT JOIN user_matching
        ON
            (
                staged_ga4_events.user_pseudo_id = user_matching.user_pseudo_id
                OR
                nullif(staged_ga4_events.shopify_id::STRING, '') = NULLIF(user_matching.shopify_id::STRING, '')
                OR
                staged_ga4_events.shop_subdomain = user_matching.shop_subdomain
            )
),

final AS (

    SELECT
        * EXCLUDE (traffic_source_medium, param_medium, manual_medium),
        {# Make "app" cuter as "PQL" #}
        IFF(lower(traffic_source_medium) = 'app', 'PQL Link', traffic_source_medium) AS traffic_source_medium,
        IFF(lower(param_medium) = 'app', 'PQL Link', param_medium) AS param_medium,
        IFF(lower(manual_medium) = 'app', 'PQL Link', manual_medium) AS manual_medium
    FROM reformatted
)

SELECT *
FROM final
