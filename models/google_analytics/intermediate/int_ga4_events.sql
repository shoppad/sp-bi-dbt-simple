WITH
user_matching AS (SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}),

staged_ga4_events AS (
    SELECT *
    FROM {{ ref("stg_ga4_events") }}
),

final AS (

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
            END AS traffic_source_source

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
)

SELECT
    * EXCLUDE (traffic_source_medium, param_medium, manual_medium),
    {# Make "app" cuter as "PQL" #}
    IFF(lower(traffic_source_medium) = 'app', 'PQL', traffic_source_medium) AS traffic_source_medium,
    IFF(lower(param_medium) = 'app', 'PQL', param_medium) AS param_medium,
    IFF(lower(manual_medium) = 'app', 'PQL', manual_medium) AS manual_medium
FROM final
