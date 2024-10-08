WITH
user_matching AS (SELECT * FROM {{ ref("stg_anonymous_to_known_user_matching") }}),

staged_ga4_events AS (
    SELECT
        * EXCLUDE user_pseudo_id,
        user_pseudo_id::STRING AS user_pseudo_id

    FROM {{ ref("stg_ga4_events") }}

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        WHERE event_timestamp_pt > '{{ get_max_updated_at('event_timestamp_pt') }}'
    {% endif %}
),

reformatted AS (

    SELECT
        user_matching.shop_subdomain,
        COALESCE(user_matching.shopify_id, staged_ga4_events.shopify_id) AS shopify_id,
        staged_ga4_events.*
            EXCLUDE (shopify_id, shop_subdomain, traffic_source_medium, traffic_source_source),

        {# Manual reformatting based on experience. #}

        CASE
            WHEN app_store_surface_type = 'search_ad'
                THEN 'App Store - CPC'
            WHEN app_store_surface_type = 'search'
                THEN 'App Store - Organic Search'
            WHEN app_store_surface_type IS NOT NULL
                THEN 'App Store - ' || app_store_surface_type
            {# If the traffic_source_medium is '(none)', then it's direct.
                But first check if param_medium is set, and if so, use that instead. #}
            WHEN lower(traffic_source_medium) = '(none)'
                THEN
                    CASE
                        WHEN lower(page_referrer_host) = 'apps.shopify.com'
                            THEN 'App Store - Direct/Other'
                        ELSE COALESCE(
                            param_medium,
                            IFF(
                                page_referrer_full IS NOT NULL,
                                nullif(PARSE_URL('https://' || page_referrer_full):parameters:utm_medium, ''),
                                NULL
                            ),
                            IFF(
                                page_referrer_full ILIKE '%apps.shopify.com%', 'App Store - Direct/Other', 'direct'
                            ) {# Do a bunch of stuff to override direct because of the way the
                                App Store works. #}
                        )
                    END

            {# Rename Shopify App Store to [medium:app store] #}
            WHEN
                lower(traffic_source_medium) = 'referral' AND traffic_source_source ILIKE '%apps.shopify%'
                    THEN 'App Store - Direct/Other'

            {# Fallback to original #}
            ELSE traffic_source_medium
            END AS traffic_source_medium,

        CASE
            WHEN
                traffic_source_source ILIKE '%direct%'
                    THEN COALESCE(
                            param_source,
                            IFF(
                                page_referrer_full IS NOT NULL,
                                nullif(PARSE_URL('https://' || page_referrer_full):parameters:utm_source, ''),
                                NULL
                            ),
                            IFF(
                                page_referrer_full ILIKE '%apps.shopify.com%', 'shopify', 'direct'
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
                    WHEN REGEXP_LIKE(page_location_path, '^/apps/[^/]+/integrate/[^/]+/[^/]+$') THEN 'Template'
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
        IFF(lower(manual_medium) = 'app', 'PQL Link', manual_medium) AS manual_medium,
        ROW_NUMBER() OVER (PARTITION BY event_name, ga_session_id ORDER BY event_timestamp_pt ASC) AS event_order,
        event_name = 'page_view'
            AND LAG(event_timestamp_pt) OVER (PARTITION BY event_name, ga_session_id ORDER BY event_timestamp_pt ASC) IS NULL AS is_landing_pageview,
        event_name = 'page_view'
            AND LEAD(event_timestamp_pt) OVER (PARTITION BY event_name, ga_session_id ORDER BY event_timestamp_pt ASC) IS NULL AS is_exit_pageview

    FROM reformatted
)

SELECT *
FROM final
