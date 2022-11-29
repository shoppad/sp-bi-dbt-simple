WITH
install_page_sessions AS (

    SELECT
        session_id,
        tstamp
    FROM {{ ref('segment_web_page_views__sessionized') }}
    WHERE page_url_path ILIKE '/apps/mesa/install%'

),

formatted_install_pageviews AS (

    SELECT
        blended_user_id AS shop_subdomain,
        utm_campaign AS acquisition_campaign,
        utm_content AS acquisition_content,
        referrer AS acquisition_referrer,
        first_page_url_path AS acquisition_first_page_path,
        CASE
            WHEN referrer ILIKE '%apps.shopify.com'
                THEN 'Shopify App Store'
            ELSE COALESCE(referrer_source, utm_source)
        END AS acquisition_source,
        COALESCE(referrer_medium, utm_medium) AS acquisition_medium,
        acquisition_source || ' - ' || acquisition_medium AS acquisition_source_medium,
        tstamp
    FROM install_page_sessions
    LEFT JOIN {{ ref('segment_web_sessions') }} USING (session_id)
    WHERE acquisition_source IS NOT NULL

),

raw_install_events AS (
    SELECT * FROM {{ source('mesa_mongo', 'mesa_install_events') }}
),

formatted_install_events AS (
    SELECT
        uuid AS shop_subdomain,
        utm_campaign AS acquisition_campaign,
        NULL AS acquisition_content,
        referer AS acquisition_referrer,
        NULL AS acquisition_first_page_path,
        CASE
            WHEN referer ILIKE '%apps.shopify.com'
                THEN 'Shopify App Store'
            ELSE COALESCE(referer, utm_source)
        END AS acquisition_source,
        utm_medium AS acquisition_medium,
        acquisition_source || ' - ' || acquisition_medium AS acquisition_source_medium,
        created_at AS tstamp
    FROM raw_install_events
),

final AS (
    SELECT
        shop_subdomain,
        acquisition_campaign,
        acquisition_content,
        acquisition_referrer,
        acquisition_first_page_path,
        acquisition_source,
        acquisition_medium,
        acquisition_source_medium
    FROM (
        SELECT * FROM formatted_install_events
        UNION ALL
        SELECT * FROM formatted_install_pageviews
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY tstamp ASC) = 1
)

SELECT * FROM final
