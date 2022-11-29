WITH
install_page_sessions AS (

    SELECT session_id
    FROM {{ ref('segment_web_page_views__sessionized') }}
    WHERE page_url_path ILIKE '/apps/mesa/install%'

),

final AS (

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
        acquisition_source || ' - ' || acquisition_medium AS acquisition_source_medium
    FROM install_page_sessions
    LEFT JOIN {{ ref('segment_web_sessions') }} USING (session_id)
    WHERE acquisition_source IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY blended_user_id ORDER BY session_start_tstamp ASC) = 1

)

SELECT * FROM final
