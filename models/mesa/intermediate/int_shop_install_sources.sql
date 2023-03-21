{#- cSpell:words INITCAP -#}

WITH
shops AS (
    SELECT
        shop_subdomain,
        first_installed_at_pt
    FROM {{ ref('stg_shops') }}
),

install_page_sessions AS (

    SELECT
        session_id,
        tstamp
    FROM {{ ref('segment_web_page_views__sessionized') }}
    WHERE page_url_path ILIKE '%/apps/mesa/install%'

),

segment_web_sessions AS (
    SELECT
        * EXCLUDE (blended_user_id),
        blended_user_id AS shop_subdomain
    FROM {{ ref('segment_web_sessions') }}
),

raw_install_events AS (
    SELECT
        * EXCLUDE (uuid),
        uuid AS shop_subdomain
    FROM {{ source('mongo_sync', 'mesa_install_events') }}
),

formatted_install_pageviews AS (
    SELECT
        shop_subdomain,
        {{ pacific_timestamp('tstamp') }} AS tstamp_pt,
        utm_content AS acquisition_content,
        first_page_url_path AS acquisition_first_page_path,
        utm_campaign AS acquisition_campaign,
        referrer AS acquisition_referrer,
        referrer_host,
        CASE
            WHEN utm_source IS NOT NULL AND utm_source != ''
                THEN utm_source
            WHEN referrer ILIKE '%apps.shopify.com%'
                THEN 'Shopify App Store'
            ELSE COALESCE(referrer_source, utm_source)
        END AS acquisition_source,
        COALESCE(referrer_medium, utm_medium) AS acquisition_medium
    FROM install_page_sessions
    LEFT JOIN segment_web_sessions USING (session_id)
    LEFT JOIN shops USING (shop_subdomain)
    WHERE acquisition_source IS NOT NULL
        AND tstamp_pt <= first_installed_at_pt + INTERVAL '60seconds'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY tstamp_pt ASC) = 1
),

formatted_install_events AS (
    SELECT
        shop_subdomain,
        {{ pacific_timestamp('created_at') }} AS tstamp_pt,
        template AS acquisition_template,
        NULL AS acquisition_content,
        NULL AS acquisition_first_page_path,
        NULLIF(utm_campaign, '') AS acquisition_campaign,
        NULLIF(referer, '') AS acquisition_referrer,
        NULLIF(utm_medium, '') AS acquisition_medium,
        CASE
            WHEN (referer ILIKE '%apps.shopify.com%')
                THEN 'Shopify App Store'
            ELSE NULLIF(COALESCE(utm_source, referer), '')
        END AS acquisition_source
    FROM raw_install_events
    LEFT JOIN shops USING (shop_subdomain)
    WHERE tstamp_pt <= first_installed_at_pt + INTERVAL '60seconds'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY tstamp_pt ASC) = 1
),

combined_install_sources AS (
    SELECT
        shop_subdomain,
        acquisition_template,
        referrer_host,
        COALESCE(formatted_install_pageviews.tstamp_pt, formatted_install_events.tstamp_pt) AS tstamp_pt,
        COALESCE(formatted_install_pageviews.acquisition_campaign, formatted_install_events.acquisition_campaign) AS acquisition_campaign,
        COALESCE(formatted_install_pageviews.acquisition_content, formatted_install_events.acquisition_content) AS acquisition_content,
        COALESCE(formatted_install_pageviews.acquisition_referrer, formatted_install_events.acquisition_referrer) AS acquisition_referrer,
        COALESCE(formatted_install_pageviews.acquisition_first_page_path, formatted_install_events.acquisition_first_page_path) AS acquisition_first_page_path,
        COALESCE(formatted_install_pageviews.acquisition_source, formatted_install_events.acquisition_source) AS acquisition_source,
        COALESCE(formatted_install_pageviews.acquisition_medium, formatted_install_events.acquisition_medium) AS acquisition_medium
    FROM formatted_install_pageviews
    FULL OUTER JOIN formatted_install_events USING (shop_subdomain)
),

referrer_mapping as (
    SELECT * FROM {{ ref('referrer_mapping') }}
),

final AS (
    SELECT
        shop_subdomain,
        acquisition_referrer,
        acquisition_template,
        acquisition_content,
        acquisition_first_page_path,
        INITCAP(REPLACE(acquisition_campaign, '_', ' ')) AS acquisition_campaign,
        INITCAP(REPLACE(COALESCE(referrer_mapping.medium, acquisition_medium), '_', ' ')) AS acquisition_medium,
        INITCAP(COALESCE(referrer_mapping.source, acquisition_source)) AS acquisition_source,
        INITCAP(
            NULLIF(
                (
                    COALESCE(referrer_mapping.source, acquisition_source)
                    || ' - '
                    || COALESCE(referrer_mapping.medium, acquisition_medium)
                ),
                ' - '
            )
        ) AS acquisition_source_medium,
        COALESCE(acquisition_first_page_path ILIKE '/blog/%', FALSE) AS is_blog_referral
    FROM shops
    LEFT JOIN combined_install_sources USING (shop_subdomain)
    LEFT JOIN referrer_mapping
        ON combined_install_sources.referrer_host = referrer_mapping.host
)

SELECT *
FROM final
