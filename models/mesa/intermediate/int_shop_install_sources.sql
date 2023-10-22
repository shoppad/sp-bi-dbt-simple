{#- cSpell:words INITCAP -#}

WITH
shops AS (
    SELECT
        stg_shops.shop_subdomain,
        first_installed_at_pt
    FROM {{ ref('stg_shops') }}
),

first_visits_ga4 AS (
    SELECT
        * EXCLUDE (page_location),
        page_location AS acquisition_first_page_path
    FROM {{ ref('stg_ga_first_visits') }}
),

ga_installations AS (
    SELECT
        * EXCLUDE (event_timestamp),
        event_timestamp AS created_at
    FROM {{ ref('stg_ga_install_events') }}
    {# LEFT JOIN {{ ref('stg_ga_app_store_page_events') }} USING (user_pseudo_id) #}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC, referrer_host, utm_medium) = 1
),

unified_install_events AS (
    SELECT
        shop_subdomain,
        app_store_surface_type,
        NULL AS app_store_search_term,
        utm_content,
        referrer,
        referrer_host,
        referrer_source,
        referrer_medium,
        utm_medium
    FROM first_visits_ga4

    UNION

     SELECT
        shop_subdomain,
        NULL AS app_store_surface_type,
        NULL AS app_store_search_term,
        utm_content,
        referrer,
        referrer_host,
        referrer_source,
        referrer_medium,
        utm_medium
    FROM ga_installations
),

formatted_install_pageviews AS (
    SELECT
        ga_installations.shop_subdomain,
        {{ pacific_timestamp('event_timestamp') }} AS tstamp_pt,
        COALESCE(app_store_search_term, utm_content) AS acquisition_content,
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
        COALESCE(app_store_surface_type, referrer_medium, utm_medium) AS acquisition_medium
    FROM ga_installations
    LEFT JOIN first_visits_ga4 USING (shop_subdomain)
    LEFT JOIN shops USING (shop_subdomain)
    WHERE
        acquisition_source IS NOT NULL
        AND tstamp_pt <= first_installed_at_pt + INTERVAL '60seconds'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY tstamp_pt ASC) = 1
),

m3_mesa_installs_templates AS (
    SELECT
        uuid AS shop_subdomain,
        {# template AS acquisition_template, #}
        {# referer, #}
        *
    FROM {{ source('mongo_sync', 'mesa_install_events') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY created_at ASC) = 1
),

formatted_install_events AS (
    SELECT
        ga_installations.shop_subdomain,
        {{ pacific_timestamp('created_at') }} AS tstamp_pt,
        acquisition_template,
        NULL AS acquisition_content,
        NULLIF(utm_campaign, '') AS acquisition_campaign,
        NULLIF(referer, '') AS acquisition_referrer,
        NULLIF(utm_medium, '') AS acquisition_medium,
        CASE
            WHEN (referer ILIKE '%apps.shopify.com%')
                THEN 'Shopify App Store'
            ELSE NULLIF(COALESCE(utm_source, referer), '')
        END AS acquisition_source
    FROM ga_installations
    LEFT JOIN shops USING (shop_subdomain)
    LEFT JOIN m3_mesa_installs_templates USING (shop_subdomain)
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
        {# COALESCE(formatted_install_pageviews.acquisition_first_page_path, formatted_install_events.acquisition_first_page_path) AS acquisition_first_page_path, #}
        COALESCE(formatted_install_pageviews.acquisition_source, formatted_install_events.acquisition_source) AS acquisition_source,
        COALESCE(formatted_install_pageviews.acquisition_medium, formatted_install_events.acquisition_medium) AS acquisition_medium
    FROM formatted_install_pageviews
    FULL OUTER JOIN formatted_install_events USING (shop_subdomain)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY COALESCE(formatted_install_pageviews.tstamp_pt, formatted_install_events.tstamp_pt) ASC) = 1
),

referrer_mapping as (
    SELECT * FROM {{ ref('referrer_mapping') }}
),

final AS (
    SELECT
        shops.shop_subdomain,
        acquisition_referrer,
        acquisition_template,
        acquisition_content,
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
        COALESCE(acquisition_first_page_path ILIKE '/blog/%', FALSE) AS is_blog_referral,
        first_visits_ga4.* EXCLUDE (shop_subdomain)
    FROM shops
    LEFT JOIN first_visits_ga4 USING (shop_subdomain)
    LEFT JOIN combined_install_sources USING (shop_subdomain)
    LEFT JOIN referrer_mapping
        ON combined_install_sources.referrer_host = referrer_mapping.host
)

SELECT * FROM unified_install_events
{# SELECT COUNT(DISTINCT shop_subdomain)

FROM app_store_installed_user_id_correlations #}
