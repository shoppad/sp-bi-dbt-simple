{#- cspell:words initcap -#}
{#
[x] get the first visit source for each shop (first touch).
[x] look at install events. find the session *right* before that AS the last touch. is it the same AS the first touch?
[x] get the session start source for each shop (last touch).
[x] get the first install event for each shop.
[x] todo: for pre-ga installs, get the first pageview for each shop FROM segment.
[x] count the number of sessions before install.
[x] tally the number of days FROM first to install.
[x] reformat columns to match the existing schema.
[x] re-add acquisition template.
[ ] Look for any and all pre-install search_ad surface_type events.
#}
WITH
shops AS (SELECT shop_subdomain, first_installed_at_pt FROM {{ ref("stg_shops") }}),

ga_attribution as (
    select
        shop_subdomain,
        {{ get_prefixed_columns(ref('int_ga_attribution'), 'ga') }}
    FROM {{ ref("int_ga_attribution") }}
),

segment_attribution as (
    select
        shop_subdomain,
        {{ get_prefixed_columns(ref('int_segment_attribution'), 'segment') }}
    FROM {{ ref("int_segment_attribution") }}
),

app_store_attribution AS (
    SELECT *
    FROM {{ ref('int_app_store_attribution') }}
),

mesa_install_records as (
    SELECT * FROM {{ ref("stg_mesa_install_records") }}
),

formatted_install_records as (
    SELECT mesa_install_records.*
    FROM mesa_install_records
    LEFT JOIN shops USING (shop_subdomain)
    HAVING
        mesa_install_record_at_pt
        <= first_installed_at_pt + interval '60seconds'
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY shop_subdomain ORDER BY mesa_install_record_at_pt ASC
        )
        = 1
),

combined_attribution AS (
    SELECT
        *,
        REPLACE(COALESCE(
            ga_first_touch_traffic_source_source,
            ga_first_touch_param_source,
            ga_first_touch_manual_source,
            segment_first_touch_traffic_source_source,
            app_store_install_traffic_source_source,
            app_store_install_param_source,
            app_store_install_manual_source,
            app_store_ad_click_traffic_source_source,
            app_store_ad_click_param_source,
            app_store_ad_click_manual_source,
            app_store_organic_click_traffic_source_source,
            app_store_organic_click_param_source,
            app_store_organic_click_manual_source,
            mesa_install_record_utm_source,
            ga_last_touch_traffic_source_source,
            ga_last_touch_param_source,
            ga_last_touch_manual_source,
            segment_last_touch_traffic_source_source
        ), 'www.', '') AS unified_traffic_source,
        COALESCE(
            ga_first_touch_traffic_source_medium,
            ga_first_touch_param_medium,
            ga_first_touch_manual_medium,
            app_store_install_traffic_source_medium,
            app_store_install_param_medium,
            app_store_install_manual_medium,
            segment_first_touch_traffic_source_medium,
            app_store_ad_click_traffic_source_medium,
            app_store_ad_click_param_medium,
            app_store_ad_click_manual_medium,
            app_store_organic_click_traffic_source_medium,
            app_store_organic_click_param_medium,
            app_store_organic_click_manual_medium,
            mesa_install_record_utm_medium,
            ga_last_touch_traffic_source_medium,
            ga_last_touch_param_medium,
            ga_last_touch_manual_medium,
            segment_last_touch_traffic_source_medium
        ) AS unified_traffic_medium,
        COALESCE(
            ga_first_touch_traffic_source_name,
            ga_first_touch_param_campaign,
            ga_first_touch_manual_campaign_name,
            app_store_install_traffic_source_name,
            app_store_install_param_campaign,
            app_store_install_manual_campaign_name,
            segment_first_touch_traffic_source_name,
            app_store_ad_click_traffic_source_name,
            app_store_ad_click_param_campaign,
            app_store_ad_click_manual_campaign_name,
            app_store_organic_click_traffic_source_name,
            app_store_organic_click_param_campaign,
            app_store_organic_click_manual_campaign_name,
            mesa_install_record_utm_campaign,
            ga_last_touch_traffic_source_name,
            ga_last_touch_param_campaign,
            ga_last_touch_manual_campaign_name,
            segment_last_touch_traffic_source_name
        ) AS unified_traffic_campaign,
        COALESCE(
            ga_first_touch_page_location,
            segment_first_touch_url,
            ga_last_touch_page_location,
            segment_last_touch_url
        ) AS unified_traffic_url,
        COALESCE(
            ga_first_touch_page_location_path,
            segment_first_touch_path,
            ga_last_touch_page_location_path,
            segment_last_touch_path
        ) AS unified_traffic_path,
        COALESCE(
            ga_first_touch_page_location_host,
            segment_first_touch_host,
            ga_last_touch_page_location_host,
            segment_last_touch_host
        ) AS unified_traffic_page_host,
        LEAST(
            ga_first_touch_at_pt,
            segment_first_touch_at_pt,
            ga_last_touch_at_pt,
            segment_last_touch_at_pt,
            app_store_install_at_pt,
            app_store_ad_click_at_pt,
            app_store_organic_click_at_pt,
            mesa_install_record_at_pt
        ) AS unified_first_touch_at_pt,
        COALESCE(
            ga_first_touch_app_store_surface_type,
            ga_last_touch_app_store_surface_type,
            app_store_ad_click_app_store_surface_type,
            app_store_organic_click_app_store_surface_type
        ) AS unified_app_store_surface_type,
        COALESCE(
            ga_first_touch_app_store_surface_detail,
            ga_last_touch_app_store_surface_detail,
            app_store_ad_click_app_store_surface_detail,
            app_store_organic_click_app_store_surface_detail
        ) AS unified_app_store_surface_detail,
        REPLACE(COALESCE(
            ga_first_touch_referrer_host,
            ga_last_touch_referrer_host,
            segment_first_touch_referrer_host,
            segment_last_touch_referrer_host,
            app_store_install_referrer_host,
            app_store_ad_click_referrer_host,
            app_store_organic_click_referrer_host
        ), 'www.', '') AS unified_referrer_host
    FROM shops
    LEFT JOIN formatted_install_records USING (shop_subdomain)
    LEFT JOIN ga_attribution USING (shop_subdomain)
    LEFT JOIN app_store_attribution USING (shop_subdomain)
    LEFT JOIN segment_attribution USING (shop_subdomain)
),

referrer_mapping as (select * FROM {{ ref("referrer_mapping") }}),

final AS (
    SELECT
        combined_attribution.* EXCLUDE (unified_traffic_source, unified_traffic_medium),

        {# Referrer Mapping #}
        INITCAP(
            COALESCE(
                referrer_mapping.source,
                IFF(
                    (unified_traffic_source IS NULL AND unified_referrer_host ILIKE '%shopify%')
                        OR
                        unified_traffic_source ILIKE '%shopify%',
                    'Shopify',
                    unified_traffic_source
                )
            )
        ) AS unified_traffic_source,
        INITCAP(
            COALESCE(
                referrer_mapping.medium,
                IFF(
                    (unified_traffic_medium IS NULL AND unified_referrer_host ILIKE '%apps.shopify.com%') OR unified_traffic_medium ILIKE '%apps.shopify.com%',
                    'app store',
                    unified_traffic_medium
                )
            )
        ) AS unified_traffic_medium,

        referrer_mapping.medium AS referrer_medium,
        referrer_mapping.source AS referrer_source
    FROM shops
    LEFT JOIN combined_attribution USING (shop_subdomain)
    LEFT JOIN
        referrer_mapping
            ON
                LOWER(REPLACE(combined_attribution.unified_traffic_source, 'www.', ''))
                    = lower(referrer_mapping.host)
                OR
                lower(REPLACE(combined_attribution.unified_referrer_host, 'www.', ''))
                    = lower(referrer_mapping.host)
)

SELECT
    * EXCLUDE (first_installed_at_pt),
        COALESCE((unified_traffic_url ILIKE '%getmesa.com/blog%' AND unified_traffic_medium = 'search'), FALSE) as is_blog_referral,
        TIMEDIFF(
            'days', unified_first_touch_at_pt, first_installed_at_pt
        ) AS days_to_install,
        COALESCE(
            unified_app_store_surface_type ilike '%search_ad%',
            FALSE
        ) AS is_app_store_search_ad_referral,
        CASE
            WHEN unified_traffic_url ILIKE '%getmesa.com/blog%' THEN 'Blog'
            WHEN unified_traffic_url ILIKE '%apps.shopify.com/mesa%' THEN 'Shopify App Store'
            WHEN unified_traffic_url ILIKE '%getmesa.com' THEN 'GetMesa Website'
            ELSE 'Other'
        END AS unified_landing_surface_area
FROM final
WHERE unified_traffic_source IS NOT NULL
