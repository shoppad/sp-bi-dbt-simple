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
with
shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

ga_attribution as (
    select
        shop_subdomain,
        {{ get_prefixed_columns(ref('int_ga_attribution'), 'ga') }}
    from {{ ref("int_ga_attribution") }}
),

segment_attribution as (
    select
        shop_subdomain,
        {{ get_prefixed_columns(ref('int_segment_attribution'), 'segment') }}
    from {{ ref("int_segment_attribution") }}
),

app_store_attribution AS (
    SELECT *
    FROM {{ ref('int_app_store_attribution') }}
),

mesa_install_records as (
    SELECT * FROM {{ ref("stg_mesa_install_records") }}
),

formatted_install_records as (
    select mesa_install_records.*
    from mesa_install_records
    left join shops using (shop_subdomain)
    having
        mesa_install_record_at_pt
        <= first_installed_at_pt + interval '60seconds'
    qualify
        row_number() over (
            partition by shop_subdomain order by mesa_install_record_at_pt asc
        )
        = 1
),

combined_attribution as (
    select
        * EXCLUDE (first_installed_at_pt),
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
    from shops
    left join formatted_install_records using (shop_subdomain)
    left join ga_attribution using (shop_subdomain)
    LEFT JOIN app_store_attribution USING (shop_subdomain)
    left join segment_attribution using (shop_subdomain)
),

referrer_mapping as (select * from {{ ref("referrer_mapping") }}),

final as (
    select
        combined_attribution.* EXCLUDE (unified_traffic_source, unified_traffic_medium),

            {# Referrer Mapping #}
        lower(
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
        lower(
            COALESCE(
                referrer_mapping.medium,
                IFF(
                    (unified_traffic_medium IS NULL AND unified_referrer_host ILIKE '%shopify%')
                        OR
                        unified_traffic_medium ILIKE '%shopify%',
                    'App Store',
                    unified_traffic_medium
                )
            )
        ) AS unified_traffic_medium,

            {# Referral Type #}
        coalesce(
            unified_traffic_path ilike '%blog%', FALSE
        ) as is_blog_referral,
        timediff(
            'days', unified_first_touch_at_pt, first_installed_at_pt
        ) as days_to_install,
        coalesce(
            unified_app_store_surface_type ilike '%search_ad%',
            FALSE
        ) as is_app_store_search_ad_referral,
        referrer_mapping.medium as referrer_medium,
        referrer_mapping.source as referrer_source
    from shops
    left join combined_attribution using (shop_subdomain)
    left join
        referrer_mapping
            on
                LOWER(REPLACE(combined_attribution.unified_traffic_source, 'www.', ''))
                    = lower(referrer_mapping.host)
                OR
                lower(REPLACE(combined_attribution.unified_referrer_host, 'www.', ''))
                    = lower(referrer_mapping.host)
)

select * from final
