with

    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

    first_install_events as (
        select * rename event_timestamp_pt as first_install_timestamp_pt
        from {{ ref("stg_segment_install_events") }}
        qualify
            row_number() over (partition by shop_subdomain order by event_timestamp_pt)
            = 1
    ),

    segment_sessions as (
        select
            * replace (
                {{ pacific_timestamp("session_start_tstamp") }} as session_start_tstamp
            )
            rename
                blended_user_id as shop_subdomain,
                session_start_tstamp as session_start_tstamp_pt
        from {{ ref("segment_web_sessions") }}
    ),

    segment_sessions_before_install as (
        select *
        from shops
        left join segment_sessions using (shop_subdomain)
        where session_start_tstamp_pt <= first_installed_at_pt + interval '1hour'
    ),

    last_touch_sessions as (
        select * exclude (rn)
        from
            (
                select
                    segment_sessions_before_install.*,
                    row_number() over (
                        partition by first_install_events.shop_subdomain
                        order by
                            segment_sessions_before_install.session_start_tstamp_pt desc
                    ) as rn
                from segment_sessions_before_install
                inner join first_install_events using (shop_subdomain)
                where
                    segment_sessions_before_install.session_start_tstamp_pt
                    <= first_install_events.first_install_timestamp_pt
                qualify rn = 1
            ) as t
    ),

    formatted_last_touch_sessions as (
        select
            shop_subdomain,
            session_start_tstamp_pt as last_touch_at_pt,
            parse_url(first_page_url) as page_params,
            split_part(first_page_url, '//', 2) as last_touch_url,
            first_page_url_host as last_touch_host,
            first_page_url_path as last_touch_path,
            first_page_url_query AS last_touch_query,
            utm_content as last_touch_traffic_source_content,
            utm_campaign as last_touch_traffic_source_name,
            coalesce(utm_medium, referrer_medium) as last_touch_traffic_source_medium,
            coalesce(utm_source, referrer_source) as last_touch_traffic_source_source,
            referrer as last_touch_referrer,
            referrer_host as last_touch_referrer_host,
            device_category as last_touch_device_category,
            NULLIF(CASE
                WHEN last_touch_url ILIKE '%getmesa.com/blog%' OR last_touch_host = 'blog.getmesa.com' THEN 'Blog'
                WHEN last_touch_url ILIKE '%apps.shopify.com/mesa%' THEN 'Shopify App Store'
                WHEN last_touch_url ILIKE '%docs.getmesa%' THEN 'Support Site'
                WHEN last_touch_url ILIKE '%getmesa.com/' THEN 'Homepage'
                WHEN last_touch_url ILIKE '%app.getmesa%' THEN 'Inside App (Untrackable)'
                WHEN last_touch_url ILIKE '%getmesa.com%' THEN initcap(SPLIT_PART(last_touch_path, '/', 2))
                WHEN last_touch_url IS NULL THEN '(Untrackable)'
                ELSE last_touch_url
                END, '') AS last_touch_page_type
        from last_touch_sessions
    )

select * exclude (page_params)
from formatted_last_touch_sessions
