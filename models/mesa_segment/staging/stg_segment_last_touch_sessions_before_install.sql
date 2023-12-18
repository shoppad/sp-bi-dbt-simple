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
            utm_content as last_touch_content,
            utm_campaign as last_touch_campaign,
            coalesce(utm_medium, referrer_medium) as last_touch_medium,
            coalesce(utm_source, referrer_source) as last_touch_source,
            nullif(
                TRIM(lower(
                    {{ target.schema }}.url_decode(
                        cast(page_params:parameters:surface_detail as string)
                    )
                )),
                'undefined'
            ) as last_touch_app_store_search_term,
            cast(
                page_params:parameters:surface_type as string
            ) as last_touch_app_store_surface_type,
            cast(
                page_params:parameters:surface_intra_position as string
            ) as last_touch_app_store_surface_intra_position,
            cast(
                page_params:parameters:surface_inter_position as string
            ) as last_touch_app_store_surface_inter_position,
            cast(
                page_params:parameters:locale as string
            ) as last_touch_app_store_locale,
            referrer as last_touch_referrer,
            referrer_host as last_touch_referrer_host,
            device_category as last_touch_device_category
        from last_touch_sessions
    )

select * exclude (page_params)
from formatted_last_touch_sessions
