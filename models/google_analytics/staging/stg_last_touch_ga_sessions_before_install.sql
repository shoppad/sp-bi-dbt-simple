with
    first_install_events as (
        select
            * rename(
                getmesa_install_convert_event_timestamp_pt as first_install_timestamp_pt
            )
        from {{ ref("stg_ga_install_events") }}
        qualify
            row_number() over (
                partition by shop_subdomain
                order by getmesa_install_convert_event_timestamp_pt
            )
            = 1
    ),

    session_starts as (select * from {{ ref("stg_ga_session_starts") }}),

    last_touch_sessions as (
        select * exclude (rn)
        from
            (
                select
                    session_starts.*,
                    row_number() over (
                        partition by first_install_events.shop_subdomain
                        order by session_starts.event_timestamp_pt desc
                    ) as rn
                from session_starts
                inner join first_install_events using (shop_subdomain)
                where
                    session_starts.event_timestamp_pt
                    <= first_install_events.first_install_timestamp_pt
                qualify rn = 1
            ) as t
    ),

    formatted_last_touch_session_starts as (
        select
            shop_subdomain,
            event_timestamp_pt as last_touch_at_pt,
            split_part(page_location, '//', 2) as last_touch_url,
            split_part(last_touch_url, '/', 1) as last_touch_host,
            split_part(last_touch_url, '?', 1) as last_touch_path,
            utm_content as last_touch_content,
            utm_campaign as last_touch_campaign,
            utm_medium as last_touch_medium,
            utm_source as last_touch_source,
            app_store_surface_detail as last_touch_app_store_search_term,
            app_store_surface_type as last_touch_app_store_surface_type,
            app_store_surface_intra_position
            as last_touch_app_store_surface_intra_position,
            app_store_locale as last_touch_app_store_locale,
            app_store_surface_inter_position
            as last_touch_app_store_surface_inter_position,
            page_referrer as last_touch_referrer,
            parse_url(last_touch_referrer):host::string as last_touch_referrer_host
        from last_touch_sessions
    )

select *
from formatted_last_touch_session_starts
