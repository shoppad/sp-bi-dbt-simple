with
    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

    segment_sessions as (
        select
            * exclude session_start_tstamp rename(blended_user_id as shop_subdomain),
            {{ pacific_timestamp("session_start_tstamp") }} as session_start_tstamp_pt
        from {{ ref("segment_web_sessions") }}
    ),

    first_segment_visits as (
        select * exclude first_installed_at_pt
        from shops
        left join segment_sessions using (shop_subdomain)
        where session_start_tstamp_pt <= shops.first_installed_at_pt + interval '1 hour'
        qualify
            row_number() over (
                partition by shop_subdomain order by session_start_tstamp_pt asc
            )
            = 1
    ),

    formatted_first_visits as (
        select
            shop_subdomain,
            session_start_tstamp_pt as first_touch_at_pt,
            first_page_url as acquisition_first_page_path,
            parse_url(first_page_url) as page_params,
            split_part(first_page_url, '//', 2) as first_touch_url,
            first_page_url_host as first_touch_host,
            first_page_url_path as first_touch_path,
            utm_content as first_touch_content,
            utm_campaign as first_touch_campaign,
            coalesce(utm_medium, referrer_medium) as first_touch_medium,
            coalesce(utm_source, referrer_source) as first_touch_source,
            lower(to_varchar(
                page_params:parameters:surface_detail
            )) as first_touch_app_store_search_term,
            to_varchar(
                page_params:parameters:surface_type
            ) as first_touch_app_store_surface_type,
            to_varchar(
                page_params:parameters:surface_intra_position
            ) as first_touch_app_store_surface_intra_position,
            to_varchar(
                page_params:parameters:surface_inter_position
            ) as first_touch_app_store_surface_inter_position,
            to_varchar(page_params:parameters:locale) as first_touch_app_store_locale,
            referrer as first_touch_referrer,
            referrer_host as first_touch_referrer_host
        from first_segment_visits
    )

select * exclude page_params
from formatted_first_visits
