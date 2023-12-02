with
    shop_anonymous_keys as (
        select * from {{ ref("stg_anonymous_to_known_user_matching") }}
    ),

    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

    first_visits as (
        select *
        from {{ ref("ga4_events") }}
        where (event_name = 'first_visit') and not (page_location ilike '%.pages.dev%')
    ),

    shop_first_visits as (

        select
            user_pseudo_id::string as user_pseudo_id,
            page_location::string as page_location,
            event_timestamp_pt,

            {# Attribution #}
            param_content as utm_content,
            param_term as utm_term,
            coalesce(traffic_source_name, param_campaign) as utm_campaign,
            coalesce(traffic_source_medium, param_medium) as utm_medium,
            coalesce(traffic_source_source, param_source) as utm_source,
            page_referrer as first_touch_referrer,
            parse_url(first_touch_referrer):host::string as first_touch_referrer_host,

            {# App Store #}
            * ilike 'app_store%'

        from first_visits
    ),

    formatted_first_visits as (
        select
            user_pseudo_id,
            event_timestamp_pt as first_touch_at_pt,
            page_location as acquisition_first_page_path,
            split_part(page_location, '//', 2) as first_touch_url,
            split_part(first_touch_url, '/', 1) as first_touch_host,
            split_part(first_touch_url, '?', 1) as first_touch_path,
            utm_content as first_touch_content,
            utm_campaign as first_touch_campaign,
            utm_medium as first_touch_medium,
            utm_source as first_touch_source,
            app_store_surface_detail as first_touch_app_surface_detail,
            app_store_surface_type as first_touch_app_store_surface_type,
            app_store_surface_intra_position
            as first_touch_app_store_surface_intra_position,
            app_store_surface_inter_position
            as first_touch_app_store_surface_inter_position,
            app_store_locale as first_touch_app_store_locale,
            first_touch_referrer,
            first_touch_referrer_host
        from shop_first_visits
    )

select *
from formatted_first_visits
inner join shop_anonymous_keys using (user_pseudo_id)
qualify row_number() over (partition by shop_subdomain order by first_touch_at_pt) = 1
