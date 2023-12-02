with
    stg_shops as (
        select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}
    ),

    user_matching as (select * from {{ ref("stg_anonymous_to_known_user_matching") }}),

    session_starts as (
        select
            user_pseudo_id,
            page_location,
            event_timestamp_pt,

            {# URL parts #}
            split_part(page_location, '//', 2) as page_url,
            split_part(page_url, '/', 1) as page_host,
            split_part(page_url, '?', 1) as page_path,

            {# Attribution #}
            coalesce(traffic_source_name, param_campaign) as utm_campaign,
            coalesce(traffic_source_medium, param_medium) as utm_medium,
            coalesce(traffic_source_source, param_source) as utm_source,
            param_content as utm_content,
            param_term as utm_term,
            page_referrer,
            * ilike 'referrer%',

            {# App Store #}
            * ilike 'app_store%'

        from {{ ref("ga4_events") }}
        where event_name = 'session_start'
    )

select *
from session_starts
inner join user_matching using (user_pseudo_id)
