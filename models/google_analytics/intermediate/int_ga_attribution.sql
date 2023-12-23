with

    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

    {# TODO: Check *all* sessions for an app_store_ad session or pageview and mark is_app_store_ad_acqusition #}
    session_counts as (
        select
            shop_subdomain,
            coalesce(
                count_if(
                    event_timestamp_pt <= first_installed_at_pt + interval '60min'
                ),
                0
            ) as ga4_sessions_til_install
        from shops
        left join {{ ref("stg_ga_session_starts") }} using (shop_subdomain)
        group by 1
    ),

    first_touches_ga4 as (select * from {{ ref("stg_ga_first_visits") }}),

    last_touches_ga4 as (
        select * from {{ ref("stg_last_touch_ga_sessions_before_install") }}
    ),

    final as (

        select * exclude (user_pseudo_id)
        from first_touches_ga4
        left join last_touches_ga4 using (shop_subdomain)
        left join session_counts using (shop_subdomain)
    )

select * EXCLUDE (parsed_url)
from final
