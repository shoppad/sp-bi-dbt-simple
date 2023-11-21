{# Note: This is the primary attribution channel before May 10, 2023 #}
with
    shops as (
        select shop_subdomain, first_installed_at_pt, shopify_id
        from {{ ref("stg_shops") }}
    ),

    segment_sessions as (
        select
            * replace (
                {{ pacific_timestamp("session_start_tstamp") }} as session_start_tstamp
            )
            rename(
                blended_user_id as shop_subdomain,
                session_start_tstamp as session_start_tstamp_pt
            )
        from {{ ref("segment_web_sessions") }}
    ),

    session_counts as (
        select
            shop_subdomain,
            coalesce(
                count_if(
                    session_start_tstamp_pt <= first_installed_at_pt + interval '60min'
                ),
                0
            ) as segment_sessions_til_install
        from shops
        left join segment_sessions using (shop_subdomain)
        group by 1
    ),

    first_touches_segment as (
        select * exclude first_installed_at_pt
        from {{ ref("stg_segment_first_visits") }}
        inner join shops using (shop_subdomain)
        where first_touch_at_pt <= first_installed_at_pt + interval '60min'
    ),

    last_touches_segment as (
        select * exclude first_installed_at_pt, shopify_id
        from {{ ref("stg_segment_last_touch_sessions_before_install") }}
        inner join shops using (shop_subdomain)
        where last_touch_at_pt <= first_installed_at_pt + interval '60min'
    ),

    final as (
        select *
        from first_touches_segment
        left join last_touches_segment using (shop_subdomain)
        left join session_counts using (shop_subdomain)
    )

select *
from final
