with
    user_matching as (select * from {{ ref("stg_anonymous_to_known_user_matching") }}),

    installation_events as (
        select user_pseudo_id, shop_subdomain, shopify_id, event_timestamp_pt
        from {{ ref("stg_ga4_events") }}
        where event_name = 'getmesa_install_convert'
    )

select
    installation_events.* exclude (shop_subdomain, shopify_id)
    rename event_timestamp_pt as getmesa_install_convert_event_timestamp_pt,
    user_matching.shop_subdomain,
    user_matching.shopify_id
from installation_events
inner join
    user_matching
    on (
        installation_events.user_pseudo_id = user_matching.user_pseudo_id
        or installation_events.shopify_id = user_matching.shopify_id
        or installation_events.shop_subdomain = user_matching.shop_subdomain
    )
qualify
    row_number() over (
        partition by user_matching.shop_subdomain order by event_timestamp_pt asc
    )
    = 1
