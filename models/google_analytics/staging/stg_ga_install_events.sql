with
    user_matching as (select * from {{ ref("stg_anonymous_to_known_user_matching") }}),

    installation_events as (
        select
            user_pseudo_id,
            user_id as shop_subdomain,
            shop_id as shopify_id,
            {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }}
            as event_timestamp_pt
        from {{ source("mesa_ga4", "events") }}
        where event_name = 'getmesa_install_convert'
        qualify
            row_number() over (
                partition by user_pseudo_id, event_timestamp
                order by param_source, name, __hevo__loaded_at
            )
            = 1
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
