with
    user_id_matches as (
        select user_pseudo_id, user_id as shop_subdomain, shop_id as shopify_id
        from {{ source("mesa_ga4", "events") }}
        where user_id is not null or shop_id is not null
        qualify
            row_number() over (
                partition by user_pseudo_id, shop_subdomain, shop_id
                order by event_timestamp
            )
            = 1
    ),

    shops as (select shop_subdomain, shopify_id from {{ ref("stg_shops") }})

select user_id_matches.user_pseudo_id, shops.shopify_id, shops.shop_subdomain
from user_id_matches
inner join
    shops
    on (
        user_id_matches.shop_subdomain = shops.shop_subdomain
        or user_id_matches.shopify_id = shops.shopify_id
    )
