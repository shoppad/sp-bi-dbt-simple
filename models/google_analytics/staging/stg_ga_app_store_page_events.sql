with
    user_matching as (select * from {{ ref("stg_anonymous_to_known_user_matching") }}),

    source as (
        select
            user_pseudo_id,
            shop_subdomain,
            event_name,
            shopify_id,
            event_timestamp_pt,
            page_location,

            {# Attribution #}
            coalesce(traffic_source_name, param_campaign) as utm_campaign,
            coalesce(traffic_source_medium, param_medium) as utm_medium,
            coalesce(traffic_source_source, param_source) as utm_source,
            param_content as utm_content,
            param_term as utm_term,
            * ilike 'referrer%',

            {# App Store #}
            * ilike 'app_store%'
        from {{ ref("ga4_events") }}
        where
            page_location ilike '%apps.shopify.com%'
            or event_name ilike 'shopify%'
            or page_location ilike '%surface_%'

    ),

    final as (

        select
            source.* exclude (utm_source, utm_campaign, shop_subdomain),
            user_matching.shop_subdomain,
            case
                when app_store_surface_type is not null
                then 'Shopify App Store'
                else utm_source
            end as utm_source,

            case
                when app_store_surface_intra_position is not null
                then
                    concat(
                        'Intra pos:',
                        app_store_surface_intra_position,
                        ' / Inter pos:',
                        app_store_surface_inter_position
                    )
                else utm_campaign
            end as utm_campaign
        from source
        inner join
            user_matching
            on (
                source.user_pseudo_id = user_matching.user_pseudo_id
                or source.shopify_id = user_matching.shopify_id
                or source.shop_subdomain = user_matching.shop_subdomain
            )
    )

select *
from final
