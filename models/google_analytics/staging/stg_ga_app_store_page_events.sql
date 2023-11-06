with
    source as (
        select
            user_pseudo_id,
            user_id,
            event_name,
            {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }}
            as event_timestamp_pt,
            page_location,
            parse_url(page_location) as page_params,
            parse_url(page_location):parameters:surface_detail::string
            as app_store_search_term,
            parse_url(page_location):parameters:surface_type::string
            as app_store_surface_type,
            parse_url(page_location):parameters:surface_intra_position::string
            as app_store_surface_intra_position,
            parse_url(page_location):parameters:surface_inter_position::string
            as app_store_surface_inter_position,
            parse_url(page_location):parameters:locale::string as app_store_locale,
            page_params:parameters:utm_content::string as utm_content,
            page_params:parameters:utm_campaign::string as utm_campaign,
            page_params:parameters:utm_medium::string as utm_medium,
            page_params:parameters:utm_source::string as utm_source,
            page_params:parameters:utm_term::string as utm_term,
            page_params:parameters:page_referrer::string as referrer,
            page_params:host::string as referrer_host,
            page_params:parameters:referrer_source::string as referrer_source,
            page_params:parameters:referrer_medium::string as referrer_medium,
            page_params:parameters:referrer_term::string as referrer_term,
            page_params:parameters:shop_id::string as shopify_id
        from {{ source("mesa_ga4", "events") }}
        where
            {# (page_location ILIKE '%apps.shopify.com%' AND event_name = 'page_view')
      OR #}
            event_name ilike 'shopify%'

    ),

    final as (

        select
            user_pseudo_id,
            shopify_id,
            case
                when app_store_surface_type is not null
                then 'Shopify App Store'
                else utm_source
            end as utm_source,
            app_store_locale,
            app_store_search_term,
            event_name,
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
            end as utm_campaign,
            case
                when app_store_surface_type = 'search_ad'
                then 'CPC'
                else coalesce(app_store_surface_type, utm_medium)
            end as app_store_surface_type
        from source
    )

select *
from final
