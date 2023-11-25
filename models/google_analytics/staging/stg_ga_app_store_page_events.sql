with
    user_matching as (select * from {{ ref("stg_anonymous_to_known_user_matching") }}),

    source as (
        select
            user_pseudo_id,
            user_id,
            event_name,
            name,
            shop_id as shopify_id,
            {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }}
            as event_timestamp_pt,
            page_location,
            parse_url(page_location) as page_params,
            nullif(
                lower(
                    {{ target.schema }}.url_decode(
                        page_params:parameters:surface_detail::string
                    )
                ),
                'undefined'
            ) as app_store_surface_detail,
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
            page_params:parameters:referrer_term::string as referrer_term
        from {{ source("mesa_ga4", "events") }}
        where
            page_location ilike '%apps.shopify.com%'
            or event_name ilike 'shopify%'
            or page_location ilike '%surface_%'

    ),

    final as (

        select
            source.* exclude (utm_source, utm_campaign),
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
            )
    )

select *
from final
