with
    shop_anonymous_keys as (
        select * from {{ ref("stg_anonymous_to_known_user_matching") }}
    ),

    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

    first_visits as (
        select *
        from {{ source("mesa_ga4", "events") }}
        where event_name = 'first_visit' and not page_location ilike '%.pages.dev%'
    ),

    shop_first_visits as (

        select
            user_pseudo_id::string as user_pseudo_id,
            page_location::string as page_location,
            {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }}::timestamp
            as event_timestamp_pt,
            parse_url(page_location::string) as page_params,
            page_params:parameters:utm_content::string as utm_content,
            page_params:parameters:utm_campaign::string as utm_campaign,
            page_params:parameters:utm_medium::string as utm_medium,
            page_params:parameters:utm_source::string as utm_source,
            nullif(
                lower(
                    {{ target.schema }}.url_decode(
                        page_params:parameters:surface_detail::string
                    )
                ),
                'undefined'
            ) as app_store_search_term,
            page_params:parameters:surface_type::string as app_store_surface_type,
            page_params:parameters:surface_intra_position::string
            as app_store_surface_intra_position,
            page_params:parameters:surface_inter_position::string
            as app_store_surface_inter_position,
            page_params:parameters:locale::string as app_store_locale,
            page_referrer as first_touch_referrer,
            parse_url(first_touch_referrer):host::string as first_touch_referrer_host
        from shops
        left join first_visits
        qualify
            row_number() over (
                partition by user_pseudo_id, event_name, event_timestamp
                order by param_source, name, __hevo__loaded_at
            )
            = 1
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
            app_store_search_term as first_touch_app_store_search_term,
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
