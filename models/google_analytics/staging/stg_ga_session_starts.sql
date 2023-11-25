with
    stg_shops as (
        select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}
    ),

    user_matching as (select * from {{ ref("stg_anonymous_to_known_user_matching") }}),

    session_starts as (
        select
            user_pseudo_id,
            page_location,
            {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }}
            as event_timestamp_pt,
            split_part(page_location, '//', 2) as page_url,
            split_part(page_url, '/', 1) as page_host,
            split_part(page_url, '?', 1) as page_path,

            parse_url(page_location) as page_params,
            page_params:parameters:utm_content::string as utm_content,
            page_params:parameters:utm_campaign::string as utm_campaign,
            coalesce(page_params:parameters:utm_medium::string, medium) as utm_medium,
            coalesce(page_params:parameters:utm_source::string, source) as utm_source,
            nullif(
                lower(
                    {{ target.schema }}.url_decode(
                        page_params:parameters:surface_detail::string
                    )
                ),
                'undefined'
            ) as app_store_surface_detail,
            page_params:parameters:surface_type::string as app_store_surface_type,
            page_params:parameters:surface_intra_position::string
            as app_store_surface_intra_position,
            page_params:parameters:surface_inter_position::string
            as app_store_surface_inter_position,
            page_params:parameters:locale::string as app_store_locale,
            page_referrer

        from {{ source("mesa_ga4", "events") }}
        where event_name = 'session_start' and not page_location ilike '%.pages.dev%'
        qualify
            row_number() over (
                partition by user_pseudo_id, event_name, event_timestamp
                order by source, name, __hevo__loaded_at
            )
            = 1
    )

select * exclude page_params
from session_starts
inner join user_matching using (user_pseudo_id)
