with

    ga4_events as (
        select
            * exclude (
                event_timestamp,
                __hevo__ingested_at,
                __hevo__loaded_at,
                surface_detail,
                surface_type
            )
            rename (
                name as traffic_source_name,
                medium as traffic_source_medium,
                source as traffic_source_source,
                category as device_category,
                __hevo_id as event_id,
                user_id as shop_subdomain,
                shop_id as shopify_id
            ),
            {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }}
            as event_timestamp_pt,

            {# Attribution #}
            parse_url(page_location) as page_params,
            page_params:parameters:utm_content::STRING as param_content,
            page_params:parameters:utm_term::STRING as param_term,
            page_params:parameters:page_referrer::STRING as referrer,
            page_params:host::STRING as referrer_host,
            page_params:parameters:referrer_source::STRING as referrer_source,
            page_params:parameters:referrer_medium::STRING as referrer_medium,
            page_params:parameters:referrer_term::STRING as referrer_term,

            {# App Store #}
            TRIM(
                LOWER(
                    {{ target.schema }}.URL_DECODE(
                        COALESCE(
                            NULLIF(surface_detail, 'undefined'),
                            page_params:parameters:surface_detail::STRING
                        )
                    )
                )
            ) AS app_store_surface_detail,
            coalesce(
                nullif(surface_type, ''), page_params:parameters:surface_type::STRING
            ) as app_store_surface_type,
            page_params:parameters:surface_intra_position::STRING
            as app_store_surface_intra_position,
            page_params:parameters:surface_inter_position::STRING
            as app_store_surface_inter_position,
            page_params:parameters:locale::STRING as app_store_locale
        FROM {{ source("mesa_ga4", "events") }}
        WHERE ga_session_id is not NULL AND (page_location IS NULL OR NOT page_location ilike '%.pages.dev%')
    )

    {% set not_empty_string_fields = [
        "param_campaign",
        "param_source",
        "param_medium",
        "param_content",
        "param_term",
        "traffic_source_name",
        "traffic_source_source",
        "traffic_source_medium",
        "referrer",
        "referrer_host",
        "referrer_source",
        "referrer_medium",
        "shop_subdomain",
    ] %}
select
    *
    exclude page_params replace (
        {% for field in not_empty_string_fields %}
            nullif({{ field }}, '') as {{ field }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
from ga4_events
