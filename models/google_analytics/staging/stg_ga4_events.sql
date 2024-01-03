with
    raw_ga4_events AS (
        SELECT *
        FROM {{ source("mesa_ga4", "events") }}
        WHERE ga_session_id is not NULL AND (page_location IS NULL OR NOT page_location ilike '%.pages.dev%')
    ),

    ga4_events as (
        select
            * exclude (
                event_timestamp,
                __hevo__ingested_at,
                __hevo__loaded_at,
                surface_detail,
                surface_type,
                page_referrer,
                user_id,
                page_location,
                shop_id
            )
            rename (
                __hevo_id as event_id,
                name as traffic_source_name,
                medium as traffic_source_medium,
                source as traffic_source_source,
                category as device_category,
                language as device_language,
                country AS location_country,
                city AS location_city,
                continent AS location_continent,
                metro AS location_metro
            ),
            shop_id::STRING as shopify_id,
            {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }}
                as event_timestamp_pt,

            {# Conditionally set shop_subdomain #}
            coalesce(user_id, COALESCE(
                        NULLIF(REGEXP_SUBSTR(page_location, 'shop=(.*?)\.myshopify\.com', 1, 1, 'ie'), ''),
                        NULLIF(REGEXP_SUBSTR(page_referrer, 'shop=(.*?)\.myshopify\.com', 1, 1, 'ie'), '')
                    )) AS shop_subdomain,

            {# Page components #}
            parse_url(page_location) as parsed_url,
            parsed_url:host::STRING as page_location_host,
            '/' || parsed_url:path as page_location_path,
            '?' || parsed_url:query as page_location_query,
            page_location_host || page_location_path AS page_location_url,
            page_location_host || page_location_path || COALESCE(page_location_query, '') AS page_location,

            {# Attribution #}
            parsed_url:parameters:utm_content::STRING as param_content,
            parsed_url:parameters:utm_term::STRING as param_term,

            {# Referrer #}
            parse_url(page_referrer) as parsed_referrer,
            parsed_referrer:host::STRING as referrer_host,
            '/' || parsed_referrer:path as referrer_path,
            '?' || parsed_referrer:query as referrer_query,
            referrer_host || referrer_path AS referrer_url,
            referrer_host || referrer_path || COALESCE(referrer_query, '') AS referrer_full,

            {# App Store #}
            TRIM(
                {{ target.schema }}.URL_DECODE(
                    NULLIF(
                        LOWER(
                            COALESCE(
                                surface_detail,
                                parsed_url:parameters:surface_detail::STRING
                            )
                        ),
                        'undefined'
                    )
                )
            ) AS app_store_surface_detail,
            coalesce(
                nullif(surface_type, ''), parsed_url:parameters:surface_type::STRING
            ) as app_store_surface_type,
            parsed_url:parameters:surface_intra_position::STRING
            as app_store_surface_intra_position,
            parsed_url:parameters:surface_inter_position::STRING
            as app_store_surface_inter_position,
            parsed_url:parameters:locale::STRING as app_store_locale
        FROM raw_ga4_events
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
        "referrer_host",
        "referrer_full",
        "referrer_url",
        "referrer_query",
        "referrer_path",
        "shop_subdomain"
    ] %}
select
    *
    exclude (parsed_url, parsed_referrer)
    replace (
        {% for field in not_empty_string_fields %}
            nullif({{ field }}, '') as {{ field }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
from ga4_events
