with
    raw_ga4_events AS (
        SELECT *
        FROM {{ source("mesa_ga4", "events") }}

        {# You have to put page_location IS NULL or the next condition will nuke NULLs #}
        WHERE page_location IS NULL OR (
                page_location NOT ilike '%.pages.dev%'
                AND
                page_location NOT ilike '%2create.studio%'
                AND
                page_location NOT ILIKE '%wp-staging.net%'
                AND
                {# Remove referral spam #}
                NOT (page_location ILIKE 'https://getmesa%' AND name ILIKE '%referral%')
            )
        {# TODO: Use the above only for everything after the first ga_session_id is present.  #}
        {# TODO: Then create another condition that looks to all the UA (pre-GA4 events) before that date. #}

        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
            AND {{ pacific_timestamp("TO_TIMESTAMP(event_timestamp)") }} > '{{ get_max_updated_at('event_timestamp_pt') }}'
        {% endif %}

        {# We still get duplicates sometimes. #}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, event_name, event_timestamp ORDER BY source) = 1
    ),

    filtered_raw_ga4_events AS (
        SELECT *
        FROM raw_ga4_events
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY
                    user_pseudo_id,
                    event_name,
                    event_timestamp
                ORDER BY
                    NOT (ga_session_id IS NULL) DESC,
                    NOT (category IS NULL) DESC,
                    NOT (manual_medium IS NULL) DESC,
                    NOT (param_medium IS NULL) DESC,
                    NOT (name IS NULL) DESC
            ) = 1
    ),

    ga4_events as (
        select
            * exclude (
                event_timestamp,
                __hevo__ingested_at,
                __hevo__loaded_at,
                surface_detail,
                surface_type,
                medium,
                name,
                source,
                param_source,
                param_medium,
                page_referrer,
                user_id,
                page_location,
                shop_id
            )
            rename (
                __hevo_id as event_id,
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
            {# TODO: Coalesce the surface_type and surface_detail from past pageloads. #}



            {# Page components #}
            parse_url(page_location) as parsed_url,
            parsed_url:host::STRING as page_location_host,
            '/' || RTRIM(parsed_url:path, '/') as page_location_path,
            '?' || parsed_url:query as page_location_query,
            page_location_host || page_location_path AS page_location_url,
            page_location_host || page_location_path || COALESCE(page_location_query, '') AS page_location,

            {# Attribution #}
            parsed_url:parameters:utm_content::STRING as param_content,
            parsed_url:parameters:utm_term::STRING as param_term,

            {# Referrer #}



                {# Filter out the referral medium and source if it's our surface area. #}
            CASE
                WHEN param_source = 'shopify_forums' THEN 'referral'
                else medium
            END as traffic_source_medium,
            CASE
                WHEN param_source = 'shopify_forums' THEN 'Shopify Forums'
                ELSE name
            END as traffic_source_name,

            CASE
                WHEN param_source = 'shopify_forums' THEN 'community.shopify.com'
                ELSE source
            END as traffic_source_source,

            CASE
                WHEN param_source = 'shopify_forums' THEN parse_url('https://community.shopify.com/')
                WHEN traffic_source_medium ILIKE '%referral%' OR traffic_source_name ILIKE '%referral%'
                    AND (page_referrer ILIKE '%apps.shopify.com%' OR page_referrer ILIKE '%getmesa.com%')
                    THEN '{}'::VARIANT
                ELSE parse_url(page_referrer)
            END as parsed_referrer,

            parsed_referrer:host::STRING as page_referrer_host,
            '/' || parsed_referrer:path as page_referrer_path,
            '?' || parsed_referrer:query as page_referrer_query,
            page_referrer_host || page_referrer_path AS page_referrer_url,
            page_referrer_host || page_referrer_path || COALESCE(page_referrer_query, '') AS page_referrer_full,

            CASE
                WHEN param_source = 'shopify_forums' THEN 'referral'
                ELSE param_medium
            END as param_medium,

            CASE
                WHEN param_source = 'shopify_forums' THEN 'referral'
                ELSE param_source
            END as param_source,

            {# App Store #}
            TRIM(
                {{ target.schema }}.URL_DECODE(
                    NULLIF(
                        LOWER(
                            COALESCE(
                                surface_detail,
                                parsed_url:parameters:surface_detail,
                                parsed_referrer:parameters:surface_detail
                            )::STRING
                        ),
                        'undefined'
                    )
                )
            ) AS app_store_surface_detail,
            coalesce(


                nullif(surface_type, ''), parsed_url:parameters:surface_type, parsed_referrer:parameters:surface_type
            )::STRING as app_store_surface_type,
            COALESCE(
                parsed_url:parameters:surface_intra_position,
                parsed_referrer:parameters:surface_intra_position
            )::STRING as app_store_surface_intra_position,
            COALESCE(parsed_url:parameters:surface_inter_position, parsed_referrer:parameters:surface_inter_position)::STRING
                as app_store_surface_inter_position,
            COALESCE(parsed_url:parameters:locale, parsed_referrer:parameters:locale)::STRING
                as app_store_locale
        FROM filtered_raw_ga4_events
    ),

    {% set not_empty_string_fields = [
        "param_campaign",
        "param_source",
        "param_medium",
        "param_content",
        "param_term",
        "traffic_source_name",
        "traffic_source_source",
        "traffic_source_medium",
        "page_referrer_host",
        "page_referrer_full",
        "page_referrer_url",
        "page_referrer_query",
        "page_referrer_path",
        "shop_subdomain"
    ] %}

final AS (
    SELECT
    * exclude (parsed_url, parsed_referrer)
    replace (
        {% for field in not_empty_string_fields %}
            nullif({{ field }}, '') as {{ field }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
    from ga4_events
)

SELECT * FROM final
