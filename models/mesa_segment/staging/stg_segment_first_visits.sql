with
    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

    {% set not_empty_string_fields = [
        "first_page_url",
        "first_page_url_host",
        "first_page_url_path",
        "utm_content",
        "utm_campaign",
        "utm_medium",
        "utm_source",
        "referrer",
        "referrer_host",
        "referrer_medium",
        "referrer_source",
    ] %}
    segment_sessions as (
        select
            * exclude session_start_tstamp replace (
                {% for field in not_empty_string_fields %}
                    nullif({{ field }}, '') as {{ field }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            )
            rename blended_user_id as shop_subdomain,

            {{ pacific_timestamp("session_start_tstamp") }} as session_start_tstamp_pt
        from {{ ref("segment_web_sessions") }}
    ),

    first_segment_visits as (
        select * exclude first_installed_at_pt
        from shops
        left join segment_sessions using (shop_subdomain)
        where
            session_start_tstamp_pt <= shops.first_installed_at_pt + interval '1 hour'
            {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            AND session_start_tstamp_pt > '{{ get_max_updated_at() }}'
            {% endif %}

        qualify
            row_number() over (
                partition by shop_subdomain order by session_start_tstamp_pt asc
            )
            = 1
    ),

    formatted_first_visits as (
        select
            shop_subdomain,
            session_start_tstamp_pt as first_touch_at_pt,
            first_page_url as acquisition_first_page_path,
            parse_url(first_page_url) as page_params,
            split_part(first_page_url, '//', 2) as first_touch_url,
            first_page_url_host as first_touch_host,
            first_page_url_path as first_touch_path,
            first_page_url_query as first_touch_query,
            NULLIF(CASE
                WHEN first_page_url ILIKE '%getmesa.com/blog%' OR first_touch_host = 'blog.getmesa.com' THEN 'Blog'
                WHEN first_page_url ILIKE '%apps.shopify.com/mesa%' THEN 'Shopify App Store'
                WHEN first_page_url ILIKE '%docs.getmesa%' THEN 'Support Site'
                WHEN first_page_url ILIKE '%getmesa.com/' THEN 'Homepage'
                WHEN first_page_url ILIKE '%app.getmesa%' THEN 'Inside App (Untrackable)'
                WHEN first_page_url ILIKE '%getmesa.com%' THEN initcap(SPLIT_PART(first_touch_path, '/', 2))
                WHEN first_page_url IS NULL THEN '(Untrackable)'
                ELSE first_page_url
                END, '') AS first_touch_page_type,
            utm_content as first_touch_traffic_source_content,
            utm_campaign as first_touch_traffic_source_name,
            coalesce(utm_medium, referrer_medium) as first_touch_traffic_source_medium,
            coalesce(utm_source, referrer_source) as first_touch_traffic_source_source,
            nullif(
                TRIM(lower(
                    {{ target.schema }}.url_decode(
                        to_varchar(page_params:parameters:surface_detail)
                    )
                )),
                'undefined'
            ) as first_touch_app_store_surface_detail,
            to_varchar(
                page_params:parameters:surface_type
            ) as first_touch_app_store_surface_type,
            to_varchar(
                page_params:parameters:surface_intra_position
            ) as first_touch_app_store_surface_intra_position,
            to_varchar(
                page_params:parameters:surface_inter_position
            ) as first_touch_app_store_surface_inter_position,
            to_varchar(page_params:parameters:locale) as first_touch_app_store_locale,
            referrer as first_touch_referrer,
            referrer_host as first_touch_referrer_host,
            device_category as first_touch_device_category
        from first_segment_visits
    )

select * exclude page_params
from formatted_first_visits
