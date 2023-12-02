{#- cspell:words initcap -#}
{#
[x] get the first visit source for each shop (first touch).
[x] look at install events. find the session *right* before that AS the last touch. is it the same AS the first touch?
[x] get the session start source for each shop (last touch).
[x] get the first install event for each shop.
[x] todo: for pre-ga installs, get the first pageview for each shop FROM segment.
[x] count the number of sessions before install.
[x] tally the number of days FROM first to install.
[x] reformat columns to match the existing schema.
[x] re-add acquisition template.
[ ] Look for any and all pre-install search_ad surface_type events.
#}
with
    shops as (select shop_subdomain, first_installed_at_pt from {{ ref("stg_shops") }}),

    ga_attribution as (select * from {{ ref("int_ga_attribution") }}),

    segment_attribution as (select * from {{ ref("int_segment_attribution") }}),

    app_store_install_events as (
        select
            shop_subdomain,
            {% set column_names = dbt_utils.get_filtered_columns_in_relation(
                from=ref("stg_ga_app_store_page_events"),
                except=[
                    "user_pseudo_id",
                    "shopify_id",
                    "shop_subdomain",
                    "event_name",
                    "referrer_term",
                    "app_store_surface_intra_position",
                    "app_store_surface_inter_position",
                    "utm_medium",
                    "utm_campaign",
                ],
            ) %}
            {% for column_name in column_names %}
                {{ column_name }} as app_store_install_{{ column_name }}
                {%- if not loop.last %},{% endif %}
            {% endfor %},
            initcap(
                utm_source || ' - ' || utm_medium
            ) as app_store_install_source_medium
        from {{ ref("stg_ga_app_store_page_events") }}
        where event_name = 'shopify_app_install'
        qualify
            row_number() over (
                partition by shop_subdomain order by event_timestamp_pt asc
            )
            = 1
    ),

    app_store_ad_clicks as (
        select
            shop_subdomain,
            {% set column_names = dbt_utils.get_filtered_columns_in_relation(
                from=ref("stg_ga_app_store_page_events"),
                except=[
                    "user_pseudo_id",
                    "shopify_id",
                    "shop_subdomain",
                    "event_name",
                    "surface_type",
                    "surface_detail",
                    "utm_term",
                    "referrer",
                    "referrer_term",
                ],
            ) %}
            {% for column_name in column_names %}
                {{ column_name }} as app_store_ad_click_{{ column_name }}
                {%- if not loop.last %},{% endif %}
            {% endfor %},
            initcap(
                utm_source || ' - ' || utm_medium
            ) as app_store_ad_click_source_medium
        from {{ ref("stg_ga_app_store_page_events") }}
        where page_location ilike '%search_ad%' or event_name = 'shopify_ad_click'
        qualify
            row_number() over (
                partition by shop_subdomain order by event_timestamp_pt asc
            )
            = 1
    ),

    app_store_ad_click_counts as (
        select
            shop_subdomain,
            count_if(
                app_store_ad_click_event_timestamp_pt
                <= first_installed_at_pt + interval '60min'
            )
            > 0 as app_store_did_click_ad_before_install,
            count(app_store_ad_clicks.app_store_ad_click_page_location)
            > 0 as app_store_did_click_ad
        from shops
        left join app_store_ad_clicks using (shop_subdomain)
        group by 1
    ),

    app_store_organic_clicks as (
        select
            shop_subdomain,
            {% set column_names = dbt_utils.get_filtered_columns_in_relation(
                from=ref("stg_ga_app_store_page_events"),
                except=[
                    "user_pseudo_id",
                    "shopify_id",
                    "shop_subdomain",
                    "event_name",
                    "name",
                    "referrer_source",
                    "utm_term",
                    "referrer",
                    "referrer_medium",
                ],
            ) %}
            {% for column_name in column_names %}
                {{ column_name }} as app_store_organic_click_{{ column_name }}
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        from {{ ref("stg_ga_app_store_page_events") }}
        where
            page_location ilike '%surface_type=%'
            and page_location not ilike '%search_ad%'
            and event_name = 'session_start'
        qualify
            row_number() over (
                partition by shop_subdomain order by event_timestamp_pt asc
            )
            = 1
    ),

    mesa_install_events as (
        select
            uuid as shop_subdomain,
            {% set column_names = dbt_utils.get_filtered_columns_in_relation(
                from=source("mongo_sync", "mesa_install_events"),
                except=[
                    "created_at",
                    "template",
                    "state",
                    "uuid",
                    "mesa_id",
                    "__hevo__ingested_at",
                    "__hevo__loaded_at",
                    "__hevo__database_name",
                    "updated_at",
                    "utm_term",
                    "route",
                    "ip",
                    "__hevo__marked_deleted",
                    "_created_at",
                    "_id",
                ],
            ) %}
            {% for column_name in column_names %}
                iff(
                    {{ column_name }}::varchar = '', null, {{ column_name }}
                ) as mesa_install_event_{{ column_name }}
                {%- if not loop.last %},{% endif %}
            {% endfor %},
            {{ pacific_timestamp("created_at") }} as mesa_install_event_timestamp_pt,
            template as acquisition_template,
            'mesa_install_event' as install_event_info_source
        from {{ source("mongo_sync", "mesa_install_events") }}
        where install_completed
    ),

    formatted_install_events as (
        select mesa_install_events.*
        from mesa_install_events
        left join shops using (shop_subdomain)
        having
            mesa_install_event_timestamp_pt
            <= first_installed_at_pt + interval '60seconds'
        qualify
            row_number() over (
                partition by shop_subdomain order by mesa_install_event_timestamp_pt asc
            )
            = 1
    ),

    data_pipeline_attributions as (
        select
            * rename ga4_sessions_til_install as sessions_til_install,
            'ga4' as install_event_info_source
        from ga_attribution

        union

        select
            * rename segment_sessions_til_install as sessions_til_install,
            'segment' as install_event_info_source
        from segment_attribution
    ),

    combined_attribution as (
        select
            shops.shop_subdomain,
            data_pipeline_attributions.* exclude (
                shop_subdomain,
                first_touch_campaign,
                last_touch_campaign,
                first_touch_medium,
                last_touch_medium,
                first_touch_source,
                last_touch_source,
                install_event_info_source
            ),
            coalesce(
                first_touch_campaign, mesa_install_event_utm_campaign
            ) as first_touch_campaign,
            coalesce(
                last_touch_campaign, mesa_install_event_utm_campaign
            ) as last_touch_campaign,
            coalesce(
                first_touch_medium, mesa_install_event_utm_medium
            ) as first_touch_medium,
            coalesce(
                last_touch_medium, mesa_install_event_utm_medium
            ) as last_touch_medium,
            coalesce(
                first_touch_source, mesa_install_event_utm_source
            ) as first_touch_source,
            coalesce(
                last_touch_source, mesa_install_event_utm_source
            ) as last_touch_source,
            coalesce(
                data_pipeline_attributions.install_event_info_source,
                formatted_install_events.install_event_info_source
            ) as install_event_info_source,
            formatted_install_events.* exclude (install_event_info_source)
        from shops
        left join formatted_install_events using (shop_subdomain)
        left join data_pipeline_attributions using (shop_subdomain)
        having
            first_touch_at_pt is null
            or first_touch_at_pt <= first_installed_at_pt + interval '60min'
        qualify
            row_number() over (
                partition by shop_subdomain order by first_touch_at_pt asc
            )
            = 1
    ),

    referrer_mapping as (select * from {{ ref("referrer_mapping") }}),

    combined as (
        select
            shop_subdomain,
            combined_attribution.* exclude (
                shop_subdomain,
                first_touch_medium,
                last_touch_medium,
                first_touch_source,
                last_touch_source
            ) replace (
                initcap(
                    replace(first_touch_campaign, '_', ' ')
                ) as first_touch_campaign,
                initcap(replace(last_touch_campaign, '_', ' ')) as last_touch_campaign
            ),
            initcap(
                replace(
                    coalesce(first_touch_referrer_mapping.medium, first_touch_medium),
                    '_',
                    ' '
                )
            ) as first_touch_medium,
            initcap(
                replace(
                    coalesce(last_touch_referrer_mapping.medium, last_touch_medium),
                    '_',
                    ' '
                )
            ) as last_touch_medium,
            initcap(
                coalesce(first_touch_referrer_mapping.source, first_touch_source)
            ) as first_touch_source,
            initcap(
                coalesce(last_touch_referrer_mapping.source, last_touch_source)
            ) as last_touch_source,
            initcap(
                nullif(
                    (
                        coalesce(
                            first_touch_referrer_mapping.source, first_touch_source
                        )
                        || ' - '
                        || coalesce(
                            last_touch_referrer_mapping.medium, first_touch_medium
                        )
                    ),
                    ' - '
                )
            ) as first_touch_source_medium,
            initcap(
                nullif(
                    (
                        coalesce(first_touch_referrer_mapping.source, last_touch_source)
                        || ' - '
                        || coalesce(
                            last_touch_referrer_mapping.medium, last_touch_medium
                        )
                    ),
                    ' - '
                )
            ) as last_touch_source_medium,
            coalesce(
                acquisition_first_page_path ilike '%/blog/%', false
            ) as is_blog_referral,
            timediff(
                'days', first_touch_at_pt, first_installed_at_pt
            ) as days_to_install,
            coalesce(
                first_touch_app_store_surface_type = true
                or last_touch_app_store_surface_type = true,
                false
            ) as is_app_store_referral,
            coalesce(
                first_touch_app_store_surface_type = 'search_ad'
                or last_touch_app_store_surface_type = 'search_ad',
                false
            ) as is_app_store_search_ad_referral,
            app_store_ad_click_counts.* exclude (shop_subdomain)
        from shops
        left join combined_attribution using (shop_subdomain)
        left join
            referrer_mapping as first_touch_referrer_mapping
            on combined_attribution.first_touch_referrer_host
            = first_touch_referrer_mapping.host
        left join
            referrer_mapping as last_touch_referrer_mapping
            on combined_attribution.last_touch_referrer_host
            = last_touch_referrer_mapping.host
        left join app_store_ad_click_counts using (shop_subdomain)
    ),

    final as (

        select
            *,
            coalesce(
                app_store_ad_click_app_store_surface_type is not null, false
            ) as app_store_has_ad_click,
            coalesce(
                app_store_organic_click_app_store_surface_type is not null, false
            ) as has_app_store_organic_click,

            case
                when
                    app_store_has_ad_click = true and has_app_store_organic_click = true
                then 'app_store_ad_click_and_organic_click'
                when app_store_has_ad_click = true
                then 'app_store_ad_click'
                when has_app_store_organic_click = true
                then 'app_store_organic_click'
                else '(direct or predates tracking)'
            end as app_store_click_type
        from combined
        left join app_store_ad_clicks using (shop_subdomain)
        left join app_store_install_events using (shop_subdomain)
        left join app_store_organic_clicks using (shop_subdomain)
    )

select *
from final
