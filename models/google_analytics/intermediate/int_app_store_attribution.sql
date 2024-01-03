WITH
shops AS (
    SELECT
        shop_subdomain,
        first_installed_at_pt
    FROM {{ ref('stg_shops') }}
),

app_store_install_events as (
    select
        shop_subdomain,
        {{ get_prefixed_columns(ref("int_ga_app_store_page_events"), 'app_store_install', exclude=['event_name']) }}
    from {{ ref("int_ga_app_store_page_events") }}
    where event_name = 'shopify_app_install'
    qualify
        row_number() over (
            partition by shop_subdomain order by event_timestamp_pt asc
        )
        = 1
),

app_store_add_app_button_events as (
    select
        shop_subdomain,
        {{ get_prefixed_columns(ref("int_ga_app_store_page_events"), 'app_store_add_app', exclude=['event_name']) }}
    from {{ ref("int_ga_app_store_page_events") }}
    where event_name = 'Add App button'
    qualify
        row_number() over (
            partition by shop_subdomain order by event_timestamp_pt asc
        )
        = 1
),

combined_app_store_install_events AS (
    {%- set column_names = dbt_utils.get_filtered_columns_in_relation(
            from=ref('int_ga4_events'),
            except=['event_timestamp_pt', 'shop_subdomain', 'user_pseudo_id', 'event_name']
        )
    %}
    SELECT
        shop_subdomain,
    {%- for column_name in column_names %}
        COALESCE(app_store_install_{{ column_name }}, app_store_add_app_{{ column_name }}) as app_store_install_{{ column_name }}
        {%- if not loop.last %},{% endif %}
    {%- endfor %},
        COALESCE(app_store_install_at_pt, app_store_add_app_at_pt) as app_store_install_at_pt
    FROM app_store_add_app_button_events
    INNER JOIN app_store_install_events USING (shop_subdomain)
),

app_store_ad_clicks as (
    select
        shop_subdomain,
        {{ get_prefixed_columns(ref("int_ga_app_store_page_events"), 'app_store_ad_click', exclude=['event_name']) }}
    from {{ ref("int_ga_app_store_page_events") }}
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
            app_store_ad_click_at_pt
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
        {{ get_prefixed_columns(ref("int_ga_app_store_page_events"), 'app_store_organic_click', exclude=['event_name']) }}
    from {{ ref("int_ga_app_store_page_events") }}
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

final AS (
    SELECT * EXCLUDE (first_installed_at_pt),
    coalesce(
                app_store_ad_click_app_store_surface_type is not NULL, FALSE
            ) as app_store_has_ad_click,
            coalesce(
                app_store_organic_click_app_store_surface_type is not NULL, FALSE
            ) as app_store_has_organic_click,

            case
                when
                    app_store_has_ad_click = TRUE and app_store_has_organic_click = TRUE
                then 'app_store_ad_click_and_organic_click'
                when app_store_has_ad_click = TRUE
                then 'app_store_ad_click'
                when app_store_has_organic_click = TRUE
                then 'app_store_organic_click'
                else '(direct or predates tracking)'
            end as app_store_click_type
    FROM shops
    LEFT JOIN combined_app_store_install_events USING (shop_subdomain)
    LEFT JOIN app_store_ad_clicks USING (shop_subdomain)
    LEFT JOIN app_store_organic_clicks USING (shop_subdomain)
    LEFT JOIN app_store_ad_click_counts USING (shop_subdomain)
)

SELECT *
FROM final
