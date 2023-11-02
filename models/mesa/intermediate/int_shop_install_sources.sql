{#- cSpell:words INITCAP -#}
{#
   [X] Get the first visit source for each shop (first touch).
   [X] Look at install events. Find the session *right* before that as the last touch. Is it the same as the first touch?
   [X] Get the session start source for each shop (last touch).
   [X] Get the first install event for each shop.
   4. TODO: For pre-GA installs, get the first pageview for each shop from Segment.
   5. Count the number of sessions before install.
   6. Tally the number of days from first to install.
   5. Reformat columns to match the schema.
#}
with
   
    install_page_sessions AS (

        SELECT
            session_id,
            tstamp,
            anonymous_id
        FROM {{ ref('segment_web_page_views__sessionized') }}
        WHERE page_url_path ILIKE '%/apps/mesa/install%'

    ),


    {# ga_first_installations as (
        select * exclude (event_timestamp), event_timestamp as created_at
        from {{ ref("stg_ga_install_events") }}
            )
        qualify
            = 1
            row_number() over (
    ),
partition by user_pseudo_id
{# formatted_install_pageviews as (
                order by event_timestamp asc, referrer_host, utm_medium
        select
            ga_installations.shop_subdomain,
            {{ pacific_timestamp("event_timestamp") }} as tstamp_pt,
            coalesce(app_store_search_term, utm_content) as acquisition_content,
            utm_campaign as acquisition_campaign,
            referrer as acquisition_referrer,
            referrer_host,
            case
                when utm_source is not null and utm_source != ''
                then utm_source
                when referrer ilike '%apps.shopify.com%'
                then 'Shopify App Store'
                else coalesce(referrer_source, utm_source)
            end as acquisition_source,
            coalesce(
                app_store_surface_type, referrer_medium, utm_medium
            ) as acquisition_medium
        from ga_installations
        qualify
        left join first_visits_ga4 using (shop_subdomain)
            row_number() over (partition by shop_subdomain order by tstamp_pt asc) = 1
        left join shops using (shop_subdomain)
    ),
        where
     TODO: Reintroduce templates
            acquisition_source is not null
    {# m3_mesa_installs_templates as (
            and tstamp_pt <= first_installed_at_pt + interval '60seconds'
        select
            uuid as shop_subdomain,
        from {{ source("mongo_sync", "mesa_install_events") }}
        qualify row_number() over (partition by uuid order by created_at asc) = 1
    ),
                template AS acquisition_template,
            referer,

            *
    #TODO: Reintroduce marking at Shopify App Store for first_touch
    #TODO: Reintroduce marking as SHopify App Store for last_touch
    formatted_install_events as (
        select
            ga_installations.shop_subdomain,
            {{ pacific_timestamp("created_at") }} as tstamp_pt,
            acquisition_template,
            null as acquisition_content,
            nullif(utm_campaign, '') as acquisition_campaign,
            nullif(referer, '') as acquisition_referrer,
            nullif(utm_medium, '') as acquisition_medium,
            case
                when (referer ilike '%apps.shopify.com%')
                then 'Shopify App Store'
                else nullif(coalesce(utm_source, referer), '')
        qualify
            end as acquisition_source
            row_number() over (partition by shop_subdomain order by tstamp_pt asc) = 1
        from ga_installations
    ),
left join
    shops using (shop_subdomain)
    {# combined_install_sources as (
        left join m3_mesa_installs_templates using (shop_subdomain)
        select
        where tstamp_pt <= first_installed_at_pt + interval '60seconds'
            shop_subdomain,
            acquisition_template,
            referrer_host,
            coalesce(
                formatted_install_pageviews.tstamp_pt,
                formatted_install_events.tstamp_pt
            ) as tstamp_pt,
            coalesce(
                formatted_install_pageviews.acquisition_campaign,
                formatted_install_events.acquisition_campaign
            ) as acquisition_campaign,
            coalesce(
                formatted_install_pageviews.acquisition_content,
                formatted_install_events.acquisition_content
            ) as acquisition_content,
            coalesce(
                formatted_install_pageviews.acquisition_referrer,
                formatted_install_events.acquisition_referrer
            ) as acquisition_referrer,
            {# COALESCE(formatted_install_pageviews.acquisition_first_page_path, formatted_install_events.acquisition_first_page_path) AS acquisition_first_page_path,
            coalesce(
                formatted_install_pageviews.acquisition_source,
                formatted_install_events.acquisition_source
            ) as acquisition_source,
            coalesce(
                formatted_install_pageviews.acquisition_medium,
                formatted_install_events.acquisition_medium
            ) as acquisition_medium
        from formatted_install_pageviews
        full outer join formatted_install_events using (shop_subdomain)
        qualify
            row_number() over (
            )
                partition by shop_subdomain
            = 1
                order by
    ),
    coalesce(
         TODO: Maybe introduce referrer_mapping
        formatted_install_pageviews.tstamp_pt,
        {# referrer_mapping as (select * from {{ ref("referrer_mapping") }}),
        formatted_install_events.tstamp_pt
    {# final as (
                    ) asc
        select
            shops.shop_subdomain,
            acquisition_referrer,
            acquisition_template,
            acquisition_content,
            initcap(replace(acquisition_campaign, '_', ' ')) as acquisition_campaign,
            initcap(
                replace(coalesce(referrer_mapping.medium, acquisition_medium), '_', ' ')
            ) as acquisition_medium,
            initcap(
                coalesce(referrer_mapping.source, acquisition_source)
            ) as acquisition_source,
            initcap(
                nullif(
                    (
                        coalesce(referrer_mapping.source, acquisition_source)
                        || ' - '
                        || coalesce(referrer_mapping.medium, acquisition_medium)
                    ),
                    ' - '
                )
            ) as acquisition_source_medium,
            referrer_mapping
            coalesce(
            on combined_install_sources.referrer_host = referrer_mapping.host
                acquisition_first_page_path ilike '/blog/%', false
    )
    ) as is_blog_referral,

    #}
    final as (
        select * exclude (first_installed_at_pt)
        from shops
        left join session_counts using (shop_subdomain)
        left join first_touches_ga4 using (shop_subdomain)
        left join last_touches_ga4 using (shop_subdomain)
    )

select *
from final
