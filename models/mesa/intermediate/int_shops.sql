{% set source_table = ref("stg_shops") %}
with

decorated_shops as (
        {% set columns_to_skip = [
            "scopes",
            "billing",
            "status",
            "entitlements",
            "timestamp",
            "shopify",
            "usage",
            "config",
            "themes",
            "webhooks",
            "messages",
            "analytics",
            "schema",
            "handle",
            "method",
            "account",
            "wizard",
            "mongoid",
            "authtoken",
            "metabase",
        ] %}
        select
            {{
                groomed_column_list(source_table, except=columns_to_skip) | join(
                    ",\n       "
                )
            }},
            shopify:currency::string as currency,
            {{ pacific_timestamp("cast(shopify:created_at AS TIMESTAMP_LTZ)") }}
            as shopify_shop_created_at_pt,
            shopify:country::string as shopify_shop_country,
            status as install_status,
            analytics:initial:orders_count::numeric
            as shopify_shop_orders_initial_count,
            analytics:initial:orders_gmv::numeric as shopify_shop_gmv_initial_total,
            analytics:orders:count::numeric as shopify_shop_orders_current_count,
            analytics:orders:gmv::numeric as shopify_shop_gmv_current_total,
            analytics:initial:shopify_plan_name::string as initial_shopify_plan_name,
            coalesce(
                wizard:builder:step = 'complete', FALSE
            ) as is_builder_wizard_completed,
            {{ datediff("shopify_shop_created_at_pt", "first_installed_at_pt", "day") }}
            as age_of_store_at_install_in_days,
            {{
                datediff(
                    "shopify_shop_created_at_pt", "first_installed_at_pt", "week"
                )
            }} as age_of_store_at_install_in_weeks,
            case
                when age_of_store_at_install_in_days = 0
                then '1-First Day'
                when age_of_store_at_install_in_days <= 7
                then '2-First Week (Day 2-7)'
                when age_of_store_at_install_in_days <= 31
                then '3-First Month (After First Week)'
                when age_of_store_at_install_in_days <= 90
                then '4-First Quarter (After First Month)'
                when age_of_store_at_install_in_days <= 180
                then '5-First Half (After First Quarter)'
                when age_of_store_at_install_in_days <= 365
                then '6-First Year (After First Half)'
                when age_of_store_at_install_in_days <= 547
                then '7-First 18 Months (After First Year)'
                when age_of_store_at_install_in_days <= 730
                then '8-First 2 Years (After 18 Months)'
                else '9-2nd Year+'
            end as age_of_store_at_install_bucket
        from {{ source_table }}
    ),

    activation_dates as (
        select uuid as shop_subdomain, apps_mesa_meta_activatedat_value as activation_date_pt
        from {{ source('php_segment', 'users') }}
    ),

    launch_session_dates as (
        select
            shop_subdomain,
            iff(
                meta_attribs.value:name = 'launchsessiondate',
                meta_attribs.value:value::date,
                NULL
            ) as launch_session_date,
            not launch_session_date
                is NULL as has_had_launch_session
        from {{ ref("stg_shops") }}, lateral flatten(input => meta) as meta_attribs
        QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY launch_session_date DESC) = 1
    ),

    conversion_rates as (
        select currency, in_usd from {{ ref("currency_conversion_rates") }}
    ),

    final as (
        select
            * exclude (
                shopify_shop_gmv_current_total, shopify_shop_gmv_initial_total, in_usd
            ),
            1.0
            * shopify_shop_gmv_initial_total
            * in_usd as shopify_shop_gmv_initial_total_usd,
            1.0
            * shopify_shop_gmv_current_total
            * in_usd as shopify_shop_gmv_current_total_usd,
            coalesce(in_usd is NULL, FALSE) as currency_not_supported
        from decorated_shops
        left join activation_dates using (shop_subdomain)
        left join launch_session_dates using (shop_subdomain)
        left join conversion_rates using (currency)
    )

select *
from final
