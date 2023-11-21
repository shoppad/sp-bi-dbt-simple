with
    raw_shops as (
        select * rename uuid as shop_subdomain
        from {{ source("mongo_sync", "shops") }}
        where
            not __hevo__marked_deleted
            and shopify:plan_name not in ('staff', 'staff_business', 'shopify_alumni')
            and status not in ('banned')

    ),

    trimmed_shops as (
        {% set exclude = ["_id", "_created_at", "timestamp", "method"] + var(
            "etl_fields"
        ) -%}

        select * exclude ({{ exclude | join(", ") }}), _created_at as created_at
        from raw_shops
    ),

    staff_subdomains as (select shop_subdomain from {{ ref("staff_subdomains") }}),

    custom_apps as (
        select
            shop_subdomain,
            true as is_custom_app,
            'custom app' as status,
            parse_json(
                '{"plan_name": "none (custom app)", "currency": "usd"}'
            ) as shopify,
            first_dt,
            last_dt
        from {{ ref("custom_app_daily_revenues") }}
    ),

    install_dates as (
        select
            shop_subdomain,
            min(coalesce(created_at, first_dt)) as first_installed_at_utc,
            max(coalesce(created_at, first_dt)) as latest_installed_at_utc,
            {{ pacific_timestamp("min(coalesce(created_at, first_dt))") }}
            as first_installed_at_pt,
            {{ pacific_timestamp("min(coalesce(created_at, first_dt))") }}::date
            as first_installed_on_pt,
            {{ pacific_timestamp("max(coalesce(created_at, first_dt))") }}
            as latest_installed_at_pt,
            date_trunc('week', first_installed_at_pt)::date as cohort_week,
            date_trunc('month', first_installed_at_pt)::date as cohort_month
        from trimmed_shops
        full outer join custom_apps using (shop_subdomain)
        group by 1
    ),

    shop_metas as (
        select shop_subdomain, array_union_agg(meta) as aggregated_meta
        from trimmed_shops
        group by 1
    ),

    shops as (
        select * exclude ("META")
        from trimmed_shops
        where
            not shop_subdomain in (select * from staff_subdomains)
            and shopify:plan_name
            not in ('affiliate', 'partner_test', 'plus_partner_sandbox')
        qualify
            row_number() over (partition by shop_subdomain order by created_at desc) = 1
    ),

    uninstall_data_points as (
        select
            shop_subdomain,
            iff(shops.status = 'active', null, uninstalled_at_pt) as uninstalled_at_pt
        from shops
        left join
            (
                select
                    id as shop_subdomain, apps_mesa_uninstalledat as uninstalled_at_pt  -- note: this timestamp is already in pst
                from {{ source("php_segment", "users") }}

                union all
                select shop_subdomain, uninstalled_at_pt  -- note: this timestamp is already in pst
                from {{ ref("stg_mesa_uninstalls") }}

                union all
                select shop_subdomain, last_dt as uninstalled_at_pt
                from custom_apps
            ) using (shop_subdomain)
    ),

    uninstall_dates as (
        select shop_subdomain, max(uninstalled_at_pt) as uninstalled_at_pt
        from uninstall_data_points
        group by 1
    ),

    final as (
        select
            * exclude (
                created_at,
                "GROUP",
                aggregated_meta,
                is_custom_app,
                first_dt,
                last_dt,
                shopify,
                status
            ),
            coalesce(shops.status, custom_apps.status) as status,
            shop_metas.aggregated_meta as meta,
            coalesce(shops.shopify, custom_apps.shopify) as shopify,
            shopify:id::variant as shopify_id,
            coalesce(custom_apps.shopify:plan_name, shops.shopify:plan_name)::string
            as shopify_plan_name,
            coalesce(
                shopify_plan_name
                in ({{ "'" ~ var("zombie_store_shopify_plans") | join("', '") ~ "'" }}),
                false
            ) as is_shopify_zombie_plan,
            {{ pacific_timestamp("to_timestamp(shopify:updated_at)") }}
            as shopify_last_updated_at_pt,
            to_timestamp_ntz(billing:plan:trial_ends::varchar)::date
            as trial_end_dt_utc,
            iff(
                uninstalled_at_pt is null,
                null,
                {{ datediff("latest_installed_at_pt", "uninstalled_at_pt", "minute") }}
            ) as minutes_until_uninstall,
            coalesce(is_custom_app, false) as is_custom_app
        from shops
        full outer join custom_apps using (shop_subdomain)
        left join shop_metas using (shop_subdomain)
        left join install_dates using (shop_subdomain)
        left join uninstall_dates using (shop_subdomain)
    )

select *
from final
