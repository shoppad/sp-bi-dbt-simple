with
    shops as (select shop_subdomain, billing from {{ ref("stg_shops") }}),

    final as (
        select
            shop_subdomain,
            coalesce(billing:method:name, 'shopify')::string as billing_method_name,
            coalesce(
                billing:overage:last_count::float, 0
            ) as billing_overage_last_count,
            billing:plan:days_complete::float as days_complete,
            billing:plan:id::string as plan_id,
            billing:plan:percent_complete::float as percent_complete,
            billing:plan:percent_used::float as percent_used,

            billing:plan:status::string as status,
            billing:plan:used::float as plan_used,
            billing:plan_name::string as plan_name,
            billing:plan:trial_days::float as trial_days,
            {{ pacific_timestamp("to_timestamp(billing:plan:trial_ends::INT, 3)") }}
            as trial_ends_pt,
            {{ pacific_timestamp("billing:plan:updated_at::STRING") }}
            as billing_updated_at_pt,
            billing:plan:billing_on::string as billing_on_pt,
            billing:plan:overlimit_date::string as overlimit_date_pt,
            billing:plan:balance_remaining::string as balance_remaining,
            billing:method:chargebee_id::string as chargebee_id,
            case
                when
                    is_null_value(billing:plan:interval)
                    or billing:plan:interval is null
                then billing:plan_interval
                else billing:plan:interval
            end::string as plan_interval,
            billing:plan_price::string as plan_price,
            billing:plan:balance_used::string as balance_used,
            billing:plan_type::string as plan_type,

            iff(
                plan_price = '',
                '0',
                cast(
                    iff(
                        plan_interval = 'annual',
                        plan_price / 365::float,
                        plan_price / 30::float
                    ) as number(38, 20)
                )
            ) as daily_plan_revenue
        from shops
    )

select *
from final
