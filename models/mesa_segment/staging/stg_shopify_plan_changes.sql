with
    raw_shopify_plan_changes as (
        select
            * exclude (timestamp, handle, {{ var("ugly_segment_fields") | join(", ") }})
            rename(id as shopify_plan_change_id, user_id as shop_subdomain),
            {{ pacific_timestamp("timestamp") }} as changed_at_pt,
            cast(changed_at_pt as date) as changed_on_pt
        from {{ source("php_segment", "shopify_plan_changes") }}
    ),

    shops as (select shop_subdomain from {{ ref("stg_shops") }}),

    -- Add a new CTE to select the fields you want to add as a new row
    manual_row as (
        select
            shopify_plan_name as plan,
            shop_subdomain,
            null as oldplan,
            null as shopify_plan_change_id,
            null as plandisplayname,
            shopify_last_updated_at_pt as changed_at_pt,
            cast(shopify_last_updated_at_pt as date) as changed_on_pt
        from {{ ref("stg_shops") }}
    ),

    final as (
        -- Combine the results of the original query with the new rowf
        select *
        from shops
        inner join raw_shopify_plan_changes using (shop_subdomain)

        union all

        select *
        from shops
        inner join manual_row using (shop_subdomain)
    )

select *
from final
