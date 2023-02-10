{%- set source_table = source('mongo_sync', 'shop_infos') -%}
WITH flattened_table AS (
    SELECT
        uuid AS shop_subdomain,
        {{
            generate_flatten_json_columns(
                model_name = source_table,
                json_column = 'data'
            )
        }}

    FROM {{ source_table }}
)

SELECT
    shop_subdomain,
    categories[0]::STRING AS store_leads_category,
    SPLIT_PART(store_leads_category, '/', 2) AS store_leads_top_level_category,
    features AS store_leads_features,
    COALESCE(LOWER(ARRAY_TO_STRING(store_leads_features, '')) ILIKE '%recharge%', FALSE) AS store_leads_has_recharge,
    platform_rank::NUMERIC AS store_leads_platform_rank,
    platform_rank_percentile::NUMERIC AS store_leads_platform_rank_percentile,
    1.0 * estimated_sales::NUMERIC / 100 AS store_leads_estimated_monthly_sales,
    monthly_app_spend::NUMERIC AS store_leads_monthly_app_spend
FROM flattened_table
