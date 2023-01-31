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
    categories[0]::STRING AS shop_category,
    features AS shop_features,
    COALESCE(LOWER(ARRAY_TO_STRING(features, '')) ILIKE '%recharge%', FALSE) AS has_recharge,
    platform_rank AS shop_platform_rank,
    platform_rank_percentile AS shop_platform_rank_percentile,
    estimated_sales AS shop_estimated_sales,
    monthly_app_spend AS shop_monthly_app_spend
FROM flattened_table
