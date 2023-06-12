{# {{
    config(
        materialized='incremental'
    )
}} #}

{%- set source_table = source('mongo_sync', 'shop_infos') -%}
WITH flattened_table AS (
    SELECT
        uuid AS shop_subdomain,
        updated_at,
        {{
            generate_flatten_json_columns(
                model_name = source_table,
                json_column = 'data'
            )
        }}

    FROM {{ source_table }}
    WHERE uuid IS NOT NULL
        AND DATA != '[]'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY shop_subdomain ORDER BY created_at DESC) = 1
),

final AS (
    SELECT
        shop_subdomain,
        updated_at,
        categories[0]::STRING AS store_leads_category,
        SPLIT_PART(store_leads_category, '/', 2) AS store_leads_top_level_category,
        features AS store_leads_features,
        COALESCE(LOWER(ARRAY_TO_STRING(store_leads_features, '')) ILIKE '%recharge%', FALSE) AS store_leads_has_recharge,
        platform_rank::NUMERIC AS store_leads_platform_rank,
        platform_rank_percentile::NUMERIC AS store_leads_platform_rank_percentile,
        1.0 * estimated_sales::NUMERIC / 100 AS store_leads_estimated_monthly_sales,
        monthly_app_spend::NUMERIC AS store_leads_monthly_app_spend
    FROM flattened_table
)

SELECT * FROM final
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE  updated_at > '{{ get_max_updated_at() }}'
    {% endif %}
