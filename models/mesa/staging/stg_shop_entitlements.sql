SELECT
    shop_subdomain,
    split_entitlements.value:name::STRING AS attribute_name,
    split_entitlements.value:value AS attribute_value,
    split_entitlements.value:status::STRING AS attribute_status
FROM {{ ref('stg_shops') }},
    LATERAL FLATTEN(input => entitlements) AS split_entitlements
