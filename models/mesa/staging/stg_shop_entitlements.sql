SELECT
    shop_subdomain,
    split_entitlements.value:name::string AS "name",
    split_entitlements.value:value AS "value",
    split_entitlements.value:status::string AS "status"
FROM {{ ref('stg_shops') }},
    LATERAL FLATTEN(input => entitlements) AS split_entitlements
