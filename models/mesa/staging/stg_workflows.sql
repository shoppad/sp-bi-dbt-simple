WITH
source_workflows as (

    select * from {{ source('mongo_sync', 'workflows') }}

),

shops AS (
    SELECT shop_subdomain
    FROM {{ ref('stg_shops') }}

),

workflows AS (

    SELECT
        * RENAME uuid AS shop_subdomain, _id AS workflow_id, __hevo__marked_deleted AS is_deleted, name AS title, enabled AS is_enabled,
        template AS template_name
    FROM source_workflows

),

final AS (

    SELECT
        workflow_id,
        {{ pacific_timestamp('created_at') }} AS created_at_pt,
        created_at_pt::DATE AS created_on_pt,
        created_by,
        COALESCE(CONTAINS(created_by, 'shoppad'), FALSE) AS is_created_by_shoppad,
        shop_subdomain,
        title,
        COALESCE(is_premium, FALSE) AS is_premium,
        description,
        key,
        tags,
        is_enabled,
        is_deleted,
        {# source AS first_step_app, #}
        {# destination AS last_step_app, TODO: Change to use Steps https://shoppad.slack.com/archives/D01UTNZKM6D/p1667343343556039 #}
        template_name,
        {{ pacific_timestamp('updated_at') }} AS updated_at_pt,
        setup
    FROM workflows
    INNER JOIN shops USING (shop_subdomain) -- Filter out any workflows that don't belong to a shop.
    WHERE
        NOT template = 'shopify/order/send_order_report_card_email' -- Filter out the order report card email workflow.

)

SELECT * FROM final
