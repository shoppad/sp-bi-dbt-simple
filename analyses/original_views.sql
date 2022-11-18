-- Create the Snowflake views. If it came from Hevo it should start with M3_ Anything that only lives in snowflake starts with an SP_
-- Important: Run as the `PIPELINE` user in Snowflake

-- Utilities

-- Constellation
CREATE OR REPLACE VIEW "M3_MESA_VW" AS SELECT *,'mesa' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_MESA WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_BLOGSTUDIO_VW" AS SELECT *,'blogstudio' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_BLOGSTUDIO WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_BOUNCER_VW" AS SELECT *,'bouncer' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_BOUNCER WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_COIN_VW" AS SELECT *,'coin' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_COIN WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_SMILE_VW" AS SELECT *,'smile' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_SMILE WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_KITKARTS_VW" AS SELECT *,'kitkarts' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_KITKARTS WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_CUSTOMIZERY_SHOPS_VW" AS SELECT *,'customizery' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_CUSTOMIZERY_SHOPS WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_PAGESTUDIO_VW" AS SELECT *,'pagestudio' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_PAGESTUDIO WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_UPLOADERY_SHOPS_VW" AS SELECT *,'uploadery' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_UPLOADERY_SHOPS WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_TRACKTOR_VW" AS SELECT *,'tracktor' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, shopify:"plan_name"::STRING as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_TRACKTOR WHERE UUID NOT IN (select UUID from SP_STAFF);
CREATE OR REPLACE VIEW "M3_TRACKTOR_WEEKLY_COUNTS_VW" AS SELECT * from "M3_TRACKTOR_WEEKLY_COUNTS" t2 where __HEVO__MARKED_DELETED = false;
CREATE OR REPLACE VIEW "M3_STORES_VW" AS SELECT *,'fablet' as APP_HANDLE, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, PLAN_NAME as SHOPIFY_PLAN_NAME FROM MONGO.PUBLIC.M3_STORES;

-- Segment events
CREATE OR REPLACE VIEW "MONGO"."GETMESA"."AUTOMATION_VW" AS SELECT * FROM "MONGO"."GETMESA"."AUTOMATION" WHERE USER_ID NOT IN (select UUID from "MONGO"."PUBLIC"."SP_STAFF");
CREATE OR REPLACE VIEW "MONGO"."GETMESA"."MESA_DASHBOARD_VW" AS SELECT * FROM "MONGO"."GETMESA"."MESA_DASHBOARD" WHERE USER_ID NOT IN (select UUID from "MONGO"."PUBLIC"."SP_STAFF");
CREATE OR REPLACE VIEW "MONGO"."GETMESA"."MESA_TEST_VW" AS SELECT * FROM "MONGO"."GETMESA"."MESA_TEST" WHERE USER_ID NOT IN (select UUID from "MONGO"."PUBLIC"."SP_STAFF");

CREATE OR REPLACE VIEW "MONGO"."GETMESA"."MESA_WORKFLOW_VW" AS SELECT
    "AUTOMATION".*,
    CASE
        WHEN "SP_MESA_MERCHANTS_FACTS_VW"."ACTIVATED_AT" >= "AUTOMATION"."TIMESTAMP" then 'activated'
        ELSE 'onboarding'
    END AS FUNNEL_PHASE
FROM "MONGO"."GETMESA"."AUTOMATION" "AUTOMATION"
LEFT JOIN "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_FACTS_VW" "SP_MESA_MERCHANTS_FACTS_VW" ON "AUTOMATION"."USER_ID" = "SP_MESA_MERCHANTS_FACTS_VW"."UUID"
WHERE "AUTOMATION"."USER_ID" NOT IN (select UUID from "MONGO"."PUBLIC"."SP_STAFF");

CREATE OR REPLACE VIEW "MONGO"."GETMESA"."MESA_FLOW_VW" AS SELECT
    "MESA_FLOW".*,
    CASE
        WHEN "SP_MESA_MERCHANTS_FACTS_BY_UUID_VW"."ACTIVATED_AT" >= "MESA_FLOW"."TIMESTAMP" then 'activated'
        ELSE 'onboarding'
    END AS FUNNEL_PHASE
FROM "MONGO"."GETMESA"."MESA_FLOW" "MESA_FLOW"
LEFT JOIN "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_FACTS_BY_UUID_VW" "SP_MESA_MERCHANTS_FACTS_BY_UUID_VW" ON "MESA_FLOW"."USER_ID" = "SP_MESA_MERCHANTS_FACTS_BY_UUID_VW"."UUID"
WHERE "MESA_FLOW"."USER_ID" NOT IN (select UUID from "MONGO"."PUBLIC"."SP_STAFF");

-- You may need to run this as ACCOUNTADMIN
GRANT SELECT on all views in schema "MONGO"."GETMESA" to role PIPELINE;


CREATE OR REPLACE VIEW "SP_CONSTELLATION_FACTS_VW" AS 
    SELECT 
        t1.UUID,
        CREATEDAT as FIRST_SEEN_AT,
        SHOPIFY_PLANNAME as SHOPIFY_PLAN_NAME,
        IFNULL(SP_CONSTELLATION_LTV.all_time, 0) as SHOPIFY_BILLING_LTV,
        IFNULL(SP_CONSTELLATION_LTV.last_30, 0) as SHOPIFY_BILLING_LTV_LAST_30,
        IFNULL(SP_CONSTELLATION_LTV.last_90, 0) as SHOPIFY_BILLING_LTV_LAST_90,
        IFNULL(apps_tracktor_isactive, false) as TRACKTOR_IS_LIVE,
        IFNULL(apps_coin_isactive, false) as COIN_IS_LIVE, 
        IFNULL(apps_customizery_isactive, false) as CUSTOMIZERY_IS_LIVE, 
        IFNULL(apps_kitkarts_isactive, false) as KITKARTS_IS_LIVE, 
        IFNULL(apps_pagestudio_isactive, false) as PAGESTUDIO_IS_LIVE, 
        IFNULL(apps_blogstudio_isactive, false) as BLOGSTUDIO_IS_LIVE, 
        IFNULL(apps_mesa_isactive, false) as MESA_IS_LIVE, 
        IFNULL(apps_bouncer_isactive, false) as BOUNCER_IS_LIVE, 
        IFNULL(apps_smile_isactive, false) as SMILE_IS_LIVE, 
        IFNULL(apps_uploadery_isactive, false) as UPLOADERY_IS_LIVE, 
        IFNULL(apps_fablet_isactive, false) as FABLET_IS_LIVE,
        iff(TRACKTOR_IS_LIVE, 1, 0) + iff(COIN_IS_LIVE,1,0) + iff(CUSTOMIZERY_IS_LIVE,1,0) + iff(KITKARTS_IS_LIVE,1,0) + iff(PAGESTUDIO_IS_LIVE,1,0) + iff(BLOGSTUDIO_IS_LIVE,1,0) + iff(MESA_IS_LIVE,1,0) + iff(BOUNCER_IS_LIVE,1,0) + iff(SMILE_IS_LIVE,1,0) + iff(UPLOADERY_IS_LIVE,1,0) + iff(FABLET_IS_LIVE,1,0) as NUM_APPS_LIVE,
        SUPPORT_LASTREPLYAT,
        IFNULL(analytics_orders, 0) as TRACKTOR_NUM_ORDERS_IN_30_DAYS
    FROM MONGO.PHP.USERS t1
    LEFT JOIN SP_CONSTELLATION_LTV
    ON SP_CONSTELLATION_LTV.uuid = t1.UUID
    WHERE t1.UUID NOT IN (select UUID from SP_STAFF);

-- Load constellation partner export
-- Use the snowsql command on the terminal and put in password when prompted
-- Run commands one at a time
create or replace stage partner_stage;
create or replace file format partner_format type = 'csv' field_delimiter = ',' FIELD_OPTIONALLY_ENCLOSED_BY ='"' skip_header = 1;
put file:///Users/aaronwadler/Desktop/partner_export.csv @partner_stage;
create or replace table SP_PARTNER_EXPORT (
    SHOP VARCHAR(16777216),
    CHARGE_CREATION_TIME TIMESTAMP_NTZ,
    PARTNER_SALE FLOAT
);
copy into SP_PARTNER_EXPORT(SHOP, CHARGE_CREATION_TIME, PARTNER_SALE) from (select t.$3, replace(t.$5, ' UTC', ''), t.$8 from @partner_stage t) file_format = (format_name = 'partner_format');
create or replace table "SP_CONSTELLATION_LTV" as 
    with last_30 as (
        select shop,
        sum(partner_sale) as sales
        from SP_PARTNER_EXPORT 
        where CHARGE_CREATION_TIME > dateadd(day, -30, current_date())
        group by shop
    ),
    last_90 as (
        select shop,
        sum(partner_sale) as sales
        from SP_PARTNER_EXPORT 
        where CHARGE_CREATION_TIME > dateadd(day, -90, current_date())
        group by shop
    ),
    all_time as (
        select shop,
        sum(partner_sale) as sales
        from SP_PARTNER_EXPORT 
        group by shop
    ),
    shops as (
        select shop
        from SP_PARTNER_EXPORT 
        group by shop
    )
    select replace(shops.shop, '.myshopify.com', '') as uuid,
        last_30.sales as last_30,
        last_90.sales as last_90,
        all_time.sales as all_time
    from shops
    join last_30 on shops.shop = last_30.shop
    join last_90 on shops.shop = last_90.shop
    join all_time on shops.shop = all_time.shop;
DROP TABLE SP_PARTNER_EXPORT;

-- Mesa-specific
CREATE OR REPLACE VIEW "SP_MESA_LOOKUP-TABLE_VW" AS SELECT t1.UUID as UUID, max(t2._ID) as MESA_ID from "MONGO"."PUBLIC"."M3_MESA_AUTOMATIONS" t1 right join "MONGO"."PUBLIC"."M3_MESA" t2 on t1.uuid = t2.uuid group by t1.uuid;
CREATE OR REPLACE VIEW "M3_MESA_TRIGGERS_VW" AS
    SELECT *, 
    (select max(_ID) from "M3_MESA" t2 where t1.uuid = t2.uuid) as MESA_ID FROM MONGO.PUBLIC.M3_MESA_TRIGGERS t1
-- CREATE OR REPLACE VIEW "M3_MESA_AUTOMATIONS_VW" AS SELECT *, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT, (select max(_ID) from "M3_MESA" t2 where t1.uuid = t2.uuid) as MESA_ID FROM MONGO.PUBLIC.M3_MESA_AUTOMATIONS t1 WHERE UUID NOT IN (select UUID from SP_STAFF) and __HEVO__MARKED_DELETED = false;
CREATE OR REPLACE VIEW "M3_MESA_ENTITLEMENTS_VW" AS SELECT ROW_NUMBER() over (order by _ID DESC) as _ID, t1._id as PARENT_ID, lv.value:name as NAME, lv.value:status as STATUS, lv.value:value as VALUE FROM M3_MESA t1, LATERAL FLATTEN(input => t1.entitlements) lv
CREATE OR REPLACE VIEW "M3_MESA_META_VW" AS SELECT ROW_NUMBER() over (order by _ID DESC) as _ID, t1._id as PARENT_ID, lv.value:name::string as NAME, lv.value:status::string as STATUS, lv.value:value::string as VALUE FROM M3_MESA t1, LATERAL FLATTEN(input => t1.meta) lv
CREATE OR REPLACE VIEW "M3_MESA_CHARGES_VW" AS SELECT * FROM "MONGO"."PUBLIC"."M3_MESA_CHARGES" WHERE UUID NOT IN (select UUID from "MONGO"."PUBLIC"."SP_STAFF");
CREATE OR REPLACE VIEW "M3_MESA_LAUNCH_SESSIONS_VW" AS SELECT "PUBLIC"."M3_MESA_META_VW"."_ID" AS "_ID", CAST("PUBLIC"."M3_MESA_META_VW"."VALUE" AS timestamp) AS "timestamp", "mesa"."_ID" AS "mesa_merchant_id", "mesa"."UUID" AS "uuid", "mesa"."_CREATED_AT_PT" AS "mesa_merchant_installed_at_pt" FROM "MONGO"."PUBLIC"."M3_MESA_META_VW" LEFT JOIN "MONGO"."PUBLIC"."M3_MESA_VW" "mesa" ON "PUBLIC"."M3_MESA_META_VW"."PARENT_ID" = "mesa"."_ID" WHERE "PUBLIC"."M3_MESA_META_VW"."NAME" = 'launchsessiondate';

CREATE OR REPLACE VIEW "M3_MESA_TASKS_VW" AS SELECT *,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT,
    (select max(_ID) from "M3_MESA" t2 where t1.uuid = t2.uuid) as MESA_ID,
    (metadata:"trigger":"step_type"::string = 'input' AND (metadata:"unbillable_reason"::string IS NULL OR metadata:"unbillable_reason"::string != 'unbilled_automation_entitlement')  AND STATUS NOT IN ('ready', 'skip')) as is_billable,
    IFNULL(metadata:parents[0]:"task_id"::string, _ID) as AUTOMATION_ID,
    metadata:automation:"_id"::string as WORKFLOW_ID,
    metadata:"child_fails"::string as CHILD_FAILS
    FROM MONGO.PUBLIC.M3_MESA_TASKS t1 WHERE UUID NOT IN (select UUID from SP_STAFF) and __HEVO__MARKED_DELETED = false;

CREATE OR REPLACE VIEW "M3_MESA_RUNS_VW" AS SELECT *,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', _CREATED_AT) as _CREATED_AT_PT,
    STATUS NOT IN ('ready', 'skip') as is_executed,
    metadata:"unbillable_reason"::string as unbillable_reason,
    metadata:automation:"_id"::string as WORKFLOW_ID,
    metadata:"child_fails"::string as FAILURES
    FROM MONGO.PUBLIC.M3_MESA_TASKS t1 WHERE
        metadata:"trigger":"step_type"::string = 'input' AND
        UUID NOT IN (select UUID from SP_STAFF) and __HEVO__MARKED_DELETED = false;
    

CREATE OR REPLACE VIEW mongo.public.m3_mesa_billing_vw AS 
    SELECT 
        ROW_NUMBER() over (order by _ID DESC) as _ID, 
        _id as PARENT_ID, 
        billing:"method"."name"::STRING as method_name, 
        to_timestamp(billing:"overage"."bucket_end"::INT, 3) as overage_bucket_end, 
        to_timestamp(billing:"overage"."bucket_start"::INT, 3) as overage_bucket_start, 
        billing:"overage"."bypass_until"::STRING as overage_bypass_until, 
        billing:"overage"."last_count"::FLOAT as overage_last_count, 
        billing:"plan"."days_complete"::FLOAT as plan_days_complete, 
        billing:"plan"."id"::STRING as plan_id, 
        billing:"plan"."percent_complete"::FLOAT as plan_percent_complete, 
        billing:"plan"."percent_used"::FLOAT as plan_percent_used, 
        to_timestamp(IFNULL(billing:"plan"."start"::INT, billing:"plan_start"::INT), 3) as plan_start, 
        billing:"plan"."status"::STRING as plan_status, 
        billing:"plan"."used"::FLOAT as plan_used, 
        billing:"plan_name"::STRING as plan_name, 
        billing:"method"."shopify_id"::FLOAT as method_shopify_id, 
        billing:"plan"."trial_days"::FLOAT as plan_trial_days, 
        to_timestamp(billing:"plan"."trial_ends"::INT, 3) as plan_trial_ends, 
        billing:"plan"."updated_at"::STRING as plan_updated_at, 
        billing:"plan"."billing_on"::STRING as plan_billing_on, 
        to_timestamp(IFNULL(billing:"plan"."end"::INT, billing:"plan_end"::INT), 3) as plan_end, 
        billing:"plan"."overlimit_date"::STRING as plan_overlimit_date, 
        billing:"plan"."balance_remaining"::STRING as plan_balance_remaining, 
        billing:"method"."chargebee_id"::STRING as method_chargebee_id, 
        billing:"plan_volume"::FLOAT as plan_volume, 
        IFNULL(billing:"plan"."interval"::STRING, billing:"plan_interval"::STRING) as plan_interval, 
        billing:"plan_price"::STRING as plan_price, 
        billing:"plan"."created_at"::STRING as plan_created_at, 
        billing:"plan"."balance_used"::STRING as plan_balance_used, 
        billing:"plan_type"::STRING as plan_type
    FROM mongo.public.m3_mesa
    WHERE UUID NOT IN (select UUID from SP_STAFF);


CREATE OR REPLACE VIEW "SP_MESA_WORKFLOWS_FACTS_VW" AS
with inputs as (
    select * from M3_MESA_TRIGGERS_VW t2 where t2.step_type = 'input' and weight = 0
    ),
    outputs as (
        select * from M3_MESA_TRIGGERS_VW t2 where t2.step_type = 'output'
    )
    SELECT workflows._ID as WORKFLOW_ID,
        workflows.MESA_ID as MESA_MERCHANT_ID,
        (select count(*) from M3_MESA_TRIGGERS_VW where automation = workflows._ID) as WORKFLOW_NUM_STEPS,
        inputs.type as WORKFLOW_FIRST_STEP_APP,
        outputs.type as WORKFLOW_LAST_STEP_APP,
        (select min(_CREATED_AT_PT) from M3_MESA_TASKS_VW t2 where t2.metadata:automation:_id::varchar = workflows._ID and is_billable = true) as AUTOMATION_FIRST_RUN_AT,
        (select min(_CREATED_AT_PT) from M3_MESA_TASKS_VW t2 where t2.metadata:automation:_id::varchar = workflows._ID and is_billable = true and t2.status = 'success') as AUTOMATION_FIRST_RUN_SUCCCESSFUL_AT,
        (select count(*) from M3_MESA_TASKS_VW t2 where t2.metadata:automation:_id::varchar = workflows._ID and is_billable = true) as AUTOMATIONS_NUM_RUN,
        (select count(*) from M3_MESA_TASKS_VW t2 where t2.metadata:automation:_id::varchar = workflows._ID and is_billable = true and t2.status = 'success') as AUTOMATIONS_NUM_RUN_SUCCESSFUL
    FROM "MONGO"."PUBLIC"."M3_MESA_AUTOMATIONS_VW" workflows
    left join inputs on inputs.automation = workflows._ID
    left join outputs on outputs.automation = workflows._ID and outputs.weight = (select max(weight) from M3_MESA_TRIGGERS_VW t3 where t3.automation = workflows._ID and t3.step_type = 'output')
    WHERE workflows.UUID NOT IN (select UUID from SP_STAFF)
    order by WORKFLOW_ID desc;


SELECT
  col1,
  col2,
  col3,
  col4
FROM table
QUALIFY ROW_NUMBER() OVER (PARTITION BY col1, col2 ORDER BY col1, col2) = 1



CREATE OR REPLACE VIEW "SP_MESA_MERCHANTS_CLIENTSIDE_INSTALL_SOURCES_VW" AS 
  SELECT
    "install"."USER_ID" as uuid,
    coalesce("page"."CONTEXT_CAMPAIGN_SOURCE", REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("page"."CONTEXT_PAGE_REFERRER", '(.*)apps\.shopify\.com(.*)', 'shopify'), '(.*)www\.google\.com(.*)', 'google'), '(.*)www\.getmesa\.com(.*)?', 'getmesa')) as combined_source,
    "page"."CONTEXT_CAMPAIGN_SOURCE" as campaign_source,
    "page"."CONTEXT_CAMPAIGN_NAME" as campaign_name,
    "page"."CONTEXT_CAMPAIGN_MEDIUM" as campaign_medium,
    "page"."CONTEXT_CAMPAIGN_CONTENT" as campaign_content,
    "page"."CONTEXT_PAGE_REFERRER" as referrer,
    "page"."ANONYMOUS_ID" as anonymous_id,
    REGEXP_REPLACE("page"."CONTEXT_PAGE_URL", '(.*)app\.getmesa\.com(.*)', '') as getmesa_page_url
    FROM "MONGO"."GETMESA"."PAGES" "page"
    LEFT JOIN "MONGO"."GETMESA"."PAGES" "install"  
      ON "page"."ANONYMOUS_ID" = "install"."ANONYMOUS_ID"
      AND "page"."TIMESTAMP" <= "install"."TIMESTAMP"
    WHERE "page"."TIMESTAMP" IN (
      SELECT MIN("TIMESTAMP") FROM "MONGO"."GETMESA"."PAGES" WHERE ("CONTEXT_CAMPAIGN_SOURCE" != '' 
        OR "CONTEXT_PAGE_REFERRER" like '%apps.shopify.com%'
        OR "CONTEXT_PAGE_REFERRER" like '%www.google.com%'
      )
      GROUP BY "ANONYMOUS_ID" 
    )
    AND (lower("install"."CONTEXT_PAGE_URL") like '%/apps/mesa/install%')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "install"."USER_ID" ORDER BY "install"."TIMESTAMP" DESC) = 1
    ORDER BY "install"."TIMESTAMP" DESC;


CREATE OR REPLACE VIEW "SP_MESA_MERCHANTS_INSTALL_SOURCES_VW" AS 
  SELECT
    "server"."UUID" as uuid,
    "server"."CREATED_AT" as created_at,
    "client"."COMBINED_SOURCE" as client_source,
    coalesce("client"."COMBINED_SOURCE", concat("server"."UTM_SOURCE")) as combined_source,
    coalesce("client"."CAMPAIGN_SOURCE", "server"."UTM_SOURCE") as campaign_source,
    coalesce("client"."CAMPAIGN_NAME", "server"."UTM_CAMPAIGN") as campaign_name,
    coalesce("client"."CAMPAIGN_SOURCE", "server"."UTM_MEDIUM") as campaign_medium,
    "client"."CAMPAIGN_CONTENT" as campaign_content,
    coalesce("client"."REFERRER", "server"."REFERER") as referrer,
    "client"."GETMESA_PAGE_URL" as getmesa_page_url,
    "client"."ANONYMOUS_ID" as anonymous_id
  FROM "MONGO"."PUBLIC"."M3_MESA_INSTALLS" "server"
  LEFT JOIN "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_CLIENTSIDE_INSTALL_SOURCES_VW" "client"
    ON "server"."UUID" = "client"."UUID"
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "server"."UUID" ORDER BY "server"."CREATED_AT" DESC) = 1
  ORDER BY "server"."CREATED_AT" DESC;


CREATE OR REPLACE VIEW "SP_MESA_MERCHANTS_FACTS_BY_UUID_VW" AS 
  SELECT * FROM SP_MESA_MERCHANTS_FACTS_VW
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "UUID" ORDER BY "INSTALLED_AT_PT" DESC) = 1
    ORDER BY INSTALLED_AT_PT DESC;


-- Hourly task to rebuild the Mesa Merchants (Fact Table)
CREATE OR REPLACE TASK mesa_merchants_fact_table
  WAREHOUSE = MONGO_WAREHOUSE
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  CREATE OR REPLACE TABLE "SP_MESA_MERCHANTS_FACTS_VW" AS 
    WITH TASK_TBL AS (
        SELECT 
            _created_at, 
            MESA_ID, 
            IS_BILLABLE,
            ROW_NUMBER() OVER (PARTITION BY M3_MESA_TASKS_VW.MESA_ID ORDER BY _created_at asc) as POS
        FROM M3_MESA_TASKS_VW
        WHERE IS_BILLABLE = TRUE
    )
    SELECT 
        M3_MESA_VW.UUID as uuid,
        M3_MESA_VW._ID as mesa_merchant_id,
        _CREATED_AT_PT as INSTALLED_AT_PT,
        (select count(distinct AUTOMATION_ID) from M3_MESA_TASKS_VW where M3_MESA_TASKS_VW.MESA_ID = M3_MESA_VW._ID and M3_MESA_TASKS_VW._CREATED_AT > dateadd(day, -30, current_date()) AND M3_MESA_TASKS_VW.IS_BILLABLE = TRUE) as num_automations_last_30,
        (select count(distinct AUTOMATION_ID) from M3_MESA_TASKS_VW where M3_MESA_TASKS_VW.MESA_ID = M3_MESA_VW._ID and M3_MESA_TASKS_VW._CREATED_AT > dateadd(day, -90, current_date()) AND M3_MESA_TASKS_VW.IS_BILLABLE = TRUE) as num_automations_last_90,
        (select count(distinct AUTOMATION_ID) from M3_MESA_TASKS_VW where M3_MESA_TASKS_VW.MESA_ID = M3_MESA_VW._ID AND M3_MESA_TASKS_VW.IS_BILLABLE = TRUE) as num_automations_all_time,
        (select count(*) from M3_MESA_AUTOMATIONS_VW WHERE M3_MESA_AUTOMATIONS_VW.MESA_ID = M3_MESA_VW._ID AND M3_MESA_AUTOMATIONS_VW._CREATED_AT > dateadd(day, -30, current_date())) num_workflows_created_last_30,
        (select count(*) from M3_MESA_AUTOMATIONS_VW WHERE M3_MESA_AUTOMATIONS_VW.MESA_ID = M3_MESA_VW._ID AND M3_MESA_AUTOMATIONS_VW._CREATED_AT > dateadd(day, -90, current_date())) num_workflows_created_last_90,
        (select count(*) from M3_MESA_AUTOMATIONS_VW WHERE M3_MESA_AUTOMATIONS_VW.MESA_ID = M3_MESA_VW._ID) num_workflows_created_all_time,
        (select count(*) from M3_MESA_AUTOMATIONS_VW WHERE M3_MESA_AUTOMATIONS_VW.MESA_ID = M3_MESA_VW._ID AND M3_MESA_AUTOMATIONS_VW.ENABLED = TRUE) num_workflows_enabled,
        (num_automations_all_time >= 50) as is_activated,
        (SELECT max(_created_at) FROM TASK_TBL where TASK_TBL.MESA_ID = M3_MESA_VW._ID AND POS = 50) as activated_at,
        shopify_plan_name,
        DATEDIFF(day, M3_MESA_VW._CREATED_AT, (select min(_CREATED_AT) from M3_MESA_TASKS_VW where M3_MESA_TASKS_VW.MESA_ID = M3_MESA_VW._ID and M3_MESA_TASKS_VW._CREATED_AT > M3_MESA_VW._CREATED_AT)) as days_from_install_to_first_automation,
        DATEDIFF(day, M3_MESA_VW._CREATED_AT, (select min(_CREATED_AT) from M3_MESA_AUTOMATIONS_VW where M3_MESA_AUTOMATIONS_VW.MESA_ID = M3_MESA_VW._ID  and M3_MESA_AUTOMATIONS_VW._CREATED_AT > M3_MESA_VW._CREATED_AT)) as days_from_install_to_first_workflow,
        (select IFNULL(sum(inc_amount), 0) from SP_MESA_DAU where SP_MESA_DAU.USER_ID = M3_MESA_VW._ID) as ltv_all_time,
        (select IFNULL(sum(inc_amount), 0) from SP_MESA_DAU where SP_MESA_DAU.USER_ID = M3_MESA_VW._ID and DT > dateadd(day, -30, current_date())) as ltv_last_30,
        (select IFNULL(sum(inc_amount), 0) from SP_MESA_DAU where SP_MESA_DAU.USER_ID = M3_MESA_VW._ID and DT > dateadd(day, -90, current_date())) as ltv_last_90,
        "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_INSTALL_SOURCES_VW"."COMBINED_SOURCE" as combined_source,
        "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_INSTALL_SOURCES_VW"."CAMPAIGN_SOURCE" as campaign_source,
        "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_INSTALL_SOURCES_VW"."CAMPAIGN_NAME" as campaign_name,
        "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_INSTALL_SOURCES_VW"."CAMPAIGN_MEDIUM" as campaign_medium,
        "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_INSTALL_SOURCES_VW"."GETMESA_PAGE_URL" as getmesa_page_url,
        "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_INSTALL_SOURCES_VW"."ANONYMOUS_ID" as anonymous_id
    FROM M3_MESA_VW
    LEFT JOIN "MONGO"."PUBLIC"."M3_MESA_BILLING_VW" ON "MONGO"."PUBLIC"."M3_MESA_BILLING_VW"."PARENT_ID" = "M3_MESA_VW"."_ID"
    LEFT JOIN "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_INSTALL_SOURCES_VW" ON "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_INSTALL_SOURCES_VW"."UUID" = "M3_MESA_VW"."UUID"
    WHERE shopify_plan_name NOT IN ('affiliate', 'partner_test', 'plus_partner_sandbox')
    AND "M3_MESA_BILLING_VW"."PLAN_NAME" IS NOT NULL;
    
ALTER TASK mesa_merchants_fact_table RESUME;

-- Mesa DAU task
CREATE OR REPLACE TABLE "PUBLIC"."SP_MESA_DAU" ("ID" INT IDENTITY(1,1), "DT" TIMESTAMP, USER_ID VARCHAR, UUID VARCHAR, DAILY_PLAN_REVENUE FLOAT, DAILY_USAGE_REVENUE FLOAT, INC_AMOUNT FLOAT, PRIMARY KEY (id));

CREATE OR REPLACE TASK MESA_DAU
  WAREHOUSE = MONGO_WAREHOUSE
  schedule = 'USING CRON 0 21 * * * America/Los_Angeles'
AS 
  INSERT INTO SP_MESA_DAU (DT, USER_ID, UUID, DAILY_PLAN_REVENUE, DAILY_USAGE_REVENUE, INC_AMOUNT) 
    WITH CHARGES AS (
        SELECT 
            "PUBLIC"."M3_MESA_CHARGES_VW"."MERCHANT_ID" as "MERCHANT_ID",
            sum("PUBLIC"."M3_MESA_CHARGES_VW"."BILLED_AMOUNT") AS "BILLED_AMOUNT"
        FROM "MONGO"."PUBLIC"."M3_MESA_CHARGES_VW"
        WHERE
            datediff(day, CREATED_AT, current_date()) = 0
        GROUP BY "PUBLIC"."M3_MESA_CHARGES_VW"."MERCHANT_ID"
    )
    SELECT 
        current_date() as dt, 
        "Mesa Merchants"._ID as user_id, 
        UUID, 
        IFF(PLAN_PRICE = '', '0', IFF(PLAN_INTERVAL = 'annual', PLAN_PRICE/365, PLAN_PRICE/30)) as DAILY_PLAN_REVENUE, 
        IFNULL(CHARGES."BILLED_AMOUNT", 0) as DAILY_USAGE_REVENUE,
        (DAILY_PLAN_REVENUE + DAILY_USAGE_REVENUE) as inc_amount
    FROM "MONGO"."PUBLIC"."M3_MESA_BILLING_VW"
    LEFT JOIN "MONGO"."PUBLIC"."M3_MESA_VW" "Mesa Merchants" 
        ON "MONGO"."PUBLIC"."M3_MESA_BILLING_VW"."PARENT_ID" = "Mesa Merchants"."_ID"
    LEFT JOIN M3_MESA_ENTITLEMENTS_VW 
        ON M3_MESA_ENTITLEMENTS_VW.parent_ID = "Mesa Merchants"."_ID" 
        AND M3_MESA_ENTITLEMENTS_VW.name = 'price_per_action'
    LEFT JOIN CHARGES
        ON CHARGES.MERCHANT_ID = "Mesa Merchants"."_ID"
    WHERE 
        (
            "MONGO"."PUBLIC"."M3_MESA_BILLING_VW"."PLAN_TRIAL_ENDS" < current_date()
            OR "MONGO"."PUBLIC"."M3_MESA_BILLING_VW"."PLAN_TRIAL_ENDS" IS NULL
        ) 
        AND "Mesa Merchants".STATUS = 'active' 
        AND SHOPIFY_PLAN_NAME NOT IN ('frozen', 'cancelled', 'fraudulent') 
        AND inc_amount > 0;

ALTER TASK MESA_DAU set timezone = 'America/Los_Angeles';
ALTER TASK MESA_DAU RESUME;

-- Daily task to record custom Mesa applications into DAU table
-- Since these values are hardcoded, they must be changed manually 
-- if plan price changes or merchant ends their subscription
CREATE OR REPLACE TASK MESA_DAU_CUSTOM_APP
  WAREHOUSE = MONGO_WAREHOUSE
  schedule = 'USING CRON 0 21 * * * America/Los_Angeles'
AS 
    INSERT INTO SP_MESA_DAU (DT, USER_ID, UUID, DAILY_PLAN_REVENUE, DAILY_USAGE_REVENUE, INC_AMOUNT)
    VALUES 
        (current_date(),'custom-app-ideou-dev', 'ideou-dev', 11.66, 0, 11.66),
        (current_date(), 'custom-app-dev-culturefly', 'dev-culturefly', 78.33, 0, 78.33),
        (current_date(), 'custom-app-dev-emson', 'dev-emson', 125.66, 0, 125.66),
        (current_date(), 'custom-app-modern-times-beers', 'modern-times-beers', 6.63, 0, 6.63),
        (current_date(), 'custom-app-dev-hawaii-tee-times', 'dev-hawaii-tee-times', 5.00, 0, 5.00);

ALTER TASK MESA_DAU_CUSTOM_APP set timezone = 'America/Los_Angeles';
ALTER TASK MESA_DAU_CUSTOM_APP RESUME;


-- Mesa Weekly Snapshots table for Master Funnel
CREATE OR REPLACE TABLE "PUBLIC"."SP_MESA_FUNNEL_SNAPSHOTS" ("ID" INT IDENTITY(1,1), "DT" TIMESTAMP, "ACTIVATED" INT, "50_PLUS_AUTOMATIONS" INT, "HAS_ENABLED_WORKFLOW" INT, "HAS_LTV" INT, PRIMARY KEY (id));
ALTER TABLE "PUBLIC"."SP_MESA_FUNNEL_SNAPSHOTS" ADD "ACTIVE_USERS" INT, "ACTIVE_WORKFLOWS" INT, "CHURNED_CUSTOMERS" INT, "CHURNED_USERS" INT, "ARR" FLOAT;
ALTER TABLE "PUBLIC"."SP_MESA_FUNNEL_SNAPSHOTS" RENAME COLUMN "HAS_LTV" TO "ACTIVE_CUSTOMERS";

CREATE OR REPLACE TASK MESA_FUNNEL_SNAPSHOTS
    WAREHOUSE = MONGO_WAREHOUSE
    schedule = 'USING CRON 0 21 * * 0 America/Los_Angeles'
AS 
    INSERT INTO SP_MESA_FUNNEL_SNAPSHOTS ("DT", "ACTIVATED", "50_PLUS_AUTOMATIONS", "HAS_ENABLED_WORKFLOW", "ACTIVE_CUSTOMERS", "ACTIVE_USERS", "ACTIVE_WORKFLOWS", "CHURNED_CUSTOMERS", "CHURNED_USERS", "ARR") 
       SELECT
            current_date(),
            sum(CASE WHEN "PUBLIC"."SP_MESA_MERCHANTS_FACTS_VW"."IS_ACTIVATED" = 1 THEN 1 ELSE 0.0 END) AS "ACTIVATED", 
            sum(CASE WHEN "PUBLIC"."SP_MESA_MERCHANTS_FACTS_VW"."NUM_AUTOMATIONS_LAST_30" >= 50 THEN 1 ELSE 0.0 END) AS "50_PLUS_AUTOMATIONS", 
            sum(CASE WHEN "PUBLIC"."SP_MESA_MERCHANTS_FACTS_VW"."NUM_WORKFLOWS_ENABLED" > 0 THEN 1 ELSE 0.0 END) AS "HAS_ENABLED_WORKFLOW",
            (SELECT count(distinct "PUBLIC"."SP_MESA_DAU"."UUID") AS "count"
                FROM "MONGO"."PUBLIC"."SP_MESA_DAU"
                WHERE ("PUBLIC"."SP_MESA_DAU"."DT" >= date_trunc("month", CAST(dateadd(month, -1, CAST(current_timestamp() AS timestamp)) AS timestamp))
                AND "PUBLIC"."SP_MESA_DAU"."DT" < date_trunc("month", CAST(current_timestamp() AS timestamp)))
            ) AS "ACTIVE_CUSTOMERS",
            (SELECT count(distinct "UUID") AS "count" FROM "MONGO"."PUBLIC"."SP_CONSTELLATION_FACTS_VW" WHERE "MESA_IS_LIVE" = TRUE) AS "ACTIVE_USERS",
            (SELECT count(*) FROM "MONGO"."PUBLIC"."SP_CONSTELLATION_FACTS_VW"
                LEFT JOIN "MONGO"."PUBLIC"."M3_MESA_AUTOMATIONS_VW" "Mesa Workflows" ON "PUBLIC"."SP_CONSTELLATION_FACTS_VW"."UUID" = "Mesa Workflows"."UUID"
                WHERE ("PUBLIC"."SP_CONSTELLATION_FACTS_VW"."MESA_IS_LIVE" = TRUE
                AND "Mesa Workflows"."ENABLED" = TRUE AND ("Mesa Workflows"."TEMPLATE" <> 'shopify/order/send_order_report_card_email'
                    OR "Mesa Workflows"."TEMPLATE" IS NULL))
            ) AS "ACTIVE_WORKFLOWS",
            -- FROM https://shoppad.metabaseapp.com/question/771-merchants-that-churned-last-30-days/notebook
            (SELECT count(*) FROM "MONGO"."PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"
                LEFT JOIN "MONGO"."PUBLIC"."M3_MESA_VW" "Mesa Merchants" ON (date_trunc("minute", CAST("PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"."INSTALLED_ON" AS timestamp)) = date_trunc("minute", CAST("Mesa Merchants"."_CREATED_AT" AS timestamp))
                AND "PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"."UUID" = "Mesa Merchants"."UUID") LEFT JOIN "MONGO"."PUBLIC"."M3_MESA_BILLING_VW" "Mesa Merchants: Billing" ON "Mesa Merchants"."_ID" = "Mesa Merchants: Billing"."PARENT_ID"
                WHERE ("PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"."UNINSTALLED_ON" >= date_trunc("day", CAST(dateadd(day, -7, CAST(current_timestamp() AS timestamp)) AS timestamp)) AND "PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"."UNINSTALLED_ON" < date_trunc("day", CAST(current_timestamp() AS timestamp))
                    AND ("Mesa Merchants: Billing"."PLAN_PRICE" != '' AND TO_NUMBER("Mesa Merchants: Billing"."PLAN_PRICE", 10, 2) > 0))
            ) AS "CHURNED_CUSTOMERS",
            (SELECT count(*) FROM "MONGO"."PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"
                LEFT JOIN "MONGO"."PUBLIC"."M3_MESA_VW" "Mesa Merchants" ON (date_trunc("minute", CAST("PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"."INSTALLED_ON" AS timestamp)) = date_trunc("minute", CAST("Mesa Merchants"."_CREATED_AT" AS timestamp))
                AND "PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"."UUID" = "Mesa Merchants"."UUID") LEFT JOIN "MONGO"."PUBLIC"."M3_MESA_BILLING_VW" "Mesa Merchants: Billing" ON "Mesa Merchants"."_ID" = "Mesa Merchants: Billing"."PARENT_ID"
                WHERE ("PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"."UNINSTALLED_ON" >= date_trunc("day", CAST(dateadd(day, -7, CAST(current_timestamp() AS timestamp)) AS timestamp)) AND "PUBLIC"."SP_MESA_INSTALL_UNINSTALL_EVENT_VW"."UNINSTALLED_ON" < date_trunc("day", CAST(current_timestamp() AS timestamp)))
            ) AS "CHURNED_USERS",
            ROUND((12.167 * (SELECT sum("PUBLIC"."SP_MESA_DAU"."INC_AMOUNT") FROM "MONGO"."PUBLIC"."SP_MESA_DAU"
                WHERE ("PUBLIC"."SP_MESA_DAU"."DT" >= date_trunc("day", CAST(dateadd(day, -30, CAST(current_timestamp() AS timestamp)) AS timestamp))
                    AND "PUBLIC"."SP_MESA_DAU"."DT" < date_trunc("day", CAST(current_timestamp() AS timestamp))))
            ), 2) AS "ARR"
        FROM "MONGO"."PUBLIC"."SP_MESA_MERCHANTS_FACTS_VW"
        LEFT JOIN "MONGO"."PUBLIC"."SP_CONSTELLATION_FACTS_VW" "CONSTELLATION" ON "PUBLIC"."SP_MESA_MERCHANTS_FACTS_VW"."UUID" = "CONSTELLATION"."UUID"
        WHERE "CONSTELLATION"."MESA_IS_LIVE" = TRUE;
ALTER TASK MESA_FUNNEL_SNAPSHOTS set timezone = 'America/Los_Angeles';
ALTER TASK MESA_FUNNEL_SNAPSHOTS RESUME;


--- Example of expanding variant object columns into new views (run these as PIPELINE user)
call create_view_over_json('MONGO.PUBLIC.M3_TRACKTOR', 'BILLING', 'M3_TRACKTOR_BILLING_VW', 'match col case', 'string');
call create_view_over_json('MONGO.PUBLIC.M3_MESA', 'WIZARD', 'M3_MESA_WIZARD_VW', 'match col case', 'string');
call create_view_over_json('MONGO.PUBLIC.M3_MESA', 'ANALYTICS', 'M3_MESA_ANALYTICS_VW', 'match col case', 'string');

-- Rebuild M3_MESA_TASKS_METADATA_VW.  We customize the data generated by the call quite a bit to limit data transfer + match column names to legacy values
-- call create_view_over_json('MONGO.PUBLIC.M3_MESA_TASKS', 'METADATA', 'MONGO.PUBLIC.M3_MESA_TASKS_METADATA_VW', 'match col case', 'string');
CREATE OR REPLACE VIEW MONGO.PUBLIC.M3_MESA_TASKS_METADATA_VW AS 
SELECT 
ROW_NUMBER() over (order by _ID DESC) as _ID, 
_id as PARENT_ID, 
METADATA:"memory"::STRING as "memory", 
METADATA:"enqueued"::STRING as "enqueued", 
METADATA:"updated_at"::STRING as "updated_at", 
METADATA:"replayed_by"::STRING as "replayed_by", 
METADATA:"source"::STRING as "source", 
METADATA:"unbillable_reason"::STRING as "unbillable_reason", 
METADATA:"depth_parent_id"::STRING as "depth_parent_id", 
METADATA:"is_test"::STRING as "is_test", 
METADATA:"trigger"."_id"::STRING as "trigger__id", 
METADATA:"trigger"."trigger_name"::STRING as "trigger_trigger_name",
METADATA:"trigger"."trigger_key"::STRING as "trigger_trigger_key", 
METADATA:"trigger"."step_type"::STRING as "trigger_step_type", 
METADATA:"active"::STRING as "active", 
METADATA:"automation"."_id"::STRING as "automation__id",
METADATA:"automation"."automation_name"::STRING as "automation_automation_name", 
METADATA:"automation"."automation_key"::STRING as "automation_automation_key", 
METADATA:"child_fails"::STRING as "child_fails", 
METADATA:"parents"::STRING as "parents", 
METADATA:"billable"::STRING as "billable", 
METADATA:"execution_time"::STRING as "execution_time", 
METADATA:"replay_of"::STRING as "replay_of", 
METADATA:"created_at"::STRING as "created_at", 
METADATA:"payload_hash"::STRING as "payload_hash", 
METADATA:"is_premium"::STRING as "is_premium", 
METADATA:"_id"::STRING as "_id",
METADATA:"external_id"::STRING as "external_id", 
METADATA:"external_label"::STRING as "external_label", 
METADATA:"replay_count"::STRING as "replay_count"
FROM MONGO.PUBLIC.M3_MESA_TASKS;

-- Create view with Mesa merchant install/uninstall events
CREATE OR REPLACE VIEW MONGO.PUBLIC.SP_MESA_INSTALL_UNINSTALL_EVENT_VW AS 
    SELECT 
        SHOPPAD_INSTALL.USER_ID AS "UUID",
        SHOPPAD_INSTALL.TIMESTAMP as "INSTALLED_ON",
        SHOPPAD_UNINSTALL.TIMESTAMP "UNINSTALLED_ON",
        datediff(day, INSTALLED_ON, IFNULL(UNINSTALLED_ON, current_date())) as "INSTALL_DURATION_DAYS",
        datediff(hour, INSTALLED_ON, IFNULL(UNINSTALLED_ON, current_date())) as "INSTALL_DURATION_HOURS"
    FROM "MONGO"."PHP"."SHOPPAD_INSTALL" "SHOPPAD_INSTALL"
    LEFT OUTER JOIN "MONGO"."PHP"."SHOPPAD_UNINSTALL" "SHOPPAD_UNINSTALL" 
        ON SHOPPAD_UNINSTALL.USER_ID = SHOPPAD_INSTALL.USER_ID
            AND SHOPPAD_UNINSTALL.HANDLE = 'mesa'
            AND SHOPPAD_UNINSTALL.TIMESTAMP = (
                SELECT min(TIMESTAMP) from "MONGO"."PHP"."SHOPPAD_UNINSTALL" 
                WHERE TIMESTAMP >= SHOPPAD_INSTALL.TIMESTAMP 
                    AND HANDLE = 'mesa' 
                    AND USER_ID = SHOPPAD_INSTALL.USER_ID
            )
    WHERE SHOPPAD_INSTALL.HANDLE = 'mesa'
        AND SHOPPAD_INSTALL.USER_ID NOT IN (
            SELECT UUID FROM SP_STAFF
        )
    ORDER BY UUID ASC, INSTALLED_ON DESC;
    

