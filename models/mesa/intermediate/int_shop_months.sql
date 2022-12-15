-- This is a translated query which can be run ON Snowflake. Originally written BY Jonathan Hsu (Tribe Capital),
-- who shared the same analytical pattern to run in PostgreSQL, linked ON this blog post:
-- https://tribecap.co/a-quantitative-approach-to-product-market-fit/

WITH dau AS (
    -- This part of the query can be pretty much anything.
    -- The only requirement is that it have three columns:
    --   dt, shop_subdomain, inc_amount
    -- Where dt is a date and shop_subdomain is some unique identifier for a user.
    -- Each dt-shop_subdomain pair should be unique in this table.
    -- inc_amount represents the amount of value that this user created ON dt.
    -- The most common CASE is
    --   inc_amount = incremental revenue FROM the user ON dt
    -- If you want to do L28 growth accounting, user inc_amount=1.
    -- The version here derives everything FROM the tutorial.yammer_events
    -- data set provided for free BY Mode.
    -- If you edit just this part to represent your data, the rest
    -- of the query should run just fine.
    -- The query here is a sample that works in the public Mode Analytics
    -- tutorial.
    SELECT
        shop_subdomain,
        dt,
        inc_amount
    FROM {{ ref('int_mesa_shop_days') }}
    WHERE is_active
),

mau AS (
    SELECT
        shop_subdomain,
        date_trunc('month', dt) AS month,
        sum(inc_amount) AS inc_amount
    FROM dau
    GROUP BY
        1,
        2
),

-- This determines the cohort date of each user. In this CASE we are
-- deriving it FROM DAU data but you can feel free to replace it with
-- registration date if that's more appropriate.
first_dt AS (
    SELECT
        shop_subdomain,
        min(dt) AS first_dt,
        date_trunc('week', min(dt)) AS first_week,
        date_trunc('month', min(dt)) AS first_month
    FROM dau
    GROUP BY 1
),

mau_decorated AS (
    SELECT
        mau.month,
        mau.shop_subdomain,
        mau.inc_amount,
        first_dt.first_month
    FROM mau
    INNER JOIN first_dt ON (mau.shop_subdomain = first_dt.shop_subdomain) and mau.inc_amount > 0
)

SELECT * FROM mau_decorated
