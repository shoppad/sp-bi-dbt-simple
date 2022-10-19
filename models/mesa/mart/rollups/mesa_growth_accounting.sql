-- This is a translated query which can be run ON Snowflake. Originally written BY Jonathan Hsu (Tribe Capital), 
-- who shared the same analytical pattern to run in PostgreSQL, linked ON this blog post:
-- https://tribecap.co/a-quantitative-approach-to-product-market-fit/

WITH dau AS (
    -- This part of the query can be pretty much anything.
    -- The only requirement is that it have three columns:
    --   dt, shop_id, inc_amt
    -- Where dt is a date and shop_id is some unique identifier for a user.
    -- Each dt-shop_id pair should be unique in this table.
    -- inc_amt represents the amount of value that this user created ON dt.
    -- The most common CASE is
    --   inc_amt = incremental revenue FROM the user ON dt
    -- If you want to do L28 growth accounting, user inc_amt=1.
    -- The version here derives everything FROM the tutorial.yammer_events
    -- data set provided for free BY Mode.
    -- If you edit just this part to represent your data, the rest
    -- of the query should run just fine.
    -- The query here is a sample that works in the public Mode Analytics
    -- tutorial.
    SELECT
        shop_id AS shop_id,
        to_date(charged_on_pt) AS dt,
        SUM(inc_amount) AS inc_amt
    FROM {{ ref('mesa_shop_days') }}
    GROUP by
        1,
        2
),

mau AS (
    SELECT
        date_trunc('month', dt) AS month,
        shop_id,
        sum(inc_amt) AS inc_amt
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
        shop_id,
        min(dt) AS first_dt,
        date_trunc('week', min(dt)) AS first_week,
        date_trunc('month', min(dt)) AS first_month
    FROM dau
    GROUP BY 1
),

mau_decorated AS (
    SELECT
        mau.month,
        mau.shop_id,
        mau.inc_amt,
        first_dt.first_month
    FROM mau
    INNER JOIN first_dt ON (mau.shop_id = first_dt.shop_id) and mau.inc_amt > 0
),

-- This is MAU growth accounting. Note that this does not require any
-- information about inc_amt. As discussed in the articles, these
-- quantities satisfy some identities:
-- MAU(t) = retained(t) + new(t) + resurrected(t)
-- MAU(t - 1 month) = retained(t) + churned(t)
mau_growth_accounting AS (
    SELECT
        coalesce(tm.month, dateadd(month, 1, lm.month)) AS month,
        count(distinct tm.shop_id) AS mau,
        count(distinct CASE WHEN lm.shop_id is not NULL THEN tm.shop_id
            ELSE NULL end) AS retained,
        count(distinct CASE WHEN tm.first_month = tm.month THEN tm.shop_id
            ELSE NULL end) AS new,
        count(distinct CASE WHEN tm.first_month != tm.month
                and lm.shop_id is NULL THEN tm.shop_id ELSE NULL END
        ) AS resurrected,
        -1 * count(distinct CASE WHEN tm.shop_id is NULL THEN lm.shop_id ELSE NULL end) AS churned
    FROM
        mau_decorated AS tm
    FULL OUTER JOIN mau_decorated AS lm ON (
        tm.shop_id = lm.shop_id
        and tm.month = dateadd(month, 1, lm.month)
        )
    GROUP BY 1
    ORDER BY 1
)

-- For MAU growth accuonting use this
SELECT * FROM mau_growth_accounting
