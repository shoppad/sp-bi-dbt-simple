WITH
      -- THIS IS MAU GROWTH ACCOUNTING. NOTE THAT THIS DOES NOT REQUIRE ANY
    -- INFORMATION ABOUT INC_AMOUNT. AS DISCUSSED IN THE ARTICLES, THESE
    -- QUANTITIES SATISFY SOME IDENTITIES:
    -- MAU(T) = RETAINED(T) + NEW(T) + RESURRECTED(T)
    -- MAU(T - 1 week) = RETAINED(T) + CHURNED(T)

    MRR_GROWTH_ACCOUNTING AS (
        SELECT
            COALESCE(TM.week, DATEADD(week,1,LM.week)) AS week,
            SUM(TM.INC_AMOUNT) AS REV,
            SUM(
                CASE
                    WHEN TM.SHOP_SUBDOMAIN IS NOT NULL AND LM.SHOP_SUBDOMAIN IS NOT NULL
                        AND TM.INC_AMOUNT >= LM.INC_AMOUNT THEN LM.INC_AMOUNT
                    WHEN TM.SHOP_SUBDOMAIN IS NOT NULL AND LM.SHOP_SUBDOMAIN IS NOT NULL
                        AND TM.INC_AMOUNT < LM.INC_AMOUNT THEN TM.INC_AMOUNT
                    ELSE 0
                END
            ) AS RETAINED,
            SUM(
                CASE WHEN TM.FIRST_week = TM.week THEN TM.INC_AMOUNT
                ELSE 0 END
            ) AS NEW,
            SUM(
                CASE WHEN TM.week != TM.FIRST_week AND TM.SHOP_SUBDOMAIN IS NOT NULL
                    AND LM.SHOP_SUBDOMAIN IS NOT NULL AND TM.INC_AMOUNT > LM.INC_AMOUNT
                    AND LM.INC_AMOUNT > 0 THEN TM.INC_AMOUNT - LM.INC_AMOUNT
                ELSE 0 END
            ) AS EXPANSION,
            SUM(
                CASE WHEN TM.SHOP_SUBDOMAIN IS NOT NULL
                    AND (LM.SHOP_SUBDOMAIN IS NULL OR LM.INC_AMOUNT = 0)
                    AND TM.INC_AMOUNT > 0 AND TM.FIRST_week != TM.week
                    THEN TM.INC_AMOUNT
                ELSE 0 END
            ) AS RESURRECTED,
            -1 * SUM(
                CASE
                    WHEN TM.week != TM.FIRST_week AND TM.SHOP_SUBDOMAIN IS NOT NULL
                        AND LM.SHOP_SUBDOMAIN IS NOT NULL
                        AND TM.INC_AMOUNT < LM.INC_AMOUNT AND TM.INC_AMOUNT > 0
                        THEN LM.INC_AMOUNT - TM.INC_AMOUNT
                ELSE 0 END
            ) AS CONTRACTION,
            -1 * SUM(
                CASE WHEN LM.INC_AMOUNT > 0 AND (TM.SHOP_SUBDOMAIN IS NULL OR TM.INC_AMOUNT = 0)
                THEN LM.INC_AMOUNT ELSE 0 END
            ) AS CHURNED
        FROM
            {{ ref('int_shop_weeks') }} TM
            FULL OUTER JOIN {{ ref('int_shop_weeks') }} LM ON (
                TM.SHOP_SUBDOMAIN = LM.SHOP_SUBDOMAIN
                    AND TM.week = DATEADD(week,1,LM.week)
            )
        GROUP BY 1
        ORDER BY 1
      )
      -- THESE NEXT TABLES ARE TO COMPUTE LTV VIA THE COHORTS_CUMULATIVE TABLE.
    -- THE LTV HERE IS BEING COMPUTED FOR WEEKLY COHORTS ON WEEKLY INTERVALS.
    -- THE QUERIES CAN BE MODIFIED TO COMPUTE IT FOR COHORTS OF ANY SIZE
    -- ON ANY TIME WINDOW FREQUENCY.

-- FOR WRR GROWTH ACCOUNTING USE THIS
SELECT * FROM MRR_GROWTH_ACCOUNTING WHERE week < DATE_TRUNC('week', CURRENT_DATE()) ORDER BY week DESC
