WITH
shop_lifespans AS (
    SELECT *
    FROM {{ ref('int_shop_lifespans') }}
),

shop_calendar AS (
    SELECT
        shop_subdomain,
        date_day AS dt
    FROM shop_lifespans
    INNER JOIN {{ ref('calendar_dates') }}
        ON dt BETWEEN first_dt AND last_dt
)

SELECT * FROM shop_calendar
