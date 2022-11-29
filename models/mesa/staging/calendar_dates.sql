{{- dbt_utils.date_spine(
    datepart='day',
    start_date="to_date('" ~ var('start_date') ~"', 'yyyy-mm-dd')",
    end_date=pacific_timestamp("current_timestamp()") ~ '::DATE'
   )
}}
