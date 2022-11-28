{{- dbt_utils.date_spine(
    datepart="day",
    start_date="to_date('" ~ var('start_date') ~"', 'yyyy-mm-dd')",
    end_date="current_date()"
   )
}}

