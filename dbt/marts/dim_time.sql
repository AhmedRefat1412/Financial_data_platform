{{ config(materialized='table') }}

with time_data as (
    select distinct
        transaction_date,
        transaction_hour,
        transaction_month,
        transaction_year,
        transaction_day_of_week
    from {{ ref('stg_transactions') }}
)

select
    row_number() over (order by transaction_date, transaction_hour) as time_id,
    transaction_date,
    transaction_hour,
    transaction_month,
    transaction_year,
    transaction_day_of_week
from time_data
