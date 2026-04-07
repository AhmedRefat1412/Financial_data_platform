{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'transactions_raw') }}
),

renamed as (
    select
        -- Customer Info
        cc_num              as customer_id,
        first               as first_name,
        last                as last_name,
        gender,
        city,
        state,
        zip,
        age,

        -- Merchant Info
        merchant            as merchant_name,
        category,
        merch_lat,
        merch_long,

        -- Transaction Info
        amt                 as amount,
        is_fraud,

        -- Time Info
        trans_date          as transaction_date,
        trans_hour          as transaction_hour,
        trans_month         as transaction_month,
        trans_year          as transaction_year,
        trans_dayofweek     as transaction_day_of_week
    from source
)

select * from renamed
