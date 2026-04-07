{{ config(materialized='table') }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

merchants as (
    select * from {{ ref('dim_merchant') }}
),

time_dim as (
    select * from {{ ref('dim_time') }}
)

select
    t.customer_id,
    m.merchant_id,
    td.time_id,
    t.amount,
    t.is_fraud,
    t.category
from transactions t
left join merchants m
    on t.merchant_name = m.merchant_name
left join time_dim td
    on t.transaction_date = td.transaction_date
    and t.transaction_hour = td.transaction_hour
