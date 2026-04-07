{{ config(materialized='table') }}

with merchants as (
    select
        merchant_name,
        category,
        avg(merch_lat) as merch_lat,
        avg(merch_long) as merch_long
    from {{ ref('stg_transactions') }}
    group by merchant_name, category
)

select
    row_number() over (order by merchant_name) as merchant_id,
    merchant_name,
    category,
    merch_lat,
    merch_long
from merchants
