{{ config(materialized='table') }}

with customers as (
    select distinct
        customer_id,
        first_name,
        last_name,
        gender,
        city,
        state,
        zip,
        age
    from {{ ref('stg_transactions') }}
)

select * from customers
