-- models/marts/price_timeseries.sql
{{ config(materialized='table') }}

with cleaned as (
    select * from {{ ref('clean_prices') }}
),

with_lag as (
    select
        product_id,
        site_name,
        product_name,
        category,
        price,
        availability,
        scraped_at,
        lag(price) over (
            partition by product_id 
            order by scraped_at asc
        ) as previous_price
    from cleaned
)

select
    product_id,
    site_name,
    product_name,
    category,
    price,
    previous_price,
    availability,
    scraped_at,
    case
        when previous_price is null then 0
        when previous_price = 0 then 0
        else round(((price - previous_price) / previous_price) * 100, 2)
    end as price_change_pct,
    -- Rolling stats sur 7 derniers scrapes
    avg(price) over (
        partition by product_id
        order by scraped_at
        rows between 6 preceding and current row
    ) as price_rolling_avg,
    min(price) over (
        partition by product_id
        order by scraped_at
        rows between 6 preceding and current row
    ) as price_rolling_min,
    max(price) over (
        partition by product_id
        order by scraped_at
        rows between 6 preceding and current row
    ) as price_rolling_max
from with_lag
order by product_id, scraped_at
