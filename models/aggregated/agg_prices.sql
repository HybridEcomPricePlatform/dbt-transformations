{{ config(materialized='table') }}

with cleaned_data as (
    select * from {{ ref('clean_prices') }}
),

windowed_prices as (
    select
        product_id,
        site_name,
        product_name,
        category,
        price,
        scraped_at,
        first_value(price) over (partition by product_id order by scraped_at asc) as first_price,
        first_value(price) over (partition by product_id order by scraped_at desc) as last_price
    from cleaned_data
),

aggregated as (
    select
        product_id,
        any_value(site_name) as site_name,
        any_value(product_name) as product_name,
        any_value(category) as category,
        min(price) as price_min,
        max(price) as price_max,
        avg(price) as price_avg,
        any_value(first_price) as first_price,
        any_value(last_price) as last_price,
        min(scraped_at) as first_scraped_at,
        max(scraped_at) as last_scraped_at,
        count(price) as price_points_count
    from windowed_prices
    group by product_id
)

select
    product_id,
    site_name,
    product_name,
    category,
    price_min,
    price_max,
    price_avg,
    case 
        when first_price > 0 then round(((last_price - first_price) / first_price) * 100, 2)
        else 0 
    end as price_change_pct,
    first_scraped_at,
    last_scraped_at,
    price_points_count
from aggregated
