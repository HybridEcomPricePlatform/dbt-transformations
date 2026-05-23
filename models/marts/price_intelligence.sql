{{ config(materialized='table') }}

with cleaned as (
    select * from {{ ref('clean_prices') }}
),

aggregated as (
    select * from {{ ref('agg_prices') }}
),

latest_prices as (
    select
        product_id,
        price as current_price,
        availability as current_availability,
        scraped_at as last_scraped_at,
        source_url,
        image_url
    from (
        select
            *,
            row_number() over (partition by product_id order by scraped_at desc) as rn
        from cleaned
    )
    where rn = 1
)

select
    a.product_id,
    a.site_name,
    a.product_name,
    a.category,
    l.current_price,
    l.current_availability,
    a.price_min,
    a.price_max,
    a.price_avg,
    a.price_change_pct,
    l.source_url,
    l.image_url,
    a.first_scraped_at,
    l.last_scraped_at
from aggregated a
join latest_prices l on a.product_id = l.product_id
