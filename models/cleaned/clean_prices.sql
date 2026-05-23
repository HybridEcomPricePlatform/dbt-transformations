{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_prices') }}
),

deduplicated as (
    select
        *,
        row_number() over (partition by product_id, scraped_at order by scraped_at desc) as rn
    from staging
),

filtered as (
    select *
    from deduplicated
    where rn = 1
      and price is not null
      and price > 0
      -- Filtrage de prix aberrants potentiels
      and price < 1000000
)

select
    product_id,
    site_name,
    site_product_id,
    product_name,
    price,
    currency,
    availability,
    category,
    image_url,
    source_url,
    scraped_at
from filtered
