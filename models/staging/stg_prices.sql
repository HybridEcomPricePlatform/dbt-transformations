{{ config(materialized='table') }}

with raw_data as (
    select * from {{ source('price_raw', 'price_events') }}
)

select
    cast(product_id as string) as product_id,
    cast(site_name as string) as site_name,
    cast(site_product_id as string) as site_product_id,
    cast(product_name as string) as product_name,
    cast(price as float64) as price,
    cast(currency as string) as currency,
    cast(availability as string) as availability,
    cast(category as string) as category,
    cast(image_url as string) as image_url,
    cast(source_url as string) as source_url,
    cast(rating as float64) as rating,
    cast(review_count as int64) as review_count,
    cast(scraped_at as timestamp) as scraped_at
from raw_data
