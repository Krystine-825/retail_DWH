{{ config(materialized='table') }}

WITH website AS (
    SELECT * FROM {{ ref('stg_website_orders') }}
),
shopee AS (
    SELECT * FROM {{ ref('stg_shopee_orders') }}
),
pos AS (
    SELECT * FROM {{ ref('stg_pos_transactions') }}
),

unioned_sales AS (
    SELECT * FROM website
    UNION ALL
    SELECT * FROM shopee
    UNION ALL
    SELECT * FROM pos
)

SELECT
    order_id,
    order_date,
    CAST(order_date AS DATE) AS date_key, -- Khóa để join với bảng dim_date sau này
    sku,
    quantity,
    unit_price,
    total_revenue,
    order_status,
    source_system
FROM unioned_sales
WHERE order_status = 'COMPLETED' -- Tài chính thường chỉ quan tâm đơn hàng thành công