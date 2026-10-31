{{ config(materialized='view') }}

SELECT
    order_sn AS order_id,
    CAST(order_date AS TIMESTAMP) AS order_date,
    REPLACE(sku, 'SP-', '') AS sku,
    quantity,
    original_price AS unit_price,
    (original_price * quantity) - shopee_fee - payment_fee AS total_revenue,
    CASE
        WHEN order_status = 'COMPLETED' THEN 'COMPLETED'
        WHEN order_status = 'CANCELLED' THEN 'CANCELLED'
        ELSE 'PENDING'
    END AS order_status,
    'shopee' AS source_system
FROM {{ source('bronze', 'shopee_orders') }}