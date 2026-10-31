{{ config(materialized='view') }}

SELECT
    order_id,
    CAST(created_at AS TIMESTAMP) AS order_date,
    sku,
    qty AS quantity,
    price AS unit_price,
    (qty * price) AS total_revenue,
    CASE
        WHEN status = 'complete' THEN 'COMPLETED'
        WHEN status IN ('canceled', 'closed') THEN 'CANCELLED'
        ELSE 'PENDING'
    END AS order_status,
    'website' AS source_system
FROM {{ source('bronze', 'website_orders') }}