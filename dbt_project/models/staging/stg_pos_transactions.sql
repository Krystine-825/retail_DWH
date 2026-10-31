{{ config(materialized='view') }}

SELECT
    transaction_id AS order_id,
    TO_TIMESTAMP(timestamp, 'DD/MM/YYYY HH24:MI:SS') AS order_date,
    product_code AS sku,
    amount AS quantity,
    (total_value / amount) AS unit_price,
    total_value AS total_revenue,
    CASE
        WHEN status = 'Thành công' THEN 'COMPLETED'
        WHEN status = 'Hoàn trả' THEN 'CANCELLED'
        ELSE 'PENDING'
    END AS order_status,
    'pos' AS source_system
FROM {{ source('bronze', 'pos_transactions') }}