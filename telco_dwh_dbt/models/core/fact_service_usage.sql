WITH usage AS (
    SELECT * FROM {{ source('telco_source', 'stg_network_usage') }}
),
dim_cust AS (
    SELECT customer_sk, customer_id, valid_from, valid_to 
    FROM {{ ref('dim_customer') }}
)

SELECT 
    u.usage_id,
    c.customer_sk,
    CAST(TO_CHAR(u.usage_date, 'YYYYMMDD') AS INT) AS usage_date_sk,
    u.data_consumed_mb,
    u.dropped_calls_count,
    u.average_latency_ms
FROM usage u
LEFT JOIN dim_cust c 
    ON u.customer_id = c.customer_id 
    AND u.usage_date >= c.valid_from 
    AND u.usage_date < c.valid_to
