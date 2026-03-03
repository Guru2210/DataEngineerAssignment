WITH billing AS (
    SELECT * FROM {{ source('telco_source', 'stg_billing_invoices') }}
),
dim_cust AS (
    SELECT 
        customer_sk, 
        customer_id, 
        valid_from::timestamp as valid_from, -- Cast here for safety
        valid_to::timestamp as valid_to
    FROM {{ ref('dim_customer') }}
),
dim_plan AS (
    SELECT plan_sk, plan_id 
    FROM {{ ref('dim_service_plan') }}
)

SELECT 
    b.invoice_id,
    c.customer_sk,
    p.plan_sk,
    -- This handles the Date SK for your dim_date join
    CAST(TO_CHAR(b.billing_period_start::timestamp, 'YYYYMMDD') AS INT) AS billing_date_sk,
    b.amount_due,
    b.is_delayed
FROM billing b
LEFT JOIN dim_cust c 
    ON b.customer_id = c.customer_id 
    -- Adding explicit casts to the comparison logic
    AND b.billing_period_start::timestamp >= c.valid_from
    AND b.billing_period_start::timestamp < c.valid_to
LEFT JOIN dim_plan p
    -- Check: Usually subscription_id joins to a subscription table, 
    -- but if your logic maps it to plan_id, this works.
    ON b.subscription_id = p.plan_id
