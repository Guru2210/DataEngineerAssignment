WITH customer_base AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        customer_segment,
        -- SCD Type 1: Broadcast the latest value to all historical rows
        FIRST_VALUE(customer_segment) OVER (PARTITION BY customer_id ORDER BY created_at DESC) as current_segment,
        FIRST_VALUE(first_name) OVER (PARTITION BY customer_id ORDER BY created_at DESC) as current_first_name,
        FIRST_VALUE(last_name) OVER (PARTITION BY customer_id ORDER BY created_at DESC) as current_last_name
    FROM {{ source('telco_source', 'stg_customers') }}
),

subscription_history AS (
    SELECT 
        s.customer_id,
        p.plan_name AS service_plan_type,
        s.start_date,
        s.end_date,
        s.status
    FROM {{ source('telco_source', 'stg_subscriptions') }} s
    LEFT JOIN {{ source('telco_source', 'stg_service_plans') }} p ON s.plan_id = p.plan_id
),

scd_logic AS (
    SELECT 
        c.customer_id,
        -- SCD Type 1 attributes 
        c.current_first_name AS first_name,
        c.current_last_name AS last_name,
        c.current_segment AS customer_segment,
        
        -- SCD Type 2 attribute
        sh.service_plan_type,
        
        -- SCD Type 3 attribute (Shows the previous plan on the current row)
        LAG(sh.service_plan_type) OVER (PARTITION BY c.customer_id ORDER BY sh.start_date) AS previous_service_plan,
        
        -- SCD Type 2 Metadata columns
        sh.start_date AS valid_from,
        COALESCE(sh.end_date, '2099-12-31'::DATE) AS valid_to,
        CASE WHEN sh.end_date IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM customer_base c
    JOIN subscription_history sh ON c.customer_id = sh.customer_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'valid_from']) }} as customer_sk,
    *
FROM scd_logic
