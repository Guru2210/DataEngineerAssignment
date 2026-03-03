SELECT 
    {{ dbt_utils.generate_surrogate_key(['plan_id']) }} AS plan_sk,
    plan_id,
    plan_name,
    monthly_fee,
    infrastructure_cost,
    data_limit_gb
FROM {{ source('telco_source', 'stg_service_plans') }}
