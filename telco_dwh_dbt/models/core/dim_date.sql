WITH date_series AS (
    SELECT generate_series(
        '2020-01-01'::DATE,
        '2030-12-31'::DATE,
        '1 day'::interval
    )::DATE AS full_date
)

SELECT 
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INT) AS date_sk,
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    CASE 
        WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM date_series
