

-- PUNTO 3

CREATE OR REPLACE TABLE keepcoding.ivr_detail AS
WITH calls AS (
    SELECT 
        ivr_id,
        phone_number,
        ivr_result,
        vdn_label,
        start_date,
        FORMAT_DATE('%Y%m%d', DATE(start_date)) AS start_date_id,
        end_date,
        FORMAT_DATE('%Y%m%d', DATE(end_date)) AS end_date_id,
        TIMESTAMP_DIFF(end_date, start_date, SECOND) AS total_duration,
        customer_segment,
        ivr_language,
        steps_module,
        module_aggregation
    FROM 
        keepcoding.ivr_calls
),
modules AS (
    SELECT 
        ivr_id,
        module_sequece,
        module_name,
        module_duration,
        module_result,
    FROM 
        keepcoding.ivr_modules
),
steps AS (
    SELECT 
        ivr_id,
        module_sequece,
        step_sequence,
        step_name,
        step_result,
        step_description_error,
        document_type,
        document_identification,
        customer_phone,
        billing_account_id
    FROM 
        keepcoding.ivr_steps
)
SELECT
    c.ivr_id AS calls_ivr_id,
    c.phone_number AS calls_phone_number,
    c.ivr_result AS calls_ivr_result,
    c.vdn_label AS calls_vdn_label,
    c.start_date AS calls_start_date,
    c.start_date_id AS calls_start_date_id,
    c.end_date AS calls_end_date,
    c.end_date_id AS calls_end_date_id,
    c.total_duration AS calls_total_duration,
    c.customer_segment AS calls_customer_segment,
    c.ivr_language AS calls_ivr_language,
    c.steps_module AS calls_steps_module,
    c.module_aggregation AS calls_module_aggregation,
    c.module_aggregation,
    m.module_sequece,
    m.module_name,
    m.module_duration,
    m.module_result,
    s.step_sequence,
    s.step_name,
    s.step_result,
    s.step_description_error,
    s.document_type,
    s.document_identification,
    s.customer_phone,
    s.billing_account_id
FROM 
    calls c
FULL JOIN 
    modules m
ON 
    c.ivr_id = m.ivr_id
FULL JOIN 
    steps s
ON 
    m.ivr_id = s.ivr_id
AND 
    m.module_sequece = s.module_sequece;


-- PUNTO 4

CREATE OR REPLACE TABLE keepcoding.ivr_detail AS 
SELECT 
    calls_ivr_id ,
    calls_vdn_label ,
    CASE 
        WHEN STARTS_WITH(calls_vdn_label, 'ATC') THEN 'FRONT' 
        WHEN STARTS_WITH(calls_vdn_label, 'TECH') THEN 'TECH'
        WHEN calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
        ELSE 'RESTO'
    END AS calls_vdn_aggregation 
    FROM keepcoding.ivr_detail

-- PUNTO 5

WITH clients_ivr_id AS (
    SELECT 
        calls_ivr_id,
        document_type,
        document_identification,
        ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS INT64) ORDER BY document_type) AS rn
    FROM 
        keepcoding.ivr_detail
)

SELECT 
    calls_ivr_id,
    document_type,
    document_identification
FROM 
    clients_ivr_id
WHERE 
    rn = 1;

-- PUNTO 6

WITH client_identification_phone AS (
    SELECT 
        calls_ivr_id,
        customer_phone,
        ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS INT64) ORDER BY customer_phone) AS rn
    FROM 
        keepcoding.ivr_detail
)

SELECT 
    calls_ivr_id,
    customer_phone
FROM 
    client_identification_phone
WHERE 
    rn = 1;

-- PUNTO 7


