CREATE OR REPLACE TABLE `keepcoding.ivr_summary` AS
WITH 
detail_vdn_aggregation AS (
  SELECT 
      calls_ivr_id,
      calls_vdn_label,
      CASE 
          WHEN STARTS_WITH(calls_vdn_label, 'ATC') THEN 'FRONT' 
          WHEN STARTS_WITH(calls_vdn_label, 'TECH') THEN 'TECH'
          WHEN calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
          ELSE 'RESTO'
      END AS vdn_aggregation 
  FROM keepcoding.ivr_detail
),

clients_ivr_id AS (
  SELECT 
      calls_ivr_id,
      document_type,
      document_identification,
      ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS INT64) ORDER BY document_identification) AS rn
  FROM keepcoding.ivr_detail
),

client_identification_phone AS (
    SELECT 
        calls_ivr_id,
        customer_phone,
        ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS INT64) ORDER BY customer_phone) AS rn
    FROM keepcoding.ivr_detail
),

client_identification_billing AS (
    SELECT 
        calls_ivr_id,
        billing_account_id,
        ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS INT64) ORDER BY billing_account_id) AS rn
    FROM keepcoding.ivr_detail
),

client_masiva_ig AS (
    SELECT 
        calls_ivr_id,
        module_name,
        CASE 
            WHEN module_name = 'AVERIA_MASIVA' THEN 1 ELSE 0
        END AS masiva_lg,
        ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS INT64) ORDER BY calls_ivr_id) AS rn
    FROM keepcoding.ivr_detail
),

client_info_phone AS (
    SELECT 
        calls_ivr_id, 
        step_name,
        customer_phone,
        CASE
            WHEN step_name = 'CUSTOMERINFOBYPHONE.TX' AND UPPER(step_result) = 'OK' THEN 1 ELSE 0
        END AS info_by_phone_lg,
        ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS INT64) ORDER BY calls_ivr_id) AS rn
    FROM keepcoding.ivr_detail
),

client_detail_info AS (
    SELECT 
        calls_ivr_id, 
        step_name,
        document_type,
        document_identification,
        CASE
            WHEN step_name = 'CUSTOMERINFOBYDNI.TX' AND UPPER(step_result) = 'OK' THEN 1 ELSE 0
        END AS info_by_dni_lg,
        ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS INT64) ORDER BY calls_ivr_id) AS rn
    FROM keepcoding.ivr_detail
),

repeated_phone_call AS (
    WITH info_24hrs_calls AS (
        SELECT 
            calls_ivr_id, 
            calls_phone_number, 
            calls_start_date,
            LAG(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date) AS previous_date,
            LEAD(calls_start_date) OVER (PARTITION BY calls_phone_number ORDER BY calls_start_date) AS next_date
        FROM keepcoding.ivr_detail
        WHERE calls_phone_number != 'UNKNOWN'
        GROUP BY calls_ivr_id, calls_phone_number, calls_start_date
    )
    SELECT 
        calls_ivr_id,
        CASE 
            WHEN previous_date IS NOT NULL AND DATETIME_DIFF(calls_start_date, previous_date, HOUR) <= 24 
            THEN 1 ELSE 0 
        END AS previous_call_within_24hrs,
        CASE 
            WHEN next_date IS NOT NULL AND DATETIME_DIFF(next_date, calls_start_date, HOUR) <= 24 
            THEN 1 ELSE 0 
        END AS next_call_within_24hrs
    FROM info_24hrs_calls
)

SELECT
    ivr_detail.calls_ivr_id AS ivr_id,
    ivr_detail.calls_phone_number AS phone_number,
    ivr_detail.calls_ivr_result AS ivr_result,
    detail_vdn_aggregation.vdn_aggregation AS vdn_aggregation,
    ivr_detail.calls_start_date AS start_date,
    ivr_detail.calls_end_date AS end_date,
    ivr_detail.calls_total_duration AS total_duration,
    ivr_detail.calls_customer_segment AS customer_segment,
    ivr_detail.calls_ivr_language AS ivr_language,
    ivr_detail.calls_steps_module AS steps_module,
    ivr_detail.calls_module_aggregation AS module_aggregation,
    clients_ivr_id.document_type AS document_type,
    clients_ivr_id.document_identification AS document_identification,
    client_identification_phone.customer_phone AS customer_phone,
    client_identification_billing.billing_account_id AS billing_account_id,
    client_masiva_ig.masiva_lg AS masiva_lg,
    client_info_phone.info_by_phone_lg AS info_by_phone_lg,
    client_detail_info.info_by_dni_lg AS info_by_dni_lg,
    repeated_phone_call.previous_call_within_24hrs AS previous_call_within_24hrs,
    repeated_phone_call.next_call_within_24hrs AS next_call_within_24hrs
FROM keepcoding.ivr_detail AS ivr_detail
LEFT JOIN detail_vdn_aggregation ON ivr_detail.calls_ivr_id = detail_vdn_aggregation.calls_ivr_id
LEFT JOIN clients_ivr_id ON ivr_detail.calls_ivr_id = clients_ivr_id.calls_ivr_id
LEFT JOIN client_identification_phone ON ivr_detail.calls_ivr_id = client_identification_phone.calls_ivr_id
LEFT JOIN client_identification_billing ON ivr_detail.calls_ivr_id = client_identification_billing.calls_ivr_id
LEFT JOIN client_masiva_ig ON ivr_detail.calls_ivr_id = client_masiva_ig.calls_ivr_id
LEFT JOIN client_info_phone ON ivr_detail.calls_ivr_id = client_info_phone.calls_ivr_id
LEFT JOIN client_detail_info ON ivr_detail.calls_ivr_id = client_detail_info.calls_ivr_id
LEFT JOIN repeated_phone_call ON ivr_detail.calls_ivr_id = repeated_phone_call.calls_ivr_id
GROUP BY 
    ivr_detail.calls_ivr_id,
    ivr_detail.calls_phone_number,
    ivr_detail.calls_ivr_result,
    detail_vdn_aggregation.vdn_aggregation,
    ivr_detail.calls_start_date,
    ivr_detail.calls_end_date, 
    ivr_detail.calls_total_duration,
    ivr_detail.calls_customer_segment,
    ivr_detail.calls_ivr_language,
    ivr_detail.calls_steps_module,
    ivr_detail.calls_module_aggregation,
    clients_ivr_id.document_type,
    clients_ivr_id.document_identification,
    client_identification_phone.customer_phone,
    client_identification_billing.billing_account_id,
    client_masiva_ig.masiva_lg,
    client_info_phone.info_by_phone_lg,
    client_detail_info.info_by_dni_lg,
    repeated_phone_call.previous_call_within_24hrs,
    repeated_phone_call.next_call_within_24hrs
ORDER BY ivr_detail.calls_ivr_id;
