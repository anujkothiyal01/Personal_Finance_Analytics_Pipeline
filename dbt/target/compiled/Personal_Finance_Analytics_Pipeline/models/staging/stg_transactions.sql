

SELECT
    txn_id,
    txn_date,
    updated_at,
    lower(trim(txn_type)) AS txn_type,
    amount,
    lower(trim(category)) AS category,
    lower(trim(subcategory)) AS subcategory,
    trim(merchant) AS merchant,
    lower(trim(payment_mode)) as payment_mode,
    is_recurring,
    trim(record_status) AS record_status,
    ingested_at
FROM Personal_Finance_Analytics_Pipeline.bronze.transactions_raw
WHERE trim(record_status) = 'Active'