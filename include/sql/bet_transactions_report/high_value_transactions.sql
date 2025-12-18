-- High-Value Transactions Query
-- Identifies unusually high-value transactions for alerting
-- Parameters: {{ params.table_name }}, {{ params.threshold }}

SELECT 
    transaction_id,
    user_id,
    bet_amount,
    bet_type
FROM {{ params.table_name }}
WHERE bet_amount > {{ params.threshold }}  -- Threshold for high-value bets
ORDER BY bet_amount DESC;