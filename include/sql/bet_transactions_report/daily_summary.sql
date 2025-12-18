-- Transaction Summary Report
-- Generates comprehensive metrics for bet transactions
-- Parameters: {{ params.table_name }}

SELECT 
    COUNT(*) as total_transactions,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(bet_amount) as total_bet_amount,
    AVG(bet_amount) as avg_bet_amount,
    MIN(bet_amount) as min_bet_amount,
    MAX(bet_amount) as max_bet_amount
FROM {{ params.table_name }};