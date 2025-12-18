-- Transaction Volume Analysis by User
-- Analyzes transaction volume patterns by user activity
-- Parameters: {{ params.table_name }}

SELECT 
    user_id,
    COUNT(*) as transaction_count,
    SUM(bet_amount) as total_bet_volume,
    AVG(bet_amount) as avg_bet_size,
    MIN(bet_amount) as min_bet,
    MAX(bet_amount) as max_bet
FROM {{ params.table_name }}
GROUP BY user_id
HAVING COUNT(*) >= 5  -- Only users with significant activity
ORDER BY total_bet_volume DESC
LIMIT 50;