-- Top Betting Patterns Report
-- Analyzes betting behavior by bet type and performance metrics
-- Parameters: {{ params.table_name }}

SELECT 
    bet_type,
    COUNT(*) as bet_count,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(bet_amount) as total_amount,
    AVG(bet_amount) as avg_amount
FROM {{ params.table_name }}
GROUP BY bet_type
HAVING COUNT(*) >= 10  -- Only include bet types with significant volume
ORDER BY total_amount DESC
LIMIT 20;