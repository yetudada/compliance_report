-- Summary Statistics Report
-- Provides comprehensive analysis and performance metrics
-- Parameters: {{ params.table_name }}

SELECT 
    COUNT(*) as total_transactions,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT bet_type) as unique_bet_types,
    ROUND(SUM(bet_amount), 2) as total_bet_volume,
    ROUND(AVG(bet_amount), 2) as avg_bet_size,
    ROUND(MIN(bet_amount), 2) as min_bet_amount,
    ROUND(MAX(bet_amount), 2) as max_bet_amount,
    ROUND(STDDEV(bet_amount), 2) as bet_volatility,
    ROUND(SUM(bet_amount) / COUNT(DISTINCT user_id), 2) as avg_volume_per_user
FROM {{ params.table_name }};