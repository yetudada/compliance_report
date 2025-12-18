-- Data Quality Checks Report
-- Monitors data integrity and identifies potential issues
-- Parameters: {{ params.table_name }}

WITH quality_checks AS (
    SELECT 
        'Total Records' as metric,
        COUNT(*) as value,
        'Count' as unit
    FROM {{ params.table_name }}
    
    UNION ALL
    
    SELECT 
        'Records with NULL user_id' as metric,
        COUNT(*) as value,
        'Count' as unit
    FROM {{ params.table_name }}
    WHERE user_id IS NULL
    
    UNION ALL
    
    SELECT 
        'Records with NULL bet_amount' as metric,
        COUNT(*) as value,
        'Count' as unit
    FROM {{ params.table_name }}
    WHERE bet_amount IS NULL
    
    UNION ALL
    
    SELECT 
        'Records with negative bet_amount' as metric,
        COUNT(*) as value,
        'Count' as unit
    FROM {{ params.table_name }}
    WHERE bet_amount < 0
    
    UNION ALL
    
    SELECT 
        'Duplicate transactions' as metric,
        COUNT(*) - COUNT(DISTINCT transaction_id) as value,
        'Count' as unit
    FROM {{ params.table_name }}
)
SELECT 
    metric,
    value,
    unit,
    CASE 
        WHEN metric LIKE '%NULL%' AND value > 0 THEN 'WARNING'
        WHEN metric LIKE '%negative%' AND value > 0 THEN 'WARNING'  
        WHEN metric = 'Duplicate transactions' AND value > 0 THEN 'ERROR'
        ELSE 'OK'
    END as status
FROM quality_checks
ORDER BY 
    CASE status 
        WHEN 'ERROR' THEN 1 
        WHEN 'WARNING' THEN 2 
        ELSE 3 
    END,
    metric;