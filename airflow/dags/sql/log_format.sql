SELECT 
    log_id, 
    dag_id, 
    task_id, 
    execution_date, 
    start_date, 
    end_date, 
    state, 
    log_message
FROM 
    log
ORDER BY 
    execution_date DESC;