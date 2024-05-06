CREATE TABLE IF NOT EXISTS log (
    log_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    execution_date DATE NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    state VARCHAR(50),
    log_message TEXT
);
