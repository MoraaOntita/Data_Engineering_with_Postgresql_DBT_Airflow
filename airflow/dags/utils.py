# utils.py

import psycopg2
from config import postgres_conn

def load_data_to_postgres(csv_file_path, track_table, trajectory_table):
    """
    Function to load data from a CSV file into PostgreSQL tables.
    
    Args:
    - csv_file_path (str): Path to the CSV file.
    - track_table (str): Name of the track table in the database.
    - trajectory_table (str): Name of the trajectory table in the database.
    """
    try:
        # Establish connection to PostgreSQL
        conn = psycopg2.connect(postgres_conn)
        cursor = conn.cursor()

        # Load data into PostgreSQL tables
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(f"COPY {track_table} FROM STDIN CSV HEADER", f)
            cursor.copy_expert(f"COPY {trajectory_table} FROM STDIN CSV HEADER", f)

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print("Data loaded successfully.")
    except Exception as e:
        print("Error:", e)

