import pandas as pd
import sys
from sqlalchemy import create_engine
from scripts.logger import logging
from scripts.exceptions import CustomException, error_message_detail

def load_data_to_postgres(csv_file_path, postgres_conn_id, track_table_name, trajectory_table_name):
    try:
        # Read CSV file into dataframe
        df = pd.read_csv(csv_file_path)

        # Connect to PostgreSQL database
        engine = create_engine(postgres_conn_id)

        # Write dataframe to PostgreSQL tables
        df_track = df.iloc[:, :4]
        df_trajectory = df.iloc[:, 4:]
        df_track.to_sql(track_table_name, engine, index=False, if_exists='replace')
        df_trajectory.to_sql(trajectory_table_name, engine, index=False, if_exists='replace')

        logging.info(f"Data loaded into {track_table_name} and {trajectory_table_name} tables successfully.")
    except Exception as e:
        error_message = f"Failed to load data into {track_table_name} and {trajectory_table_name} tables: {str(e)}"
        logging.error(error_message)
        raise CustomException(error_message, error_detail=error_message_detail(e, sys.exc_info()))

