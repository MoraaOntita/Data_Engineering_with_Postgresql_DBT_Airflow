import sys
sys.path.append('/media/moraa/New Volume/Ontita/10Academy/Cohort B/Projects/Week2/Data_Engineering_with_Postgresql_DBT_Airflow')  # Add the project root directory to the Python path

import pandas as pd
from sqlalchemy import create_engine
from scripts.logger import logging  # Import the logging module from your logger.py
from scripts.exceptions import CustomException, error_message_detail

class StagingDataLoader:
    def __init__(self, data_filepath: str, postgres_conn_id: str):
        self.data_filepath = data_filepath
        self.postgres_conn_id = postgres_conn_id

    def load_data(self):
        try:
            # Read and preprocess CSV file into dataframe
            df_track, df_trajectory = self._read_and_preprocess_csv()
            
            # Connect to PostgreSQL database
            engine = create_engine(self.postgres_conn_id)
            
            # Write dataframes to PostgreSQL tables
            df_track.to_sql('staging_track_table', engine, index=False, if_exists='replace')
            df_trajectory.to_sql('staging_trajectory_table', engine, index=False, if_exists='replace')
            
            logging.info("Data loaded into Staging PostgreSQL tables successfully.")
        except Exception as e:
            error_message = "Failed to load data into Staging PostgreSQL tables: {}".format(str(e))
            logging.error(error_message)
            raise CustomException(error_message, error_detail=sys.exc_info())

    def _read_and_preprocess_csv(self):
        # Read CSV file into list of lines
        with open(self.data_filepath, 'r') as file:
            lines = file.readlines()

        # Preprocess CSV data
        # Extract column names
        cols = lines.pop(0).strip('\n').strip().strip(';').split(';')
        track_cols = cols[:4]
        trajectory_cols = ['track_id'] + cols[4:]

        # Initialize lists for track and trajectory information
        track_info = []
        trajectory_info = []

        # Iterate over remaining lines in CSV file
        for line in lines:
            row = line.strip('\n').strip().strip(';').split(';')
            track_id = row[0]

            # Add first 4 values to track_info
            track_info.append(row[:4])

            remaining_values = row[4:]
            # Reshape list into a matrix and add track_id
            trajectory_matrix = [[track_id] + remaining_values[i:i+6] for i in range(0, len(remaining_values), 6)]
            # Add matrix rows to trajectory_info
            trajectory_info.extend(trajectory_matrix)

        # Create dataframes from track and trajectory information
        df_track = pd.DataFrame(data=track_info, columns=track_cols)
        df_trajectory = pd.DataFrame(data=trajectory_info, columns=trajectory_cols)

        return df_track, df_trajectory

def main():
    try:
        data_loader = StagingDataLoader("/path/to/your/data/file.csv", "your_postgres_connection_string")
        data_loader.load_data()
    except CustomException as ce:
        logging.error("Custom exception occurred: {}".format(ce))
    except Exception as e:
        error_message = "An unexpected error occurred: {}".format(str(e))
        logging.error(error_message)
        raise CustomException(error_message, error_detail=sys.exc_info())

if __name__ == "__main__":
    main()
