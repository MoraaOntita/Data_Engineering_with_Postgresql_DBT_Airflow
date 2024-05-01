from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def create_db_engine():
    """
    Creates and returns a SQLAlchemy engine for connecting to the database.

    Returns:
        Engine: SQLAlchemy engine object.
    """
    # Get database connection parameters from environment variables
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database_name = os.getenv('DB_NAME')

    # Construct the connection string
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

    # Create the engine
    engine = create_engine(connection_string)

    return engine

