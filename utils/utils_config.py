"""
Config Utility
File: utils/utils_config.py

This script provides the configuration functions for the project. 

It centralizes the configuration management 
by loading environment variables from .env in the root project folder
 and constructing file paths using pathlib. 

If you rename any variables in .env, remember to:
- recopy .env to .env.example (and hide the secrets)
- update the corresponding function in this module.
"""

#####################################
# Imports
#####################################

# import from Python Standard Library
import os
import pathlib

# import from external packages
from dotenv import load_dotenv

# import from local modules
from .utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_message_interval_seconds_as_int() -> int:
    """Fetch MESSAGE_INTERVAL_SECONDS from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 5))
    logger.info(f"MESSAGE_INTERVAL_SECONDS: {interval}")
    return interval


def get_base_data_path() -> pathlib.Path:
    """Fetch BASE_DATA_DIR from environment or use default."""
    project_root = pathlib.Path(__file__).parent.parent
    data_dir = project_root / os.getenv("BASE_DATA_DIR", "data")
    logger.info(f"BASE_DATA_DIR: {data_dir}")
    return data_dir


def get_live_data_path() -> pathlib.Path:
    """Fetch LIVE_DATA_FILE_NAME from environment or use default."""
    live_data_path = get_base_data_path() / os.getenv(
        "LIVE_DATA_FILE_NAME", "score_date.json"
    )
    logger.info(f"LIVE_DATA_PATH: {live_data_path}")
    return live_data_path


def get_sqlite_path() -> pathlib.Path:
    """Fetch SQLITE_DB_FILE_NAME from environment or use default."""
    sqlite_path = get_base_data_path() / os.getenv("SQLITE_DB_FILE_NAME", "scores_db.sqlite")
    logger.info(f"SQLITE_PATH: {sqlite_path}")
    return sqlite_path


def get_database_type() -> str:
    """Fetch DATABASE_TYPE from environment or use default."""
    db_type = os.getenv("DATABASE_TYPE", "sqlite")
    logger.info(f"DATABASE_TYPE: {db_type}")
    return db_type

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    # Test the configuration functions
    logger.info("Testing configuration.")
    try:
        get_message_interval_seconds_as_int()
        get_base_data_path()
        get_live_data_path()
        get_sqlite_path()
        get_database_type()
        logger.info("SUCCESS: Configuration function tests complete.")

    except Exception as e:
        logger.error(f"ERROR: Configuration function test failed: {e}")