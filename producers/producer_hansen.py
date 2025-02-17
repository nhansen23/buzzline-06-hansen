"""
producer_hansen.py

Stream JSON data to a file.

Example JSON message
{
    "grade": "6",
    "subject": "reading",
    "test_date": "2024-08-29",
    "score": 152,
    "student_id": 1584
}

Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime, timedelta


# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger


#####################################
# Generate random test dates
#####################################

def random_weekday(start_date, end_date):
    date_diff = (end_date - start_date).days
    while True:
        random_days = random.randint(0, date_diff)
        test_date = start_date + timedelta(days=random_days)
        if test_date.weekday() < 5:
            return test_date


#####################################
# Define Message Generator
#####################################


def generate_messages():
    """
    Generate a stream of JSON messages.
    """
    GRADE = ['3','4','5','6','7','8']
    SUBJECT = ["Math", "Reading", "Science"]

    # Define the date range
    start_date = datetime.strptime('2024-08-01', '%Y-%m-%d')
    end_date = datetime.strptime('2025-01-31', '%Y-%m-%d')

    while True:
        grade = random.choice(GRADE)
        subject = random.choice(SUBJECT)
        test_date = random_weekday(start_date, end_date).strftime("%Y-%m-%d %H:%M:%S")
        score = random.randint(150, 290)  
        student_id = random.randint(1000, 2000)  # Assuming student_id is a random integer

        # Create JSON message
        json_message = {
            "grade": grade,
            "subject": subject,
            "test_date": test_date,
            "score": score,
            "student": student_id,
        }

        yield json_message
