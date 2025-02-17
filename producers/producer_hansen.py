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
from datetime import datetime

# import external modules
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger