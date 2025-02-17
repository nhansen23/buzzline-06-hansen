"""
sqlite_producer.py

Insert records into a sqlite database.

Example record:
"grade": "6",
"subject": "reading",
"test_date": "2024-08-29",
"score": 152,
"student_id": 1584

Environment variables are in utils/utils_config module. 
"""

import random
import sqlite3
from datetime import datetime, timedelta

def random_weekday(start_date, end_date):
    date_diff = (end_date - start_date).days
    while True:
        random_days = random.randint(0, date_diff)
        test_date = start_date + timedelta(days=random_days)
        if test_date.weekday() < 5:
            return test_date

def generate_messages(conn):
    """
    Generate a stream of JSON messages and insert into SQLite3 database.
    """
    GRADE = ['3', '4', '5', '6', '7', '8']
    SUBJECT = ["Math", "Reading", "Science"]

    # Define the date range
    start_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-12-31', '%Y-%m-%d')

    while True:
        grade = random.choice(GRADE)
        subject = random.choice(SUBJECT)
        test_date = random_weekday(start_date, end_date).strftime("%Y-%m-%d %H:%M:%S")
        score = random.randint(0, 100)
        student_id = random.randint(1000, 9999)

        # Create JSON message
        json_message = {
            "grade": grade,
            "subject": subject,
            "test_date": test_date,
            "score": score,
            "student": student_id,
        }

        # Insert the message into the database
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO messages (grade, subject, test_date, score, student_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (grade, subject, test_date, score, student_id))
        conn.commit()

        yield json_message

# Open a connection to the SQLite3 database
conn = sqlite3.connect('messages.db')

# Generate messages and insert into the database
message_generator = generate_messages(conn)

# Generate a few messages (for example, 10 messages)
for _ in range(10):
    print(next(message_generator))

# Close the database connection
conn.close()
