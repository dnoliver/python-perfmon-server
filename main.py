"""
Metrics Collection Server
"""

import sqlite3
import time
from threading import Thread

import pythoncom
from fastapi import FastAPI
from pyperfmon import pyperfmon

# Constant to adjust collection interval
COLLECTION_INTERVAL = 2

# Constant to define database storage
DATABASE = "metrics.db"

# Connect to the database
conn = sqlite3.connect(DATABASE)
cursor = conn.cursor()

# Create the Metrics table if it doesn't exist
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS Metrics (
        timestamp INTEGER,
        name TEXT,
        value REAL
    )
    """
)
conn.commit()
cursor.close()


# Function to collect metrics
def collect_metrics():
    """
    Collection Loop
    """

    # Connect to the database
    _conn = sqlite3.connect(DATABASE)
    _cursor = _conn.cursor()

    # Initialize the COM library
    pythoncom.CoInitialize()  # pylint: disable=no-member

    # Create a Performance Monitor object
    pm = pyperfmon.pyperfmon()

    # Connect to localhost
    pm.connect("localhost")

    while True:
        # Capture Epoch
        timestamp = time.time()

        # Get the Metrics
        processor_usage = pm.getCounter(
            r"Processor Information\_Total\% Processor Time"
        )
        memory_usage = pm.getCounter(r"Memory\% Committed Bytes In Use")
        metrics = [
            (timestamp, "CPUTotalUsagePercentage", float(processor_usage[1])),
            (timestamp, "MemoryTotalUsagePercentage", float(memory_usage[1])),
        ]

        # Store the Metrics
        for metric in metrics:
            _cursor.execute(
                """
                INSERT INTO Metrics (timestamp, name, value)
                VALUES (?, ?, ?)
                """,
                metric,
            )
        _conn.commit()

        # Sleep for COLLECTION_INTERVAL seconds
        time.sleep(COLLECTION_INTERVAL)


# Create a daemon thread
daemon = Thread(target=collect_metrics, daemon=True, name="Monitor")

# Start collecting metrics
daemon.start()

# Create a FastAPI app
app = FastAPI()


# Define the root endpoint
@app.get("/")
def read_available_counters():
    """
    Root endpoint returns list of available counters
    """

    # Open database connection
    _conn = sqlite3.connect(DATABASE)
    _cursor = _conn.cursor()
    _results = None

    try:
        # Get the list of available values in the database
        _cursor.execute(
            """
            SELECT DISTINCT name
            FROM Metrics
            """
        )
        rows = _cursor.fetchall()
        _results = [row[0] for row in rows]
    except Exception as e:
        raise e
    finally:
        _cursor.close()

    return _results


# Define the counter endpoint
@app.get("/counter/{counter_name}")
def read_performance_counter(counter_name: str):
    """
    Endpoint to return the performance counter
    """

    # Open database connection
    _conn = sqlite3.connect(DATABASE)
    _cursor = _conn.cursor()
    _results = None

    try:
        # Get the list of available values in the database
        _cursor.execute(
            """
            SELECT * FROM Metrics WHERE name = :name
            """,
            {"name": counter_name},
        )
        rows = _cursor.fetchall()
        _results = [{"timestamp": row[0], "value": row[2]} for row in rows]
    except Exception as e:
        raise e
    finally:
        _cursor.close()

    return _results
