import sqlite3
import time
import os
from datetime import datetime

# Path to SQLite database file
DB_FILE = "plc_operating_states.db"

def query_latest_state():
    """
    Query the latest operating state from the SQLite database.
    
    Returns:
        tuple or None: (timestamp, ip_address, port, operating_state) or None if no records or error.
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT timestamp, ip_address, port, operating_state "
                "FROM operating_states ORDER BY timestamp DESC LIMIT 1"
            )
            row = cursor.fetchone()
            return row if row else None
    except sqlite3.Error as e:
        print(f"Error querying database: {e}")
        return None

def get_record_count():
    """
    Get the total number of records in the operating_states table.
    
    Returns:
        int: Number of records, or -1 if an error occurs.
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM operating_states")
            count = cursor.fetchone()[0]
            return count
    except sqlite3.Error as e:
        print(f"Error getting record count: {e}")
        return -1

def clear_console():
    """
    Clear the console (cross-platform).
    """
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    """
    Monitor the SQLite database and display a live preview of the PLC operating state.
    """
    # Check if the database file exists
    if not os.path.exists(DB_FILE):
        print(f"Error: Database file not found at {DB_FILE}")
        return

    print("Starting live preview of PLC operating states. Press Ctrl+C to stop.")
    last_record_count = get_record_count()
    last_state = None

    while True:
        try:
            # Get the current record count
            current_record_count = get_record_count()

            # Check if new records have been added
            if current_record_count > last_record_count and current_record_count > 0:
                last_record_count = current_record_count

                # Get the latest operating state
                current_state = query_latest_state()
                if current_state and (
                    last_state is None or
                    current_state[0] != last_state[0] or
                    current_state[3] != last_state[3]
                ):
                    # Clear console and display the new state
                    clear_console()
                    print("PLC Operating State Monitor")
                    print("------------------------")
                    print(f"Timestamp:      {current_state[0]}")
                    print(f"IP Address:     {current_state[1]}")
                    print(f"Port:          {current_state[2]}")
                    print(f"Operating State: {current_state[3]}")
                    print("------------------------")
                    last_state = current_state

            # Sleep briefly to avoid overloading the CPU
            time.sleep(0.5)  # 500ms polling interval

        except KeyboardInterrupt:
            print("\nLive preview stopped.")
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(1)  # Wait before retrying

if __name__ == "__main__":
    main()
