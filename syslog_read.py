import socket
import logging
import re
import sqlite3
from datetime import datetime

# Configure logging to display received syslog messages and state changes
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# SQLite database setup
DB_FILE = "plc_operating_states.db"

def init_database():
    """
    Initialize the SQLite database and create the operating_states table if it doesn't exist.
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS operating_states (
                    timestamp TEXT NOT NULL,
                    ip_address TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    operating_state TEXT NOT NULL
                )
            """)
            conn.commit()
            logging.info(f"SQLite database initialized: {DB_FILE}")
    except sqlite3.Error as e:
        logging.error(f"Failed to initialize SQLite database: {e}")

def log_to_database(ip_address, port, operating_state):
    """
    Log the operating state to the SQLite database with a timestamp.
    
    Args:
        ip_address (str): The IP address of the PLC.
        port (int): The port from which the message was received.
        operating_state (str): The new operating state (e.g., 'RUN', 'STOP', 'STARTUP').
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute(
                """
                INSERT INTO operating_states (timestamp, ip_address, port, operating_state)
                VALUES (?, ?, ?, ?)
                """,
                (timestamp, ip_address, port, operating_state)
            )
            conn.commit()
            logging.info(f"Logged to database: {timestamp}, {ip_address}:{port}, {operating_state}")
    except sqlite3.Error as e:
        logging.error(f"Failed to log to database: {e}")

def parse_operating_state(message):
    """
    Parse the syslog message to extract the new operating state.
    
    Args:
        message (str): The raw syslog message.
        
    Returns:
        str or None: The new operating state (e.g., 'RUN', 'STOP', 'STARTUP') or None if not found.
    """
    # Regular expression to match the newState field
    state_match = re.search(r'newState="(\w+)"', message)
    if state_match:
        return state_match.group(1)
    return None

def start_syslog_server(host='0.0.0.0', port=10514):
    """
    Start a UDP syslog server to receive and process Siemens PLC syslog messages,
    extracting and logging the current operating state to an SQLite database.
    
    Args:
        host (str): The host IP to bind the server (default: '0.0.0.0' for all interfaces)
        port (int): The port to listen on (default: 10514 to avoid root privileges)
    """
    # Initialize the database
    init_database()

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # Bind the socket to the host and port
    try:
        sock.bind((host, port))
        logging.info(f"Syslog server started on {host}:{port}")
    except PermissionError:
        logging.error("Permission denied: Run the script with sudo or as root to bind to port 514")
        return
    except Exception as e:
        logging.error(f"Failed to start syslog server: {e}")
        return

    current_state = "UNKNOWN"  # Track the current operating state
    relevant_messages = ["SE_OPMOD_CHANGED"]  # Filter for relevant messages

    # Main loop to receive syslog messages
    while True:
        try:
            # Receive data (buffer size of 65535 bytes)
            data, addr = sock.recvfrom(65535)
            # Decode the message (assuming UTF-8, fallback to ignoring errors)
            message = data.decode('utf-8', errors='ignore')
            
            # Check if the message is relevant (contains SE_OPMOD_CHANGED)
            if any(msg in message for msg in relevant_messages):
                # Extract the new operating state
                new_state = parse_operating_state(message)
                if new_state:
                    current_state = new_state
                    # Print the current operating state
                    logging.info(f"PLC Operating State: {current_state} (from {addr[0]}:{addr[1]})")
                    # Log to SQLite database
                    log_to_database(addr[0], addr[1], current_state)
                else:
                    logging.info(f"Received relevant message but no state found: {message.strip()}")
            else:
                # Optionally log ignored messages for debugging
                logging.debug(f"Ignored message: {message.strip()}")

        except KeyboardInterrupt:
            logging.info("Syslog server stopped")
            break
        except Exception as e:
            logging.error(f"Error processing message: {e}")
    
    # Close the socket
    sock.close()

if __name__ == "__main__":
    # Start the syslog server
    start_syslog_server(port=10514)  # Use port 10514 to avoid needing root privileges