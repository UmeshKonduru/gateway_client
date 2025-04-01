import os
import sqlite3
import json
from datetime import datetime
import requests
import serial.tools.list_ports

# Configuration
SERVER_URL = "http://192.168.43.56:8000"  # Update with your API server address
API_PREFIX = "/api/v1"
DEVICE_ENDPOINT = f"{SERVER_URL}{API_PREFIX}/devices"
GATEWAY_ID = 1  # The gateway's unique ID
DB_PATH = "gateway_devices.db"  # SQLite database file

def get_device_port(device_id: int) -> str:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT port FROM devices WHERE device_id = ?", (device_id,))
        result = cur.fetchone()
        return result[0] if result else None

def initialize_database():
    """Create database and table if they don't exist"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            port TEXT NOT NULL,
            description TEXT,
            gateway_id INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()

def discover_connected_devices():
    """Discover connected serial devices using pyserial"""
    ports = serial.tools.list_ports.comports()
    return [
        {
            "port": port.device,
            "description": port.description,
            "hwid": port.hwid
        } for port in ports
    ]

def select_device(devices):
    """Display devices and prompt user selection"""
    if not devices:
        print("No connected devices found.")
        exit(1)
        
    print("Available Devices:")
    for idx, dev in enumerate(devices, start=1):
        print(f"{idx}. {dev['port']} - {dev['description']}")
    
    try:
        index = int(input("Select a device by number: ")) - 1
        return devices[index]
    except (ValueError, IndexError):
        print("Invalid selection. Exiting.")
        exit(1)

def add_device_to_server(device_name):
    """Register device with the central server"""
    payload = {
        "name": device_name,
        "gateway_id": GATEWAY_ID
    }
    try:
        response = requests.post(DEVICE_ENDPOINT, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Server error: {str(e)}")
        exit(1)

def store_device_mapping(server_data, port_info):
    """Store device-port mapping in SQLite database"""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
            INSERT INTO devices (device_id, name, port, description, gateway_id)
            VALUES (?, ?, ?, ?, ?)
            """, (
                server_data['id'],
                server_data['name'],
                port_info['port'],
                port_info['description'],
                GATEWAY_ID
            ))
            conn.commit()
        print(f"Device {server_data['name']} mapped to {port_info['port']}")
    except sqlite3.IntegrityError:
        print(f"Device {server_data['name']} already exists in database")
    except sqlite3.Error as e:
        print(f"Database error: {str(e)}")
        exit(1)

def main():
    initialize_database()
    
    # Device discovery and selection
    devices = discover_connected_devices()
    selected = select_device(devices)
    print(f"Selected: {selected['port']} ({selected['description']})")
    
    # Get device name from user
    name = input("Enter device name: ").strip()
    if not name:
        print("Device name required. Exiting.")
        exit(1)
    
    # Server registration
    server_response = add_device_to_server(name)
    
    # Local storage
    store_device_mapping(server_response, selected)
    
    print(f"Device {name} successfully registered and stored")

if __name__ == "__main__":
    main()
