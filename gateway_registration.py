# gateway_registration.py
import hashlib
import requests

# Replace with your API server's IP or hostname
API_BASE_URL = "http://192.168.43.56:8000/api/v1"

def register_gateway():
    print("=== Gateway Registration ===")
    gateway_name = input("Enter gateway name: ")
    registration_token = input("Enter registration token provided by admin: ")

    # Build the payload as required by your /gateways/register endpoint.
    # Our schema expects { "name": <gateway_name>, "token": <registration_token> }
    payload = {
        "name": gateway_name,
        "token": registration_token
    }
    
    try:
        response = requests.post(f"{API_BASE_URL}/gateways/register", json=payload)
        if response.status_code == 200:
            data = response.json()
            print("Gateway registration successful!")
            print("Gateway details:")
            print(f"Name: {data.get('name')}")
            print(f"Status: {data.get('status')}")
            print(f"Last Seen: {data.get('last_seen')}")
        else:
            print(f"Registration failed: {response.status_code} {response.text}")
    except Exception as e:
        print(f"Error during registration: {str(e)}")

if __name__ == "__main__":
    register_gateway()

