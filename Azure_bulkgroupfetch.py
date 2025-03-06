import requests
import json
import os
import subprocess

# === Azure AD Credentials ===
CLIENT_ID = ""
CLIENT_SECRET = ""
TENANT_ID = ""

# === Authentication Config ===
GRANT_TYPE = 'client_credentials'
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPE = "https://graph.microsoft.com/.default"

# === JSON File Path ===
JSON_FILE = "azure_groups_devices_audit.json"

# === Function to Get Access Token ===
def get_access_token():
    payload = {
        "grant_type": GRANT_TYPE,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(TOKEN_URL, headers=headers, data=payload)
    return response.json().get('access_token')

# === Function to Fetch All Groups ===
def get_groups(access_token):
    url = "https://graph.microsoft.com/v1.0/groups"
    headers = {'Authorization': f"Bearer {access_token}"}
    groups = []

    while url:
        response = requests.get(url, headers=headers)
        data = response.json()
        groups.extend(data.get("value", []))
        url = data.get("@odata.nextLink", None)  # Handle pagination

    return groups

# === Function to Fetch Devices for a Specific Group ===
def get_group_devices(group_id, access_token):
    url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members"
    headers = {'Authorization': f"Bearer {access_token}"}
    devices = []

    while url:
        response = requests.get(url, headers=headers)
        data = response.json()

        for member in data.get("value", []):
            if "@odata.type" in member and "device" in member["@odata.type"].lower():
                devices.append({
                    "device_id": member.get("id"),
                    "device_name": member.get("displayName", "Unknown")
                })

        url = data.get("@odata.nextLink", None)  # Handle pagination

    return devices

# === Save Data to JSON File ===
def save_data(data):
    with open(JSON_FILE, "w") as json_file:
        json.dump(data, json_file, indent=4)

# === Fetch and Save Initial Data ===
def fetch_bulk_data():
    access_token = get_access_token()
    if not access_token:
        print("Failed to retrieve access token.")
        return

    print("Fetching bulk data...")

    # Step 1: Get all groups
    groups = get_groups(access_token)
    group_data = []

    for group in groups:
        group_id = group["id"]
        devices = get_group_devices(group_id, access_token)  # Fetch associated devices

        group_data.append({
            "group_id": group_id,
            "group_name": group.get("displayName", "Unknown"),
            "total_devices": len(devices),
            "devices": devices
        })

    save_data(group_data)
    print("Initial data fetched and saved.")

if __name__ == "__main__":
    fetch_bulk_data()