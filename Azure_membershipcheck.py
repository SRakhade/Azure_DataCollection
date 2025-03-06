import requests
import json
import time
import os
import re
from datetime import datetime, timedelta, timezone

# === Azure AD Credentials ===
CLIENT_ID = ""
CLIENT_SECRET = ""
TENANT_ID = ""

# === Authentication Config ===
GRANT_TYPE = 'client_credentials'
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPE = "https://graph.microsoft.com/.default"

# === Get Access Token ===
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

access_token = get_access_token()

if not access_token:
    print("Failed to retrieve access token.")
    exit()

# === Headers for Graph API Requests ===
headers = {'Authorization': f"Bearer {access_token}"}

# === JSON File Path ===
JSON_FILE = "azure_groups_devices_audit.json"

# === Function to Clean and Convert Timestamp (Trims Microseconds and 'Z') ===
def clean_timestamp(raw_time):
    """
    Cleans an Azure timestamp and converts it to YYYY-MM-DD HH:MM:SS format.
    - Removes extra microseconds and 'Z'.
    - Ensures proper datetime conversion.
    """
    try:
        clean_time = re.sub(r"\.\d+Z", "", raw_time)  # Remove microseconds and 'Z'
        clean_time = re.sub(r"\.\d+\+\d{2}:\d{2}", "", clean_time)  # Remove timezone offset
        return datetime.strptime(clean_time, "%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        print(f"[ERROR] Failed to clean timestamp '{raw_time}': {e}")
        return None  # Return None if parsing fails

# === Function to Fetch Group Membership Changes ===
def get_group_membership_changes():
    url = "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits?$filter=category eq 'GroupManagement' " \
          "and (activityDisplayName eq 'Add member to group' or activityDisplayName eq 'Remove member from group')" \
          "&$orderby=activityDateTime desc"

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error fetching audit logs: {response.status_code} - {response.text}")
        return {}

    data = response.json()
    changes = {}

    # Set the cutoff time (2 minutes ago) and remove microseconds for accurate comparison
    cutoff_time = datetime.utcnow() - timedelta(minutes=2)
    cutoff_time = cutoff_time.replace(microsecond=0)  # Remove microseconds
    print(f"\n[DEBUG] Cutoff Time (UTC): {cutoff_time}")

    for entry in data.get("value", []):
        action = entry.get("activityDisplayName")
        raw_time = entry.get("activityDateTime")

        # Convert Azure timestamp to proper YYYY-MM-DD HH:MM:SS datetime
        event_time = clean_timestamp(raw_time)
        if event_time is None:
            print(f"[ERROR] Skipping entry due to invalid timestamp: {raw_time}")
            continue  # Skip this entry

        # Debugging timestamps before filtering
        print(f"[DEBUG] Event Time (UTC): {event_time} | Comparing with Cutoff Time: {cutoff_time}")

        # Compare full date and time without microseconds
        if event_time < cutoff_time:
            print(f"[SKIPPED] Event is older than cutoff time: {event_time} < {cutoff_time}")
            continue
        else:
            print(f"[PROCESSED] Event is within time range: {event_time} >= {cutoff_time}")

        # Extract Group ID from modifiedProperties
        group_id = None
        for resource in entry.get("targetResources", []):
            if resource["type"] == "Device":
                for prop in resource.get("modifiedProperties", []):
                    if prop["displayName"] == "Group.ObjectID":
                        if action == "Add member to group":
                            group_id = prop.get("newValue", "").strip('"')  # Use newValue
                        elif action == "Remove member from group" and not prop.get("newValue"):
                            group_id = prop.get("oldValue", "").strip('"')  # Use oldValue if newValue is null
                        break  # Stop after finding the first valid Group ID

        if not group_id:
            print("[SKIPPED] No valid group_id found in event.")
            continue  # Skip if no valid group_id is found

        # Initialize change tracking for the group
        if group_id not in changes:
            changes[group_id] = {"added": {}, "removed": {}}

        # Extract devices from targetResources
        for resource in entry.get("targetResources", []):
            if resource["type"] == "Device":
                device_id = resource.get("id", "Unknown")
                device_name = resource.get("displayName", "Unknown")

                # Store only the latest action for each device
                if action == "Add member to group":
                    changes[group_id]["added"][device_id] = device_name
                elif action == "Remove member from group":
                    changes[group_id]["removed"][device_id] = device_name

    return changes

# === Load Existing JSON Data ===
def load_existing_data():
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as file:
            return json.load(file)
    return []

# === Save Data to JSON File ===
def save_data(data):
    with open(JSON_FILE, "w") as json_file:
        json.dump(data, json_file, indent=4)

# === Continuous Monitoring Loop ===
while True:
    print("\nChecking for updates...")

    # Load existing data
    existing_data = load_existing_data()

    # Get membership changes
    membership_changes = get_group_membership_changes()

    # Apply Changes
    for group in existing_data:
        group_id = group["group_id"]

        # Get current device list for the group
        current_devices = {d["device_id"]: d["device_name"] for d in group["devices"]}

        # Get Added Devices
        added_devices = membership_changes.get(group_id, {}).get("added", {})
        for device_id, device_name in added_devices.items():
            if device_id not in current_devices:  # Only add if not already in JSON
                print(f"[+] Adding device: {device_name} ({device_id}) to group: {group['group_name']} ({group_id})")
                group["devices"].append({"device_id": device_id, "device_name": device_name})

        # Get Removed Devices
        removed_devices = membership_changes.get(group_id, {}).get("removed", {})
        for device_id, device_name in removed_devices.items():
            if device_id in current_devices:  # Only remove if present in JSON
                print(f"[-] Removing device: {device_name} ({device_id}) from group: {group['group_name']} ({group_id})")
                group["devices"] = [d for d in group["devices"] if d["device_id"] != device_id]

        # Update total count
        group["total_devices"] = len(group["devices"])

    # Save updated data
    save_data(existing_data)

    print("Updates applied. Sleeping for 1 minute...")
    time.sleep(60)  # Sleep for 1 minute before checking again