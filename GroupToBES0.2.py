import logging
import requests
import json
import os

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

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        logging.warning("Error:", response.status_code, response.text)
        exit()
        
            # === Function to Fetch all Group identities  ===
def get_groups(access_token):
    url = "https://graph.microsoft.com/v1.0/groups?$select=id,displayName"
    headers = {'Authorization': f"Bearer {access_token}"}
    groups = []

    while url:
        response = requests.get(url, headers=headers)
        data = response.json()
        groups.extend(data.get("value", []))
        url = data.get("@odata.nextLink", None)  # Handle pagination

    return groups


# === MAIN ===

# ===  setup logging first thing
script_name = os.path.basename(__file__)
logfile = f"{script_name}.log"
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(logfile)])    

# === get config
with open("config.json", "r") as config_file:
    config = json.load(config_file)

if config.get("LOGLEVEL",None) == 'INFO' :
    logging.getLogger().setLevel(logging.INFO)

# === Authentication Config ===
CLIENT_ID = config["CLIENT_ID"]
CLIENT_SECRET = config["CLIENT_SECRET"]
TENANT_ID = config["TENANT_ID"]
GRANT_TYPE = config["GRANT_TYPE"]
CLIENT_ID = config["CLIENT_ID"]
SCOPE = config["SCOPE"]
INTERESTING_AZURE_GROUPS = config["INTERESTING_AZURE_GROUPS"]
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

BigFixAutoGroupPrefix = config["BigFixAutoGroupPrefix"]
BigFixGroupSettingName = config["BigFixGroupSettingName"]
BigFixOwnerGroupSettingName = config["BigFixOwnerGroupSettingName"]

access_token = get_access_token()
groups = get_groups(access_token)
for ig in INTERESTING_AZURE_GROUPS:
    print (ig.get('id'))
    for group in groups:
        group_id = f"{group.get('id')}"
        logging.debug(group.get('id'))
        if ig.get('id') == group_id:
            group_name = f"{BigFixAutoGroupPrefix}{group.get('displayName')}"[:100]
            file_name = f"{group_name}.bes"
            logging.info (f"   writing'{file_name}' '{group_name}' '{group_id}'")
            my_xml=f"""<?xml version="1.0" encoding="UTF-8"?>
        <BES xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="BES.xsd">
                <ComputerGroup>
                        <Title>{group_name}</Title>
                        <Domain>BESC</Domain>
                        <JoinByIntersection>true</JoinByIntersection>
                        <SearchComponentRelevance Comparison="IsTrue">
                                <Relevance>exists settings ("{BigFixGroupSettingName}";"{BigFixOwnerGroupSettingName}") whose (value of it as string contains "{group_id}") of client</Relevance>
                        </SearchComponentRelevance>
                </ComputerGroup>
        </BES>
        """

            logging.debug(my_xml)
                
            bes = open(file_name, "w")
            bes.write (my_xml)
            bes.close()
logging.info("------- bye ----------")    
