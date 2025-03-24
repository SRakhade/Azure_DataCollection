import msal
import requests
import json
import logging
import os
import ast
import besapi

# Authentication details
client_id = ''
client_secret = ''
tenant_id = ''
scopes = ['https://graph.microsoft.com/.default']

# === BigFix Operator Credentials NMO is OK but needs to manage computers that will receive settings and needs to be able to take actions and use RESTAPI
BigFixOperator = ""
BigFixPassword = ""
BigFixRootURL = "https://SERVER:52311"

# === BigFix objects
BigFixDeviceSettingName = "AzureDeviceID"
BigFixGroupSettingName = "AzureGroupIDs"
BigFixOwnerIDSettingName = "AzureOwnerID"
BigFixOwnerNameSettingName = "AzureOwnerName"
BigFixOwnerUserPrincipalNameSettingName = "AzureOwnerUserPrincipalName"
ActionTitlePrefix = "AzureOwnerSettingAutomation"

# === BigFix cleanups
AutoDelete = True   #stops and deletes old actions
AutoStop = True     #just stops old actions (overridden by AutoDelete)

script_name = os.path.basename(__file__)
LOGGING = f"{script_name}.log"
to_do={}


# === Function to setup logging ===
def log_setup(LOGGING):
    logger = logging.getLogger('main')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fh = logging.FileHandler(LOGGING)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def access_token():
    # Authenticate and get the access token
    app = msal.ConfidentialClientApplication(
        client_id, authority=f'https://login.microsoftonline.com/{tenant_id}', client_credential=client_secret
    )
    token_response = app.acquire_token_for_client(scopes=scopes)
    access_token = token_response['access_token']
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    logger.info("Got Azure REST connection")
    return headers


def connect_BigFix():
    bf_conn = besapi.besapi.BESConnection(BigFixOperator, BigFixPassword, BigFixRootURL)  
    if not bf_conn:
        logger.error("--- Failed to retrieve BigFix REST Connection.")
        exit ("Failed to retrieve BigFix REST Connection.")    
    else:
        logger.info("Got BigFix REST Connection.")
    return bf_conn

def get_BigFix_devices(bf_conn):
    #  doing the dictionary formatting in relevance to save steps
    session_relevance = f"""("%7b" & it & "%7d") of concatenation ", " of
("'" & item 0 of it 
& "': %7b'compId': '" & item 1 of it as string 
& "', 'compname': '" &item 2 of it
& "', 'ownerId': '" &item 3 of it
&"','groups': ['" & concatenation "', '" of tuple string items of item 4 of it  &"']" & "%7d"
) of (
(value of client setting whose (name of it = "{BigFixDeviceSettingName}") of it)|"NoDeviceID"
, id of it
, name of it
, (value of client setting whose (name of it = "{BigFixOwnerIDSettingName}") of it)|"NoOwnerID"
, value of client setting whose (name of it = "{BigFixGroupSettingName}") of it |"NoGroups" 
) of bes computers whose (exists client setting whose (name of it = "{BigFixDeviceSettingName}" and value of it != "NotAzureADJoined") of it)"""
    
    logger.debug (session_relevance)
    rr = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
    logger.debug (json.dumps(rr, indent=2))
    logger.info ("Got BigFix Devices")
    return rr

def az_pull_and_queue(did,b):
    #logger.info(did)
    #logger.info(b)
    #logger.info (b.get('compId'))
    headers = access_token() 
    url=f"https://graph.microsoft.com/v1.0/devices?$filter=deviceId eq '{did}'&$select=id,displayName,deviceId&$expand=registeredOwners($select=id,displayName,userPrincipalName)"
    response = requests.get(url, headers=headers)
    logger.debug(url)
    #logger.info(response)

    if response.status_code == 200:
        data = response.json()
        logger.debug(f"device pulled: {data}")
    else:
        logger.warning("Error:", response.status_code, response.text)

    if 'value' in data:
        reg_own = data.get('value')[0].get('registeredOwners')[0]
        if reg_own.get('id'):
            reg_own_id = reg_own.get('id')
            reg_own_displayName = reg_own.get('displayName')
            reg_own_userPrincipalName = reg_own.get('userPrincipalName')
            to_do.update({did: {
                'compId': b.get('compId'),
                'reg_own_id': reg_own.get('id'),
                'reg_own_displayName': reg_own.get('displayName'),
                'reg_own_userPrincipalName': reg_own.get('userPrincipalName'),
                }})
            logger.debug(f"to_do updated to {to_do}")
    logger.info("Got Azure devices")
    return 

def clean_BigFix_actions (bf_conn):
    logger.debug("=== getting BigFix actions ===")
    session_relevance = f"""("%7b" & it as string & "%7d") of concatenation ", " of  ( ("'" & it as string & "': %7b" ) of following text of first "{ActionTitlePrefix} for " of name of it & " 'action_id': '" &  id of it as string & "'%7d" ) of bes actions whose (name of issuer of it = "{BigFixOperator}" AND name of it starts with "{ActionTitlePrefix} for ")"""
    #session_relevance = f"""("%7b" & it as string & "%7d") of names of bes actions"""
    logger.debug(f"session_relevance {session_relevance}")
    rd = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
    logger.debug (rd)
    clean_BigFix_prior_actions(rd)
    logger.info("Done cleaning old BigFix Actions")
    return


# === check for actions that are ready to be cleaned up ===
def clean_BigFix_prior_actions(bf_actions):
    logger.debug ("    Finding prior Actions")
    for computer_id in bf_actions:
        bfcid = computer_id
        bfaid = bf_actions[computer_id].get('action_id')
        logger.debug (f"{bfaid} deleting the old action")
        delete_or_stop(bfaid)
    return

def delete_or_stop(bfaid):
    if AutoDelete:
        logger.debug("*** Stopping and Deleting Action: " + bfaid)
        action_delete_result = bf_conn.delete("action/" + bfaid)
        logger.debug(f"REST delete result: {action_delete_result}")
    else:
        if AutoStop:
            logger.debug("*** Stopping Action: " + bfaid)
            action_stop_result = bf_conn.post("action/" + bfaid + "/stop", "")
            logger.debug(f"REST stop result: {action_stop_result}")
    return


# === send the final to_do list to tbe BigFix server by posting one large propagation with many mailbox actions in the xml.
def send_it (to_do):
    relevance = "true"
    when = '{parameter "action issue date" of action}'
    action_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <BES xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="BES.xsd">"""
    for id in to_do:
        title = f"{ActionTitlePrefix} for {to_do[id].get('compId')}" # we  use this title format for later deletions.
        action_xml = action_xml + (f"""
            <SingleAction>
		<Title>{title}</Title>
		<Relevance><![CDATA[{relevance}]]></Relevance>
		<ActionScript MIMEType="application/x-Fixlet-Windows-Shell"><![CDATA[setting "{BigFixOwnerIDSettingName}" = "{to_do[id].get('reg_own_id')}" on "{when}" for client
setting "{BigFixOwnerNameSettingName}" = "{to_do[id].get('reg_own_displayName')}" on "{when}" for client
setting "{BigFixOwnerUserPrincipalNameSettingName}" = "{to_do[id].get('reg_own_userPrincipalName')}" on "{when}" for client]]></ActionScript>
		<SuccessCriteria />
		<Settings />
		<SettingsLocks />
		<Target>
                    <ComputerID>{to_do[id].get('compId')}</ComputerID>
                </Target>
            </SingleAction>""")
    action_xml = action_xml + ("""
        </BES>""")
    logger.debug (action_xml)
    rr = bf_conn.post(bf_conn.url("actions"),action_xml) #RESTAPI post action
    logger.debug (rr)
    logger.info("Done sending BigFix actions")
        



        
# ====  main()
logger=log_setup(LOGGING)
if __name__ == '__main__':
    try:
        bf_conn = connect_BigFix()
        b = get_BigFix_devices(bf_conn)
        c = clean_BigFix_actions (bf_conn)
        
        for did in b:
            if b[did].get('ownerId')=="NoOwnerID":
                #logger.info(f"need to action {did}")
                az_pull_and_queue(did, b[did])
        if to_do:
            res = send_it(to_do)

        #devices_with_owners = get_devices_with_owners()
        #logger.info(json.dumps(devices_with_owners, indent=2))
    except Exception as e:
        logger.warning(f"An error occurred: {e}")

