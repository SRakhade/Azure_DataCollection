# === Swapnil and JT in March 2025
# === thanks to Gary for the environment and JGStew for Python libraries and thoughts
# === property of HCL Software

import requests
import besapi
import json
import ast
import logging

# === Azure AD Credentials ===
CLIENT_ID = ""
CLIENT_SECRET = ""
TENANT_ID = ""
INTERESTING_AZURE_GROUPS = [{'id': '6d78a828-10ca-475b-9fb4-bd1ec36883ab'},{'id': 'aec5722f-ad84-4649-bd9a-270c495fcd25'},{'id': 'c9a0f383-3b73-46ca-a70e-244f76a7f44e'},{'id': 'cddbba6e-4b0d-4527-870a-5f3f993e1ba3'}]
LOGGING = "azure_align_1.log"

# === BigFix Operator Credentials NMO is OK but needs to manage computers that will receive settings and needs to be able to take actions and use RESTAPI
BigFixOperator = ""
BigFixPassword = ""
BigFixRootURL = "https://<SERVER>:52311"
# === BigFix objects
BigFixDeviceSettingName = "AzureDeviceID"
BigFixGroupSettingName = "AzureGroupIDs"
ActionTitlePrefix = "AzureGroupSettingAutomation"
# === BigFix cleanups
AutoDelete = True   #stops and deletes old actions
AutoStop = True     #just stops old actions (overridden by AutoDelete)

# === Authentication Config ===
GRANT_TYPE = 'client_credentials'
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPE = "https://graph.microsoft.com/.default"

# === Function to setup logging ===
def log_setup():
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

# === Fetch each Azure Groups and transform into Dictionary keyed to deviceID
def azure_to_dict (ig):
    azdd={}
    for group in ig:
        group_id = group["id"]
        devices = get_group_devices(group_id, access_token)  # Fetch the Group devices then transform them into a library keyed to devices
        for device in devices:
            logger.debug(f"device {device} to group {group_id}")
            if  device["device_id"] in azdd:
                azdd[device["device_id"]].append(group_id)
            else:
                azdd[device["device_id"]] = [group_id]
    return azdd

# === Function to Fetch Devices for a Specific Group ===
def get_group_devices(group_id, access_token):
    logger.info (f"Getting Azure Group: {group_id}")
    url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members?$select=deviceId,displayName"
    headers = {'Authorization': f"Bearer {access_token}"}
    devices = []

    while url:
        response = requests.get(url, headers=headers)
        data = response.json()

        for member in data.get("value", []):
            if "@odata.type" in member and "device" in member["@odata.type"].lower():
                devices.append({
                    "device_id": member.get("deviceId"),
                    "device_name": member.get("displayName", "Unknown")
                })

        url = data.get("@odata.nextLink", None)  # Handle pagination
    logger.debug(devices)    
    return devices

# === Fetch and Save BigFix Computer Settings Data ===
def get_BigFix_data(bf_conn):
    bf_conn = besapi.besapi.BESConnection(BigFixOperator, BigFixPassword, BigFixRootURL)
    #  doing the dictionary formatting in relevance to save steps
    session_relevance = f"""("%7b" & it & "%7d") of concatenation ", " of ("'" & item 0 of it & "': %7b'compid': '" & item 1 of it as string & "', 'compname': '" &item 2 of it& "', 'groups': ['" & concatenation "', '" of tuple string items of item 3 of it  &"']" & "%7d") of ((value of client setting whose (name of it = "{BigFixDeviceSettingName}") of it)|"NoDeviceID", id of it, name of it,  value of client setting whose (name of it = "{BigFixGroupSettingName}") of it |"NoGroups" ) of bes computers whose (exists client setting whose (name of it = "{BigFixDeviceSettingName}" and value of it != "NotAzureADJoined") of it)"""
    logger.debug (session_relevance)
    rd = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
    logger.debug (rd)
    return rd    
# === find matches or mismatches for all BigFix devices with AzureIDs reported and return the to_do list ===
def align(az_devices,bf_devices):
    for dev in  bf_devices:
        bfd = sorted(bf_devices[dev]['groups'])
        azd = az_devices.get(dev)
        if azd != None:
            azd = sorted(azd)
        #Might be tenant mismatch issue possible here.
        if bfd != azd and not(bfd == ['NoAzureGroups'] and azd == None):
            logger.debug (f"(MISMATCH) for {dev} with BigFix Comptuer ID {bf_devices[dev]['compid']}  bfd is {bfd} and azd is {azd}")
            bf_devices[dev]['state']="MismatchDetected"
            to_do.update ({bf_devices[dev]['compid']: azd})
        else:
            logger.debug (f"(MATCH) {dev} with BigFix Comptuer ID {bf_devices[dev]['compid']}  bfd is {bfd} and azd is {azd}")
    return to_do

# === send the final to_do list to tbe BigFix server by posting one large propagation with many mailbox actions in the xml.
def send_it (to_do):
    relevance = "true"
    when = '{parameter "action issue date" of action}'
    action_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <BES xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="BES.xsd">"""
    for id in to_do:
        title = f"{ActionTitlePrefix} for {id}" # we  use this title format for later deletions.
        if to_do[id] == None:
            value = "NoAzureGroups"
        else:
            value = ", ".join(to_do[id])          
        action_xml = action_xml + (f"""
            <SingleAction>
		<Title>{title}</Title>
		<Relevance><![CDATA[{relevance}]]></Relevance>
		<ActionScript MIMEType="application/x-Fixlet-Windows-Shell"><![CDATA[
		setting "{BigFixGroupSettingName}" = "{value}" on "{when}" for client
		]]></ActionScript>
		<SuccessCriteria />
		<Settings />
		<SettingsLocks />
		<Target>
                    <ComputerID>{id}</ComputerID>
                </Target>
            </SingleAction>""")
        
    action_xml = action_xml + ("""
        </BES>""")
    logger.debug (action_xml)
    rr = bf_conn.post(bf_conn.url("actions"),action_xml) #RESTAPI post action
    logger.debug (rr)
    
# === some initial connnection setups for Azure and BigFix ===
def initial():
    access_token = get_access_token()
    if not access_token:
        logger.error ("--- Failed to retrieve Azure Access Token.")
        exit ("Failed to retrieve Azure Access Token.")
    else:
        logger.info("    Got Azure Access Token.")
    bf_conn = besapi.besapi.BESConnection(BigFixOperator, BigFixPassword, BigFixRootURL)  
    if not bf_conn:
        logger.error("--- Failed to retrieve BigFix REST Connection.")
        exit ("Failed to retrieve BigFix REST Connection.")    
    else:
        logger.info("    Got BigFix REST Connection.")
    return [access_token, bf_conn]

# === check for actions that are ready to be cleaned up ===
def clean_BigFix_prior_actions(bf_conn,to_do):
    logger.info ("    Finding prior Actions")
    if not bf_conn:
        bf_conn = besapi.besapi.BESConnection(BigFixOperator, BigFixPassword, BigFixRootURL)
    logger.info ("=== Check the to_do list for any computer IDs that might have active actions already and remove them from the to_do list ===")
    logger.debug (to_do)
    keep= '("' + '"; "'.join(to_do.keys()) + '")'
    logger.info (f"keep actions with these IDs in their titles: {keep}")
    if AutoDelete:
        session_relevance = f"""("[" & it & "]") of concatenation ", " of (it as string) of ids of bes actions whose (name of issuer of it = "{BigFixOperator}" AND name of it starts with "{ActionTitlePrefix} for " AND following text of last "{ActionTitlePrefix} for " of name of it is not contained by set of {keep})"""
        logger.debug(f"session_relevance {session_relevance}")
        rd = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
        logger.debug (rd)
        if rd:
            logger.info ("    Found some actions to stop and delete")
            for action_id in rd:
                aid=str(action_id)
                logger.info("*** Stopping and Deleting Action: " + aid)
                action_delete_result = bf_conn.delete("action/" + aid)
                logger.info(action_delete_result)
        else:
            logger.info ("    No Prior Actions found to stop and delete")
    else:
        if AutoStop:
            logger.info ("   Finding actions to stop")
            session_relevance = f"""("[" & it & "]") of concatenation ", " of (it as string) of ids of bes actions whose (name of issuer of it = "{BigFixOperator}" AND name of it starts with "{ActionTitlePrefix} for " AND following text of last "{ActionTitlePrefix} for " of name of it is not contained by set of {keep}) AND state of it = "Open" )"""
            logger.debug(f"session_relevance {session_relevance}")
            rd = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
            logger.debug (rd)
            if rd:
                for action_id in rd:
                    aid=str(action_id)
                    logger.info("*** Stopping Action: " + aid)
                    action_stop_result = bf_conn.post("action/" + aid + "/stop", "")
                    logger.info(action_stop_result)
            else:
                logger.info ("   No Prior Actions found to stop, you may want to manually delete old actions, as AutoDelete is set to false")
    return

# === make sure we are not recreating existing open actions (we do recreate stopped or expired actions if they are in the to_do)===
def clean_keepers_from_to_do(to_do):
    keep= '("' + '"; "'.join(to_do.keys()) + '")'
    logger.debug (f"keep = {keep}")
    session_relevance = f"""("[" & it & "]") of concatenation ", " of ((it as trimmed string) of following text of last "{ActionTitlePrefix} for " of it as string) of names of bes actions whose (name of issuer of it = "{BigFixOperator}" AND state of it = "Open" AND name of it starts with "{ActionTitlePrefix} for " AND following text of last "{ActionTitlePrefix} for " of name of it is contained by set of {keep})"""
    logger.debug(f"session_relevance {session_relevance}")
    rd = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
    logger.debug (f"keepers = {rd}")
    for k in rd:
        logger.debug (f"removing {k} from to_do")
        to_do.pop(f"{k}", None)
    return to_do
  
# === MAIN ===
logger = log_setup()

logger.info ("================== Starting New Session =============================")
logger.info ("=== Setup connections ===")
i = initial ()
access_token = i[0]
bf_conn = i[1]
to_do={}

# === if interesting groups not set, pull all groups
if INTERESTING_AZURE_GROUPS == {}:
    ig = get_groups(access_token)
    logger.warning("--- no interesting groups: Getting All ---")
else:
    ig = INTERESTING_AZURE_GROUPS
    logger.info ("--- Groups of interest ----")
logger.debug (ig)  

logger.info ("=== Azure Device Dictionary ===")
az_devices = azure_to_dict(ig)
logger.debug (az_devices)
logger.info ("--- done with Azure Device Dictionary ---")

logger.info ("=== BigFix Device Dictionary ===")
bf_devices = get_BigFix_data(bf_conn)
logger.debug (bf_devices)
logger.info ("--- done with BigFix Device Dictionary ---")

logger.info ("=== Detect mismatches  ===")
to_do = align(az_devices,bf_devices)    
logger.debug (f"to_do is currently {to_do}")
logger.info ("--- done with mismatch detection ---")

logger.info  ("=== Prior Action Cleanups ===")
if AutoStop or AutoDelete:
    clean_BigFix_prior_actions(bf_conn,to_do)
else:
    logger.warning (f"+++ Autodelete and Autostop both False -  please clean up actions by hand in the BigFix console")
logger.info ("--- done with action cleaning ---")
       
if to_do:       #only clean if there is a to_do
    logger.debug (f"to_do prior to cleaning {to_do}")
    to_do = clean_keepers_from_to_do(to_do)
    logger.debug (f"to_do after cleaning {to_do}")
if to_do:       #only send if there is still to_do after cleaning
    logger.info ("=== Sending actions to BigFix ====") 
    send_it(to_do)
    logger.info ("------- Actions Sent ---------")
else:
    logger.info("========= No actions needed =========")

logger.info("========= initial pull done - moving to delta watching =========")
    
# ===========  still left -  once full pull is run, pull deltas on a chron
# ===  Pull the deltas and pull interesting groups
# === for small deltas, pull the computer's current full groups and send a one-off
# === for large deltas, consider a full pull? or just full pull for the group? or full pulls for just the devices?
# === for large deltas consider how to minimize propagations and how to clean the actions later.
# === consider tenant aware as a next revision?
# === consider auto creating the properties / groups in the console with more rest commands?
# === look for a method to bulk delete old actions, as we are currently deleting serially
# === look for optimization for large group moves in Azure like creating a new rule based azure group and having a large amount of delta at once?
# ===     might be able to do a bulk action to the impacted endpoints and just append the new group to the setting instead of sending unique actions to all the impacted?
# === what if an endpoint is in a lot of groups -  will a client setting still hold it all? perhaps break into individual settings, or a different storage medium for the client side?

logger.info ("--- BYE ---")    
logger.debug ("================== ENDING  SESSION  =============================")
    
