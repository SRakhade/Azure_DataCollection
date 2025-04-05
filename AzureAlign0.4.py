# === Swapnil and JT in March 2025
# === thanks to Gary for the environment and JGStew for Python libraries and thoughts
# === property of HCL Software
# ++ April 2025 addded config file and batching
# ++ April 2025 refactored settings handling to better use batching, added UserGroup parts.

import requests
import besapi
import json
import ast
import os
import logging
import itertools

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

# === Fetch each Azure Groups and transform into Dictionary keyed to deviceID and userID
def azure_to_dict (INTERESTING_AZURE_GROUPS):
    azure_devices={}
    azure_users={}
    logging.info(f"INTERESTING_AZURE_GROUPS = {json.dumps(INTERESTING_AZURE_GROUPS, indent=2)}")
    for group in INTERESTING_AZURE_GROUPS:
        group_id = group["id"]
        devices,users = get_group_devices(group_id, access_token)  # Fetch the Group devices then transform them into a library keyed to devices
        for device in devices:
            logging.debug(f"device {device} to group {group_id}")
            if  device["device_id"] in azure_devices:
                azure_devices[device["device_id"]].append(group_id)
            else:
                azure_devices[device["device_id"]] = [group_id]
        for user in users:
            logging.debug(f"user {user} to group {group_id}")
            if  user["user_id"] in azure_users:
                azure_users[user["user_id"]].append(group_id)
            else:
                azure_users[user["user_id"]] = [group_id]                
    return azure_devices, azure_users

# === sub-Function to Fetch Devices for a Specific Group ===
def get_group_devices(group_id, access_token):
    logging.info (f"Getting Azure Group: {group_id}")
    url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members?$select=id,deviceId,displayName"
    headers = {'Authorization': f"Bearer {access_token}"}
    devices = []
    users = []

    while url:
        response = requests.get(url, headers=headers)
        data = response.json()

        for member in data.get("value", []):
            if "@odata.type" in member and "microsoft.graph.device" in member["@odata.type"].lower():
                devices.append({
                    "device_id": member.get("deviceId"),
                    "device_name": member.get("displayName", "Unknown")
                })
            if "@odata.type" in member and "microsoft.graph.user" in member["@odata.type"].lower():
                users.append({
                    "user_id": member.get("id"),
                    "user_name": member.get("displayName", "Unknown")
                })
        url = data.get("@odata.nextLink", None)  # Handle pagination
    return devices, users

# === Fetch and Save BigFix Computer Settings Data ===
def get_BigFix_data(bf_conn):
    bf_conn = besapi.besapi.BESConnection(BigFixOperator, BigFixPassword, BigFixRootURL)
    #  doing the dictionary formatting in relevance to save steps
    session_relevance = f"""("%7b" & it & "%7d") of concatenation ", " of ("'" & item 0 of it &
"': %7b'compid': '" & item 1 of it as string
& "', 'compname': '"&item 2 of it
& "', 'groups': ['" & concatenation "', '" of tuple string items of item 3 of it  &"']"
& ", 'user_groups': ['" & concatenation "', '" of tuple string items of item 4 of it  &"']"
& ", 'user_id': '" & item 5 of it
& "'%7d") of (
(value of client setting whose (name of it = "{BigFixDeviceSettingName}") of it)|"NoDeviceID"
, id of it , name of it
,  value of client setting whose (name of it = "{BigFixGroupSettingName}") of it |"NoGroups"
,  value of client setting whose (name of it = "{BigFixOwnerGroupSettingName}") of it |"NoUserGroups"
,  value of client setting whose (name of it = "{BigFixOwnerIDSettingName}") of it |"NoOwnerID" )
of bes computers whose (exists client setting whose (name of it = "{BigFixDeviceSettingName}" and value of it != "NotAzureADJoined") of it)"""
    
    logging.debug (session_relevance)
    rd = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
    logging.debug (rd)
    return rd

def az_pull_and_queue(d_id,compid):
    url=f"https://graph.microsoft.com/v1.0/devices?$filter=deviceId eq '{d_id}'&$select=id,displayName,deviceId&$expand=registeredOwners($select=id,displayName,userPrincipalName)"
    headers = {'Authorization': f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    logging.debug(url)
    logging.debug(response)

    if response.status_code == 200:
        data = response.json()
        logging.debug(f"device pulled: {data}")
    else:
        logging.warning("Error:", response.status_code, response.text)
    if 'value' in data:
        reg_own = data.get('value')[0].get('registeredOwners')[0]
        # fill the settings to update dict.
        to_do_settings.update({compid:{
                            BigFixOwnerIDSettingName:                   reg_own.get( 'id' , 'NoRegisteredOwnerFound' ),
                            BigFixOwnerNameSettingName:                 reg_own.get( 'displayName' , 'NoRegisteredOwnerFound' ),
                            BigFixOwnerUserPrincipalNameSettingName:    reg_own.get( 'userPrincipalName' , 'NoUserPrincipalNameFound' ),
        }})
        # if we are here, we are missing the ownerID from bf_devices, which we will actually need when we user group match so insert our AZ data into the BF dict.
        logging.debug(f"appending BF data with AZ data {d_id} with {reg_own.get('id' , 'NoRegisteredOwnerFound' )} for user group aggregation. it's still in to_do_settings for later")
        bf_devices[d_id].update ({'user_id': reg_own.get('id' , 'NoRegisteredOwnerFound' )})
    else:
        logging.warning("missing value with azure data for {d_id}")
    logging.debug (json.dumps(bf_devices, indent=2))
    logging.debug(f"to_do_settings currently {to_do_settings}")
    logging.info(f"Got Azure device {d_id}")
    return 

# === find matches or mismatches for all BigFix devices with AzureIDs reported and return the to_do list ===
def align(az,bf_devices):
    az_devices = az[0]
    logging.debug("++++++ device groups ++++++++++")
    logging.debug(json.dumps(az_devices, indent=2))

    az_users = az[1]
    logging.debug("++++++++ user groups ++++++++++")
    logging.debug(json.dumps(az_users, indent=2))
    logging.debug("+++++++++++++++++++++++++++++++")
    
    for dev in  bf_devices:
        # device group section
        compid = bf_devices[dev].get('compid')
        bfd = sorted(bf_devices[dev]['groups'])
        azd = az_devices.get(dev)
        if azd != None:
            azd = sorted(azd)
        #Might be tenant mismatch issue possible here.
        if bfd != azd and not(bfd == ['NoAzureGroups'] and azd == None):
            logging.debug (f"(DG MISMATCH) for {dev} with BigFix Comptuer ID {bf_devices[dev]['compid']}  bfd is {bfd} and azd is {azd}")
            to_do_settings[compid].update({BigFixGroupSettingName: azd})
        else:
            logging.debug (f"(DG MATCH) {dev} with BigFix Comptuer ID {bf_devices[dev]['compid']}  bfd is {bfd} and azd is {azd}")
        # user group section
        user = bf_devices[dev].get('user_id','NoUser')
        bfu = sorted(bf_devices[dev]['user_groups'])
        azu = az_users.get(bf_devices[dev]['user_id'],"NoUser")
        if azu != None:
            azu =sorted(azu)
        if azu != bfu and not (bfu == ['NoRegisteredOwnerFound']):
            logging.debug (f"(UG MISMATCH) for {user} with BigFix Comptuer ID {compid}  bfu is {bfu} and azu is {azu}")
            to_do_settings[compid].update({BigFixOwnerGroupSettingName: azu})
        else:
            logging.debug (f"(UG MATCH) {user} with BigFix Comptuer ID {compid}  bfu is {bfu} and azu is {azu}")            
    return to_do_settings

# === send the final to_do list to tbe BigFix server by posting one large propagation with many mailbox actions in the xml.
def batched_send_it(to_do_settings,action_batchsize):
    outer_action_xml = ["""<BES xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" SkipUI="true">"""]
    relevance = "true"
    when = '{parameter "action issue date" of action}'
    c = 0
    for batch in (itertools.batched(to_do_settings,action_batchsize)):
        c=c+1
        logging.debug(f"============ Batch {c} ")
        title = f"""{ActionTitlePrefix} for Batch {c}"""
        batch_targets = []
        action_xml=["""<ActionScript MIMEType="application/x-Fixlet-Windows-Shell"><![CDATA["""]
        for id in batch:
            batch_targets.append( f"""            <ComputerID>{id}</ComputerID>""")
            setting_xml = [f"if {{ computer id = {id} }}"]
            for key in to_do_settings.get(id):
                setting = to_do_settings[id].get(key)
                if type(setting) == list:
                    setting = ", ".join(setting)
                setting_xml.append(f"""   setting "{key}" = "{setting}"  on "{when}" for client""")
            setting_xml.append("""   exit("0")\nendif""") 
            computer_xml = ("\n   ".join(setting_xml))
            action_xml.append(computer_xml)

        action_xml.append("]]></ActionScript>")
        action_xml= "\n".join(action_xml)
        bt= "\n".join(batch_targets)
        batch_xml = f"""<SingleAction>\n    <Title>{title}</Title>\n    <Relevance><![CDATA[{relevance}]]></Relevance>\n    {action_xml}\n    <SuccessCriteria />
    <Settings>\n        <HasTimeRange>false</HasTimeRange>\n        <HasStartTime>false</HasStartTime>
        <HasEndTime>true</HasEndTime>\n        <EndDateTimeLocalOffset>P1D</EndDateTimeLocalOffset>
    </Settings>
    <SettingsLocks />\n    <Target>\n{bt}\n    </Target>\n</SingleAction>"""
        outer_action_xml.append(batch_xml)
        logging.debug(f"============ Batch {c} DONE")
    outer_action_xml.append("</BES>")
    outer_action_xml="\n".join(outer_action_xml)
    logging.debug(f"============ Full Action XML")  
    logging.debug(outer_action_xml)
    logging.debug(f"============ Full Action XML End - sending to BigFix REST")
    rr = bf_conn.post(bf_conn.url("actions"),outer_action_xml) #RESTAPI post action
    logging.debug (rr)
    return c

# === some initial connnection setups for Azure and BigFix ===
def initial():

    #input validation
    if not BigFixOperator or not BigFixPassword or not BigFixRootServerName:
        logging.warning(f"Missing BigFix operator account, password, or server name")
        exit("Missing BigFix operator account, password, or root server  fqdn")
    if not INTERESTING_AZURE_GROUPS:
        logging.warning(f"missing INTERESTING_AZURE_GROUPS")
    if len(INTERESTING_AZURE_GROUPS) > 200:
        logging.warning(f"INTERESTING_AZURE_GROUPS {INTERESTING_AZURE_GROUPS} is more than recommended 200")
    if not BigFixOperator:
        logging.warning(f"missing BigFix credentials")
    if action_batchsize > 100:
        logging.warning(f"action_batchsize of {action_batchsize} is more than recommended 100")
        
    access_token = get_access_token()
    if not access_token:
        logging.error ("--- Failed to retrieve Azure Access Token.")
        exit ("Failed to retrieve Azure Access Token.")
    else:
        logging.info("    Got Azure Access Token.")

    bf_conn = besapi.besapi.BESConnection(BigFixOperator, BigFixPassword, BigFixRootURL)  
    if not bf_conn:
        logging.error("--- Failed to retrieve BigFix REST Connection.")
        exit ("Failed to retrieve BigFix REST Connection.")    
    else:
        logging.info("    Got BigFix REST Connection.")
    return [access_token, bf_conn]

# === check for actions that are ready to be cleaned up ===
def clean_BigFix_prior_actions(bf_conn):
    logging.info ("    Finding prior Actions")
    if not bf_conn:
        bf_conn = besapi.besapi.BESConnection(BigFixOperator, BigFixPassword, BigFixRootURL)
    if AutoDelete:
        session_relevance = f"""("[" & it & "]") of concatenation ", " of (it as string) of ids of bes actions whose (name of issuer of it = "{BigFixOperator}" AND name of it starts with "{ActionTitlePrefix} for ")"""
        logging.debug(f"session_relevance {session_relevance}")
        rd = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
        logging.debug (rd)
        if rd:
            logging.debug ("    Found some actions to stop and delete")
            for action_id in rd:
                aid=str(action_id)
                logging.info("*** Stopping and Deleting Action: " + aid)
                action_delete_result = bf_conn.delete("action/" + aid)
                logging.debug(action_delete_result)
        else:
            logging.info ("    No Prior Actions found to stop and delete")
    else:
        if AutoStop:
            logging.debug ("   Finding actions to stop")
            session_relevance = f"""("[" & it & "]") of concatenation ", " of (it as string) of ids of bes actions whose (name of issuer of it = "{BigFixOperator}" AND name of it starts with "{ActionTitlePrefix} for " ) AND state of it = "Open" )"""
            logging.debug(f"session_relevance {session_relevance}")
            rd = ast.literal_eval(bf_conn.session_relevance_string(session_relevance))
            logging.debug (rd)
            if rd:
                for action_id in rd:
                    aid=str(action_id)
                    logging.info("*** Stopping Action: " + aid)
                    action_stop_result = bf_conn.post("action/" + aid + "/stop", "")
                    logging.debug(action_stop_result)
            else:
                logging.info ("   No Prior Actions found to stop, you may want to manually delete old actions, as AutoDelete is set to false")
    return

  
# === MAIN ===
# ===  setup logging first thing
script_name = os.path.basename(__file__)
logfile = f"{script_name}.log"
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(logfile)])

with open("config.json", "r") as config_file:
    config = json.load(config_file)

if config.get("LOGLEVEL",None) == 'INFO' :
    logging.getLogger().setLevel(logging.INFO)    
    
# === for Global Scope
# === Azure AD Credentials ===

CLIENT_ID = config["CLIENT_ID"]
CLIENT_SECRET = config["CLIENT_SECRET"]
TENANT_ID = config["TENANT_ID"]
INTERESTING_AZURE_GROUPS = config["INTERESTING_AZURE_GROUPS"]

# === Authentication Config ===
GRANT_TYPE = config["GRANT_TYPE"]
CLIENT_ID = config["CLIENT_ID"]
SCOPE = config["SCOPE"]
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

# === BigFix Operator Credentials NMO is OK but needs to manage computers that will receive settings and needs to be able to take actions and use RESTAPI
BigFixOperator = config["BigFixOperator"]
BigFixPassword = config["BigFixPassword"]
BigFixRootServerName = config["BigFixRootServerName"]
BigFixRootURL = f"https://{BigFixRootServerName}:52311"
bf_conn=""

# === BigFix objects
BigFixDeviceSettingName = config["BigFixDeviceSettingName"]
BigFixGroupSettingName = config["BigFixGroupSettingName"]
BigFixOwnerIDSettingName = config["BigFixOwnerIDSettingName"]
BigFixOwnerNameSettingName = config["BigFixOwnerNameSettingName"]
BigFixOwnerGroupSettingName = config["BigFixOwnerGroupSettingName"] 
BigFixOwnerUserPrincipalNameSettingName = config["BigFixOwnerUserPrincipalNameSettingName"]
ActionTitlePrefix = config["ActionTitlePrefix"]
action_batchsize = int(config["action_batchsize"])
AutoDelete = config["AutoDelete"]
AutoStop = config["AutoStop"]



    
logging.info ("================== Starting New Session =============================")
    
logging.debug ("=== Setup connections ===")
i = initial ()
access_token = i[0]
bf_conn = i[1]
to_do_settings={}

logging.info ("=== BigFix Device Dictionary ===")
bf_devices = get_BigFix_data(bf_conn)
logging.debug (json.dumps(bf_devices, indent=2))
logging.debug ("--- done with BigFix Device Dictionary ---")

logging.info ("=== Process Device Owners ===")
for d_id in bf_devices:
    if bf_devices[d_id].get('user_id')=="NoOwnerID":
        logging.debug(f"need owner for {d_id}")
        az_pull_and_queue(d_id, bf_devices[d_id].get('compid'))
    else:
        logging.debug (f"{d_id} has owner {bf_devices[d_id].get('user_id')}")
logging.debug(json.dumps(to_do_settings, indent=2))


logging.info ("=== Azure Device Dictionary ===")
az = azure_to_dict(INTERESTING_AZURE_GROUPS)

logging.info ("=== Detect mismatches  ===")
to_do_settings = align(az ,bf_devices)    
logging.debug (f"to_do_settings is currently {to_do_settings}")
logging.info (f"--- done with mismatch detection with {len(to_do_settings)} computers needing action---")


logging.info  ("=== Prior Action Cleanups ===")
if AutoStop or AutoDelete:
    clean_BigFix_prior_actions(bf_conn)
else:
    logging.warning (f"+++ Autodelete and Autostop both False -  please clean up actions by hand in the BigFix console")
logging.debug ("--- done with action cleaning ---")
       

if to_do_settings:       #only send if there are settings to send
    logging.info ("=== Sending device actions to BigFix ====") 
    c = batched_send_it(to_do_settings,action_batchsize)
    logging.info (f"------- {c} Action Batches Sent ---------")
else:
    logging.info("========= No device actions needed =========")



    
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

logging.info ("--- BYE ---")    
logging.debug ("================== ENDING  SESSION  =============================")
    
