# === Swapnil and JT in March 2025
# === thanks to Gary for the environment and JGStew for Python libraries and thoughts
# === property of HCL Software
# ++ April 2025 addded config file and batching
# ++ April 2025 refactored settings handling to better use batching, added UserGroup parts.
# ++ April 2025 added success criteria to actions - added option USE_BIGFIX_FOR_INTERESTING_GROUPS to override or replace INTERESTING_AZURE_GROUPS
# ++ April 2025 dropped exit (0) from action script, converted action to elseif instead of exit(0)
# ++ April 2025 went back to ast method due to json.loads errors - test build
# ++ April 2025 added urllib to fix encoding issue for REST posts to BigFix 
# ++ April 2025 returned to BESAPI after JG added urllib for safety; went back to ' and replace with " to cope with REST bug - this means you need to avoid " in your setting values;
# ++ April 2025 added full encoding test and replace for & ; and "  both literal and embedded inside of strings.

import requests
#from requests.auth import HTTPBasicAuth
import besapi
import json
import os.path
import logging
import itertools
import ast
import sys
import urllib.parse

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
        sys.exit()

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
    session_relevance = f"""({bf_doublequote_literal}{{{bf_doublequote_literal} {bf_amp_literal} it {bf_amp_literal} {bf_doublequote_literal}}}{bf_doublequote_literal})
of concatenation {bf_doublequote_literal}, {bf_doublequote_literal} of ({bf_doublequote_literal}'{bf_doublequote_literal} {bf_amp_literal} item 0 of it {bf_amp_literal}
{bf_doublequote_literal}': {{'compid': '{bf_doublequote_literal} {bf_amp_literal} item 1 of it as string
{bf_amp_literal} {bf_doublequote_literal}', 'compname': '{bf_doublequote_literal}{bf_amp_literal}item 2 of it
{bf_amp_literal} {bf_doublequote_literal}', 'groups': ['{bf_doublequote_literal}
{bf_amp_literal} concatenation {bf_doublequote_literal}', '{bf_doublequote_literal} of tuple string items of item 3 of it  {bf_amp_literal}{bf_doublequote_literal}']{bf_doublequote_literal}
{bf_amp_literal} {bf_doublequote_literal}, 'user_groups': ['{bf_doublequote_literal}
{bf_amp_literal} concatenation {bf_doublequote_literal}', '{bf_doublequote_literal} of tuple string items of item 4 of it  {bf_amp_literal}{bf_doublequote_literal}']{bf_doublequote_literal}
{bf_amp_literal} {bf_doublequote_literal}, 'user_id': '{bf_doublequote_literal} {bf_amp_literal} item 5 of it
{bf_amp_literal} {bf_doublequote_literal}'}}{bf_doublequote_literal}) of (
(value of client setting whose (name of it = {bf_doublequote_literal}{BigFixDeviceSettingName}{bf_doublequote_literal}) of it)|{bf_doublequote_literal}NoDeviceID{bf_doublequote_literal}
, id of it , name of it
,  value of client setting whose (name of it = {bf_doublequote_literal}{BigFixGroupSettingName}{bf_doublequote_literal}) of it |{bf_doublequote_literal}NoGroups{bf_doublequote_literal}
,  value of client setting whose (name of it = {bf_doublequote_literal}{BigFixOwnerGroupSettingName}{bf_doublequote_literal}) of it |{bf_doublequote_literal}NoUserGroups{bf_doublequote_literal}
,  value of client setting whose (name of it = {bf_doublequote_literal}{BigFixOwnerIDSettingName}{bf_doublequote_literal}) of it |{bf_doublequote_literal}NoOwnerID{bf_doublequote_literal} )
of bes computers whose (exists client setting whose (name of it = {bf_doublequote_literal}{BigFixDeviceSettingName}{bf_doublequote_literal} and value of it != {bf_doublequote_literal}NotAzureADJoined{bf_doublequote_literal}) of it)"""
    rd = bigfix_session_relevance(session_relevance) #fully escaped with tested replacements - note python escaping for { and }
    return rd

def get_bigfix_groups(bf_conn): 
    session_relevance = f""" ({bf_doublequote_literal}[{bf_doublequote_literal} {bf_amp_literal} it {bf_amp_literal} {bf_doublequote_literal}]{bf_doublequote_literal})
of concatenation {bf_doublequote_literal},{bf_doublequote_literal} of
({bf_doublequote_literal}{{'id': '{bf_doublequote_literal} {bf_amp_literal} item 0 of it {bf_amp_literal} {bf_doublequote_literal}'}}{bf_doublequote_literal})
of (match (regex {bf_doublequote_literal}[0-9A-Fa-f]{{8}}-([0-9A-Fa-f]{{4}}-){{3}}[0-9A-Fa-f]{{12}}{bf_doublequote_literal})
of  relevance of it, following text of first {bf_doublequote_literal}AzureGroup{bf_doublequote_literal} of name of it)
of bes fixlets whose (group flag of it and name of it starts with {bf_doublequote_literal}AzureGroup{bf_doublequote_literal}
and exists relevance whose (exists match (regex {bf_doublequote_literal}[0-9A-Fa-f]{{8}}-([0-9A-Fa-f]{{4}}-){{3}}[0-9A-Fa-f]{{12}}{bf_doublequote_literal}) of it) of it)"""
    rd = bigfix_session_relevance(session_relevance) #fully escaped with tested replacements - note python escaping for { and }
    return rd

def bigfix_session_relevance(session_relevance):
    logging.debug ("---------- doing BigFix Session Relevance-------")
    logging.debug (session_relevance)

    response = bf_conn.session_relevance_string(session_relevance)
    logging.debug ("++++++++++++++ REST resonse ++++++++++++++++++++")
    logging.debug(response)
    return bf_string_to_dict(response)

def bf_string_to_dict(response):
    logging.debug ("""+++++++++ changing ' to " for json(loads) +++++++++++""")
    response = response.replace("'",'"')
    logging.debug(response)
    logging.debug ("+++++++++++++ converting response to dict +++++++++++++++++") 
    logging.debug(response)
    try:
        rd = json.loads(str(response))
        logging.debug(f"++rd = json.loads(str(response)) worked")#testing - remove later
        logging.debug(rd)
    except Exception as e:
        logging.debug(f"--rd = json.loads(str(response)) failed")
        logging.exception(e)
        sys.exit(1)
    return rd

def bf_doublequote_literal_test():
    logging.debug('**** session relevance REST literal doublequote encoding tests for " %22, %2522 and %252522 &quot; ****')
    if  bf_conn.session_relevance_string(f' "tophat" ') == 'tophat':
        logging.info(f'literal doublequote as " works')
        return '"'
    if  bf_conn.session_relevance_string(f' %22tophat%22') == 'tophat':
        logging.info(f'literal doublequote as %22 works')
        return '%22'
    if  bf_conn.session_relevance_string(f' %2522tophat%2522') == 'tophat':
        logging.info(f'literal doublequote as %2522 works')
        return '%2522'
    if  bf_conn.session_relevance_string(f' %252522tophat%252522') == 'tophat':
        logging.info(f'literal doublequote as %252522 works')
        return '%252522'
    if  bf_conn.session_relevance_string(f' &quot;tophat&quot;') == 'tophat':
        logging.info(f'literal doublequote as %252522 works')
        return '&quot;'
    logging.exception(f'ERROR: Could not find suitable literal doublequote encoding - tried " %22 %2522  %252522 and &quot;')
    sys.exit(f'ERROR: Could not find suitable literal doublequote encoding - tried " %22 %2522  %252522 and &quot;')      

def bf_amp_literal_test():
    logging.debug('**** session relevance REST literal apmersand encoding tests for & %26 %2526 %252526 and &amp; ****')
    if  bf_conn.session_relevance_string(f' {bf_doublequote_literal}top{bf_doublequote_literal} & {bf_doublequote_literal}hat{bf_doublequote_literal} ') == "tophat":
        logging.info(f'literal apmersand as & works')
        return '&'
    if  bf_conn.session_relevance_string(f' {bf_doublequote_literal}top{bf_doublequote_literal} %26 {bf_doublequote_literal}hat{bf_doublequote_literal} ') == "tophat":
        logging.info(f'literal apmersand as %26 works')
        return '%26'
    if  bf_conn.session_relevance_string(f' {bf_doublequote_literal}top{bf_doublequote_literal} %2526 {bf_doublequote_literal}hat{bf_doublequote_literal} ') == "tophat":
        logging.info(f'literal apmersand as %2526 works')
        return '%2526'
    if  bf_conn.session_relevance_string(f' {bf_doublequote_literal}top{bf_doublequote_literal} %252526 {bf_doublequote_literal}hat{bf_doublequote_literal}') == "tophat":
        logging.info(f'literal apmersand as %26 works')
        return '%252526'
    if  bf_conn.session_relevance_string(f' {bf_doublequote_literal}top{bf_doublequote_literal} &amp; {bf_doublequote_literal}hat{bf_doublequote_literal} ') == "tophat":
        logging.info(f'literal apmersand as &amp; works')
        return '&amp;'
    logging.exception(f'ERROR: Could not find suitable ampersand encoding - tried & %26 %2526 %252526 and &amp;')
    sys.exit(f'ERROR: Could not find suitable literal ampersand encoding - tried & %26 and &amp;')

def bf_semicolon_literal_test():
    logging.debug('**** session relevance REST literal semicolon encoding tests for ; %3b %253b %25253b****')
    if  bf_conn.session_relevance_string(f' concatenation of ({bf_doublequote_literal}top{bf_doublequote_literal} ; {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == "tophat":
        logging.info(f'literal semicolon as ; works')
        return ';'
    if  bf_conn.session_relevance_string(f' concatenation of ({bf_doublequote_literal}top{bf_doublequote_literal} %3b {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == "tophat":
        logging.info(f'literal semicolon as %3b works')
        return '%3b'
    if  bf_conn.session_relevance_string(f' concatenation of ({bf_doublequote_literal}top{bf_doublequote_literal} %253b {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == "tophat":
        logging.info(f'literal semicolon as %253b works')
        return '%253b'
    if  bf_conn.session_relevance_string(f' concatenation of ({bf_doublequote_literal}top{bf_doublequote_literal} %25253b {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == "tophat":
        logging.info(f'literal semicolon as %25253b works')
        return '%25253b'
    logging.exception(f'ERROR: Could not find suitable literal semicolon encoding - tried ; %3b %253b %25253b')
    sys.exit(f'ERROR: Could not find suitable literal semicolon encoding - tried ; %3b %253b %25253b')
  
def bf_doublequote_embedded_test():
    logging.debug('**** session relevance REST embedded doublequote encoding tests for %22, %2522 and %252522 ****')
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%22{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top"hat':
        logging.info(f'embedded doublequote as %22 works')
        return '%22'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%2522{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} ; {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top"hat':
        logging.info(f'embedded doublequote as %2522 works')
        return '%2522'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%252522{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} ; {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top"hat':
        logging.info(f'embedded doublequote as %252522 works')
        return '%252522'    
    logging.exception(f'ERROR: Could not find suitable embedded doublequote encoding - tried %22, %2522 and %252522')
    sys.exit(f'ERROR: Could not find suitable embedded doublequote encoding - tried %22, %2522 and %252522')    


def bf_amp_embedded_test():
    logging.debug('**** session relevance REST embedded apmersand encoding tests for & %26 %2526 %252526 &amp; and &amp; with literal subsitutions ****')
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}&{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top&hat':
        logging.info(f'embedded apmersand as & works')
        return '&'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%26{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top&hat':
        logging.info(f'embedded apmersand as %26 works')
        return '%26'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%2526{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top&hat':
        logging.info(f'embedded apmersand as %2526 works')
        return '%2526'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%252526{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top&hat':
        logging.info(f'embedded apmersand as %252526 works')
        return '%252526'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}&amp;{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top&hat':
        logging.info(f'embedded apmersand as &amp; works')
        return '&amp;'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}{bf_amp_literal}amp{bf_semi_literal}{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top&hat':
        logging.info(f'embedded apmersand as &amp; works but only with literal substitutions')
        return f'{bf_amp_literal}amp{bf_semi_literal}'     
    logging.exception(f'ERROR: Could not find suitable embedded ambersand encoding - tried & %26 %2526 %252526 &amp; and &amp; with literal subsitutions ')
    sys.exit(f'ERROR: Could not find suitable embedded ambersand encoding - tried & %26 %2526 %252526 &amp; and &amp; with literal subsitutions ')

def bf_semicolon_embedded_test():
    logging.debug('**** session relevance REST embedded semicolon encoding tests for ; %3b %253b %25253b****')
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal};{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top;hat':
        logging.info(f'embedded semicolon as ; works')
        return ';'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%3b{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top;hat':
        logging.info(f'embedded semicolon as %3b works')
        return '%3b'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%253b{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top;hat':
        logging.info(f'embedded semicolon as %253b works')
        return '%253b'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%25253b{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top;hat':
        logging.info(f'embedded semicolon as %25253b works')
        return '%25253b'
    if  bf_conn.session_relevance_string(f' concatenation {bf_doublequote_literal}%25253b{bf_doublequote_literal} of ({bf_doublequote_literal}top{bf_doublequote_literal} {bf_semi_literal} {bf_doublequote_literal}hat{bf_doublequote_literal}) ') == 'top;hat':
        logging.info(f'embedded semicolon as %25253b works')
        return '%25253b'
    logging.exception(f'ERROR: Could not find suitable embedded semicolon encoding - tried ; %3b %253b %25253b')
    sys.exit(f'ERROR: Could not find suitable embedded semicolon encoding - tried ; %3b %253b %25253b')

    
def az_pull_and_queue(d_id,compid):
    url=f"https://graph.microsoft.com/v1.0/devices?$filter=deviceId eq '{d_id}'&$select=id,displayName,deviceId&$expand=registeredOwners($select=id,displayName,userPrincipalName)"
    logging.debug(url)
    headers = {'Authorization': f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
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
        if bfd != azd and not((bfd == ['NoAzureGroups'] or bfd == ['None']) and azd == None):
            logging.debug (f"(DG MISMATCH) for {dev} with BigFix Comptuer ID {bf_devices[dev]['compid']}  bfd is {bfd} and azd is {azd}")
            if compid in to_do_settings:
                to_do_settings[compid].update({BigFixGroupSettingName: azd})
            else:
                to_do_settings[compid] ={BigFixGroupSettingName: azd}
        else:
            logging.debug (f"(DG MATCH) {dev} with BigFix Comptuer ID {bf_devices[dev]['compid']}  bfd is {bfd} and azd is {azd}")
        # user group section
        user = bf_devices[dev].get('user_id','NoUser')
        bfu = sorted(bf_devices[dev]['user_groups'])
        if bfu == ['N', 'U', 'e', 'o', 'r', 's'] or bfu == ['NoUserGroups'] or bfu == "":
            bfu = "NoUserGroups"
        azu = az_users.get(bf_devices[dev]['user_id'],"NoUser")
        if azu != None:
            azu =sorted(azu)
        if azu == ['N', 'U', 'e', 'o', 'r', 's'] or azu == "": #old edge case
            azu = "NoUserGroups"
        if azu != bfu and not (bfu == ['NoRegisteredOwnerFound']):
            logging.debug (f"(UG MISMATCH) for {user} with BigFix Comptuer ID {compid}  bfu is {bfu} and azu is {azu}")
            if compid in to_do_settings:
                to_do_settings[compid].update({BigFixOwnerGroupSettingName: azu})
            else:
                to_do_settings[compid] = {BigFixOwnerGroupSettingName: azu}
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
            setting_xml.append("""   \nendif""") 
            computer_xml = ("\n   ".join(setting_xml))
            action_xml.append(computer_xml)

        action_xml.append("]]></ActionScript>")
        action_xml= "\n".join(action_xml)
        bt= "\n".join(batch_targets)
        batch_xml = f"""<SingleAction>\n    <Title>{title}</Title>\n    <Relevance><![CDATA[{relevance}]]></Relevance>\n    {action_xml}\n
    <SuccessCriteria Option="RunToCompletion"></SuccessCriteria>
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



# === check for actions that are ready to be cleaned up ===
def clean_BigFix_prior_actions(bf_conn):
    if not bf_conn:
        bf_conn = besapi.besapi.BESConnection(BigFixOperator, BigFixPassword, BigFixRootURL)
    if AutoDelete:
        logging.debug(f"Automatically deleting old actions")
        session_relevance = f"""({bf_doublequote_literal}[{bf_doublequote_literal} {bf_amp_literal} it {bf_amp_literal} {bf_doublequote_literal}]{bf_doublequote_literal})
of concatenation {bf_doublequote_literal}, {bf_doublequote_literal} of (it as string) of ids of bes actions whose
(name of issuer of it = {bf_doublequote_literal}{BigFixOperator}{bf_doublequote_literal} AND name of it starts with {bf_doublequote_literal}{ActionTitlePrefix} for {bf_doublequote_literal})"""
        logging.debug(f"session_relevance {session_relevance}")#fully escaped with tested replacements
        rd = bigfix_session_relevance(session_relevance)
        logging.debug(f'---- {rd} ------')
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
            logging.debug(f"Automatically stopping old actions")
            logging.debug ("   Finding actions to stop")
            session_relevance = f"""({bf_doublequote_literal}[{bf_doublequote_literal} {bf_amp_literal} it {bf_amp_literal} {bf_doublequote_literal}]{bf_doublequote_literal})
of concatenation {bf_doublequote_literal}, {bf_doublequote_literal} of (it as string) of ids of bes actions whose
(name of issuer of it = {bf_doublequote_literal}{BigFixOperator}{bf_doublequote_literal} AND name of it starts with {bf_doublequote_literal}{ActionTitlePrefix} for {bf_doublequote_literal}
 ) AND state of it = {bf_doublequote_literal}Open{bf_doublequote_literal} )"""
            logging.debug(f"session_relevance {session_relevance}")#fully escaped with tested replacements
            rd = bigfix_session_relevance(session_relevance)
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
USE_BIGFIX_FOR_INTERESTING_GROUPS = config["USE_BIGFIX_FOR_INTERESTING_GROUPS"]
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

BigFixGroups={}
to_do_settings={}
bf_conn=""
    
logging.info ("================== Starting New Session =============================")
    
logging.debug ("=== Setup connections ===")
access_token, bf_conn = initial()
#the encoding circus

logging.info (f"===========Testing BigFix REST encodings============")
bf_doublequote_literal = bf_doublequote_literal_test() 
bf_amp_literal = bf_amp_literal_test()
bf_semi_literal = bf_semicolon_literal_test()
bf_doublequote_embedded = bf_doublequote_embedded_test()
bf_amp_embedded = bf_amp_embedded_test()
bf_semi_embedded = bf_semicolon_embedded_test()




if USE_BIGFIX_FOR_INTERESTING_GROUPS:
    logging.info (f"=========overriding INTERESTING_AZURE_GROUPS because USE_BIGFIX_FOR_INTERESTING_GROUPS = {USE_BIGFIX_FOR_INTERESTING_GROUPS}")
    INTERESTING_AZURE_GROUPS = get_bigfix_groups(bf_conn)
    logging.debug (f"INTERESTING_AZURE_GROUPS is now {INTERESTING_AZURE_GROUPS}")

logging.info ("=== BigFix Device Dictionary ===")
bf_devices = get_BigFix_data(bf_conn)
logging.debug (json.dumps(bf_devices, indent=2))
logging.debug (f"--- done with BigFix Device Dictionary with {len(bf_devices)} entries ---")

logging.info ("=== Process Device Owners ===")
for d_id in bf_devices:
    if bf_devices[d_id].get('user_id')=="NoOwnerID": #do we also need to check for none/null?
        logging.debug(f"need owner for {d_id}")
        az_pull_and_queue(d_id, bf_devices[d_id].get('compid'))
    else:
        logging.debug (f"{d_id} already has owner {bf_devices[d_id].get('user_id')}")
logging.debug(json.dumps(to_do_settings, indent=2))
logging.info (f"--- done with Device Owners with {len(to_do_settings)} computers needing action ---")

logging.info ("=== Azure Device Dictionary ===")
az = azure_to_dict(INTERESTING_AZURE_GROUPS)
logging.info (f"--- done with with pulling groups. {len(az)} interesting groups have members ---")

logging.info ("=== Detect mismatches  ===")
to_do_settings = align(az ,bf_devices)    
logging.debug (f"to_do_settings is currently {to_do_settings}")
logging.info (f"--- done with mismatch detection with {len(to_do_settings)} computers needing action ---")

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
    
