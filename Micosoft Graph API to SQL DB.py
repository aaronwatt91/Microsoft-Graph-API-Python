#========================================================================================
#	Proc Name:		PyImportUsers
#	Description:	Gets All users from Graph API and imports in to Stage DB
#	
#	Created By:		Aaron Watt
#	Created On:		2021-11-26
#========================================================================================
#	Change Log
#	Version	|	User	|	Date		|	Change
#	1.0			 AW		   2021-11-26		Inital Creation
#========================================================================================
#
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
import urllib

#Declare Parameters 

PackageCode = "E001"

app_id = 'app_id'
client_secret = 'client secret'
users_url = 'https://graph.microsoft.com/v1.0/users?$top=100'
token_url = 'token url'

DatabaseTable = "stg_msgraph_user_test"
SQLSTAGEDB = "DRIVER={SQL Server};SERVER=;DATABASE=;UID=;PWD=;Trusted_Connection=no;"

token_data = {
                'grant_type': 'client_credentials',
                'client_id': app_id,
                'client_secret': client_secret,
                'resource': 'https://graph.microsoft.com',
                'scope': 'https://graph.microsoft.com',
             }

token_r = requests.post(token_url, data=token_data)
token = token_r.json().get('access_token')

#Use the token using microsoft graph endpoints

headers = {'Authorization': 'Bearer {}'.format(token)}

#First Exec Against Azure AD

response_data = json.loads(requests.get(users_url,headers=headers).text)

Records_df = pd.DataFrame.from_dict(response_data['value'])

#Get Next Link 

while True:
    if response_data.get('@odata.nextLink') is None:
        break
    users_url = response_data.get('@odata.nextLink')
    response_data = json.loads(requests.get(users_url,headers=headers).text)
    Records_df = Records_df.append(pd.DataFrame.from_dict(response_data['value']), ignore_index=True)

#Create Dataframe with correct values for SQL

new_df =  Records_df.rename(columns={"id": "AZGuid"})
new_df = new_df.reindex(columns=[
                                    "AZGuid",
                                    "displayName",
                                    "givenName",
                                    "jobTitle",
                                    "mail",
                                    "mobilePhone",
                                    "officeLocation",
                                    "preferredLanguage",
                                    "surname",
                                    "userPrincipalName",
                                    
                                ])

print (new_df)


#Connect and insert rows into SQL 

params = urllib.parse.quote_plus(SQLSTAGEDB)

conn_str = "mssql+pyodbc:///?odbc_connect={}".format(params)
engine = create_engine(conn_str, fast_executemany=True)
new_df.to_sql(name=DatabaseTable, con=engine, if_exists="append",index=False)
