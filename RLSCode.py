

import math
import numpy as np
import http.client
from io import BytesIO
import pandas as pd
from io import StringIO
import sqlalchemy
import xlrd
import io
import datetime as dt
from pyspark.sql import SparkSession
import pyspark
import json
import base64

#Snowflake Credentials
user = 'hidden'
password = 'hidden'
#Dundas Credentials
accountName = "hidden"
dundaspassword = "hidden"



#Connect to Dundas and create a sessionid
def DundasConnection():
  baseUrl ='https://dundas.hq.asea.net'
  conn = http.client.HTTPSConnection("dundas.hq.asea.net")
  payload = json.dumps({
  "accountName": accountName,
  "password": dundaspassword,
  "cultureName":"en-us",
  "deleteOtherSessions":"false",
   "isWindowsLogOn":"false"
})
  headers = {
  'Content-Type': 'application/json'
}
  url= baseUrl + '/Api/LogOn/'
  conn.request("POST",url, payload, headers)
  res = conn.getresponse()
  data = res.read()
  data.decode("utf-8")
  data = json.loads(data)
  print(data)
  sessionid = data[ "sessionId"]
  return sessionid






#Snowflake connection to DB_ASEA_REPORTS
options2 = {
  "sfUrl": "https://ba62849.east-us-2.azure.snowflakecomputing.com/",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "DB_ASEA_REPORTS",
  "sfSchema": "ASSOCIATE_SUPPORT",
  "sfWarehouse": "COMPUTE_MACHINE",
  "truncate_table" : "ON",
  "usestagingtable" : "OFF"
}


#Extract the RLS_FINAL_SNAPSHOT Table and out it in a Spark dataframe
spark = SparkSession.builder.appName(
  "pandas to spark").getOrCreate()


RLS_Spark = spark.read \
  .format("snowflake") \
  .options(**options2) \
  .option("dbtable", "RLS_FINAL_SNAPSHOT") \
  .load()


#Convert the Spark datframe to a python dataframe
RLS = RLS_Spark.toPandas()
# print(RLS)


#Get the manager email list
MATCHED_MANAGEMENT_DUNDAS_USERNAMEListduplicate = RLS['MATCHED_MANAGEMENT_DUNDAS_USERNAME'].tolist()
# MATCHED_MANAGEMENT_DUNDAS_USERNAMEListduplicate.remove(None)
MATCHED_MANAGEMENT_DUNDAS_USERNAMEList = list(set(MATCHED_MANAGEMENT_DUNDAS_USERNAMEListduplicate)) 
#MATCHED_MANAGEMENT_DUNDAS_USERNAMEList.remove(None)
MATCHED_MANAGEMENT_DUNDAS_USERNAMEList.remove("mwoodward@asea.net")
MATCHED_MANAGEMENT_DUNDAS_USERNAMEList.remove("mabidi@asea.net")
MATCHED_MANAGEMENT_DUNDAS_USERNAMEList.remove("ksagers@asea.net")

print(MATCHED_MANAGEMENT_DUNDAS_USERNAMEList)


#Get agent email list
AGENT_DUNDAS_USERNAMEListduplicate = RLS['AGENT_DUNDAS_USERNAME'].tolist()
# AGENT_DUNDAS_USERNAMEListduplicate.remove(None)
AGENT_DUNDAS_USERNAMEList = list(set(AGENT_DUNDAS_USERNAMEListduplicate)) 



CallCenterEmailList = MATCHED_MANAGEMENT_DUNDAS_USERNAMEList + AGENT_DUNDAS_USERNAMEList
print(CallCenterEmailList)



BI_List = ['mwoodward@asea.net','mabidi@asea.net','ksagers@asea.net']



#Snowflake Connection to  DB_RAW_DATA"
options = {
  "sfUrl": "https://ba62849.east-us-2.azure.snowflakecomputing.com/",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "DB_RAW_DATA",
  "sfSchema": "ASSOCIATE_SUPPORT",
  "sfWarehouse": "COMPUTE_MACHINE",
  "truncate_table" : "ON",
  "usestagingtable" : "OFF"
}


#Extract the DUNDAS_AGENT_ACCOUNTS Table and out it in a Spark dataframe
spark = SparkSession.builder.appName(
  "pandas to spark").getOrCreate()


DUNDAS_AGENT_ACCOUNTS_Spark = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "DUNDAS_AGENT_ACCOUNTS") \
  .load()

#Convert the Spark datframe to a python dataframe
DUNDAS_AGENT_ACCOUNTS = DUNDAS_AGENT_ACCOUNTS_Spark.toPandas()
print(DUNDAS_AGENT_ACCOUNTS)



# Bulk Dundas accounts creation and Store their id and their emails in the snowflake table  DUNDAS_AGENT_ACCOUNTS, as long as the account exists no matter how many times you run the code it does not create duplicate dundas accounts for trhe same Dundas user
def CreatingNewAccounts(AGENT_AD_EMAIL):
    conn = http.client.HTTPSConnection("dundas.hq.asea.net")
    sessionid = DundasConnection()
    targetpath = '/API/Account/?sessionId=' + sessionid
    isEnabled = "true"
    isApiAccount = "false"
    trackLogOnHistory = "true"
    isSeatReserved= "false"
    canChangePassword = "true"
    passwordNeverExpires = "false"
    isAccountExpired = "false"
    isPasswordExpired="false"
    isLocked="false"
    payload = json.dumps({"__classType":"dundas.account.Account","name":AGENT_AD_EMAIL,"description":"","accountType":"ExternalUser","timeZone":"","isEnabled":isEnabled,"isApiAccount":isApiAccount,"trackLogOnHistory":trackLogOnHistory,"allowedIPAddresses":"","logOnCount":0,"seatKind":"StandardUser","isSeatReserved":isSeatReserved,"grantedApplicationPrivilegeIds":[],"deniedApplicationPrivilegeIds":[],"customAttributes":[],"canChangePassword":canChangePassword,"passwordNeverExpires":passwordNeverExpires,"isAccountExpired":isAccountExpired,"isPasswordExpired":isPasswordExpired,"tenantId":"81711fa2-9bab-40a0-8421-36389fdf2b70","isLocked":isLocked,"federatedAuthenticationProviderIds":["AzureAD"],"warnings":[]})
    headers = {
      'Content-Type': 'application/json'
    }
    
    
    conn.request("POST", targetpath, payload, headers)
    res = conn.getresponse()
    if res.getcode() != 200:
        pass
    else:
        data = res.read()
        print(data.decode("utf-8"))
        data = json.loads(data)
        DUNDAS_ACCOUNT_ID1 = data["id"]
        print(DUNDAS_ACCOUNT_ID1)
        AGENT_AD_EMAIL1 =  data["name"]
        print(AGENT_AD_EMAIL1)
        DUNDAS_AGENT_ACCOUNTS_Spark1 = spark.read \
        .format("snowflake") \
         .options(**options) \
        .option("dbtable", "DUNDAS_AGENT_ACCOUNTS") \
         .load()

        #Convert the Spark datframe to a python dataframe
        DUNDAS_AGENT_ACCOUNTS1 = DUNDAS_AGENT_ACCOUNTS_Spark1.toPandas()
#         DUNDAS_AGENT_ACCOUNTS1.loc[len(DUNDAS_AGENT_ACCOUNTS1.index)] = [DUNDAS_ACCOUNT_ID, AGENT_AD_EMAIL] 
#         print(DUNDAS_AGENT_ACCOUNTS1)
        
        
        df2 = {'DUNDAS_ACCOUNT_ID': DUNDAS_ACCOUNT_ID1, 'AGENT_AD_EMAIL': AGENT_AD_EMAIL1}
        DUNDAS_AGENT_ACCOUNTS1 = DUNDAS_AGENT_ACCOUNTS1.append(df2, ignore_index = True)
        
        
        DUNDAS_AGENT_ACCOUNTS_spark2 = spark.createDataFrame(DUNDAS_AGENT_ACCOUNTS1)
        DUNDAS_AGENT_ACCOUNTS_spark2.write.format("net.snowflake.spark.snowflake") \
        .options(**options) \
        .option("dbtable", "DUNDAS_AGENT_ACCOUNTS") \
        .mode('Overwrite') \
        .options(header=False) \
        .save()

for AGENT_AD_EMAIL  in CallCenterEmailList:
  CreateAccount = CreatingNewAccounts(AGENT_AD_EMAIL)

  

#add all the values of the custome attribute "InContactID" to each Dundas account according to  the security Hierachy defined in the snowflake table DB_RAW_DATA.ASSOCIATE_SUPPORT.WFM_WEEKLY_EGNYTE_AGENT_BASE
def AddCustomAttrbuteValues(Id, sessionid,CustomeAttributeKey, CustomeAttributeValues):
       isInherited = "false"
       isInheritanceConflict = "false"
       targetpath = '/API/Account/UpdateCustomAttributesForAccount/' + str(Id) + '/?sessionId=' + sessionid
# print(targetpath)
# conn.request("GET", targetpath, payload, headers)
       conn = http.client.HTTPSConnection("dundas.hq.asea.net")
       payload = json.dumps([{"__classType":"dundas.account.CustomAttribute","key":CustomeAttributeKey,"value":{"__classType":"dundas.account.CustomAttributeValue","attributeId":CustomeAttributeKey,"values":CustomeAttributeValues,"isInherited":isInherited,"isInheritanceConflict":isInheritanceConflict}}])
       headers = {
  'Content-Type': 'application/json'
}
       conn.request("POST", targetpath, payload, headers)
       res = conn.getresponse()
       data = res.read()
       data.decode("utf-8")
       data = json.loads(data)
       return data
  

  
##Extract the updated DUNDAS_AGENT_ACCOUNTS Table and put it in a Spark dataframe
DUNDAS_AGENT_ACCOUNTS_Spark = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "DUNDAS_AGENT_ACCOUNTS") \
  .load()

#Convert the Spark datframe to a python dataframe
DUNDAS_AGENT_ACCOUNTS = DUNDAS_AGENT_ACCOUNTS_Spark.toPandas()
print(DUNDAS_AGENT_ACCOUNTS)

sessionid = DundasConnection()





def addtogroup(Id, sessionid):
      conn = http.client.HTTPSConnection("dundas.hq.asea.net")
      groupId = 'bf60517b-869c-4d5d-917b-da102637e4d4'
      targetpath = '/API/Group/AddMembers/' + groupId + '/?sessionId=' + sessionid
      list=[]
      list.append(Id)
      payload = json.dumps({"accountIds":list})
      headers = {
  'Content-Type': 'application/json'
}
      conn.request("POST",targetpath, payload, headers)
      res = conn.getresponse()
      data = res.read()
      data.decode("utf-8")
      print(res.getcode())




    
#loop through  the managers emails list and through  the agent emails list  and extract their dundas account Id, and assign values to their Custom Attribute "InContactID" by figurig out the AGENT_INCONATCTID linked to every agents and the many AGENT_INCONATCTID numbers  that every supervisor and manager is responsable of, And then assigning the values of those AGENT_INCONATCTID numbers to the Custom Attribute " InComtactID" of every manager, Superviosr and agent.
for Email in MATCHED_MANAGEMENT_DUNDAS_USERNAMEList:
    InContactID=RLS.loc[RLS.MATCHED_MANAGEMENT_DUNDAS_USERNAME == Email,'AGENT_INCONTACTID'].values.tolist()
    length = len(InContactID)
    for i in range(length):
      InContactID[i] = str(InContactID[i])
    print(InContactID)
      
    Idduplicateliste = DUNDAS_AGENT_ACCOUNTS.loc[DUNDAS_AGENT_ACCOUNTS.AGENT_AD_EMAIL == Email,'DUNDAS_ACCOUNT_ID'].values.tolist()
    Idliste= list(set(Idduplicateliste))
#     print(type(Idliste))
#     print(Idliste)
    Id= Idliste[0]
    print(Id)
    CustomeAttributeKey = "74ff406c-7318-449f-8c14-ceb4724e7816"
    CustomeAttributeValues = InContactID
    result = AddCustomAttrbuteValues(Id, sessionid,CustomeAttributeKey, CustomeAttributeValues)
    print(result) 
    AddingAssocaiteSupportToGroup =   addtogroup(Id, sessionid)  
    print(AddingAssocaiteSupportToGroup) 

for Email in AGENT_DUNDAS_USERNAMEList:
    rls = RLS[['AGENT_INCONTACTID', 'AGENT_DUNDAS_USERNAME']]
    rls.drop_duplicates(inplace=True)
    InContactID=rls.loc[rls.AGENT_DUNDAS_USERNAME == Email,'AGENT_INCONTACTID'].values.tolist()
    length = len(InContactID)
    for i in range(length):
      InContactID[i] = str(InContactID[i])
    print(InContactID)
      
    
    Idduplicateliste = DUNDAS_AGENT_ACCOUNTS.loc[DUNDAS_AGENT_ACCOUNTS.AGENT_AD_EMAIL == Email,'DUNDAS_ACCOUNT_ID'].values.tolist()
    Idliste= list(set(Idduplicateliste))
#     print(Idliste)
    Id = Idliste[0]
    print(Id)
    CustomeAttributeKey = "74ff406c-7318-449f-8c14-ceb4724e7816"
    CustomeAttributeValues = InContactID
    result = AddCustomAttrbuteValues(Id, sessionid,CustomeAttributeKey, CustomeAttributeValues)
    print(result)
    AddingAssocaiteSupportToGroup =   addtogroup(Id, sessionid)  
    print(AddingAssocaiteSupportToGroup) 
    
    
    
   
for Email in BI_List :
    InContactID=RLS.loc[RLS.MATCHED_MANAGEMENT_DUNDAS_USERNAME == Email,'AGENT_INCONTACTID'].values.tolist()
    length = len(InContactID)
    for i in range(length):
      InContactID[i] = str(InContactID[i])
    print(InContactID)
      
    Idduplicateliste = DUNDAS_AGENT_ACCOUNTS.loc[DUNDAS_AGENT_ACCOUNTS.AGENT_AD_EMAIL == Email,'DUNDAS_ACCOUNT_ID'].values.tolist()
    Idliste= list(set(Idduplicateliste))
#     print(type(Idliste))
#     print(Idliste)
    Id= Idliste[0]
    print(Id)
    CustomeAttributeKey = "74ff406c-7318-449f-8c14-ceb4724e7816"
    CustomeAttributeValues = InContactID
    result = AddCustomAttrbuteValues(Id, sessionid,CustomeAttributeKey, CustomeAttributeValues)
    print(result)
    AddingAssocaiteSupportToGroup =   addtogroup(Id, sessionid)  
    print(AddingAssocaiteSupportToGroup) 
    
