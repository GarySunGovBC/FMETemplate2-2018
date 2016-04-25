'''
Created on Apr 18, 2016

@author: kjnether

# Automated template implementation

## Net new parameters:

### Description of Task:

- Add to the header section, in this case its a 
  new line at the bottom of the existing parameters
  example: 
  
  ...
#          --Dest_Instance_Connect "port:5153"
#          --Dest_Password "********"
#          --DEST_DB_ENV_KEY "DLV"
#          --DEST_DB_ENV_KEY "DLV"


### New parameter list

-  DEST_DB_ENV_KEY
-  FILE_CHANGE_DETECTION
-  LOGFILE
-  SRC_ORA_PASSWORD


parameter renames:
USER_ID       to DEST_SCHEMA
DestFeature1  to DEST_FEATURE_1
Dest_Server   to DEST_SERVER
Dest_Instance_Connect  to DEST_PORT
Dest_Password to DEST_PASSWORD
Dest_Instance to DEST_INSTANCE



'''
