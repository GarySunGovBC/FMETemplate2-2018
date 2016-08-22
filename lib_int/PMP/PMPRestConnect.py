'''
Created on Sep 2, 2014

@author: kjnether

playing around for now trying to figure out if I can connect
to the PMP rest interface from python
'''
import os.path
import requests
import logging
import sys
import warnings

class PMP(object):
    '''
    This class provides a simple python api that will 
    interface with the pmp rest api.  It interacts with 
    the rest api allowing for the retrieval of accounts
    and then passwords assocated with those accounts.
    '''
    
    def __init__(self, configDict):
        self.__initLogging()
        self.token = configDict['token']
        self.baseUrl = configDict['baseurl']
        self.restDir = configDict['restdir']
        self.tokenKey = 'AUTHTOKEN'
        
    def __initLogging(self):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Logging set up in the module: " + str(os.path.basename(__file__)))
        
    def getTokenDict(self):
        tokenDict = {self.tokenKey: self.token}
        return tokenDict
    
    def getResources(self):
        url = 'https://' + self.baseUrl + self.restDir + 'resources'
        self.logger.debug("PMP resource url:" + url)
        tokenDict = self.getTokenDict()
        
        r = requests.get(url, params=tokenDict, verify=False)
        #r = requests.get(url, verify=False)
        resources = r.json()
        resourcObjects = None
        if ( resources.has_key('operation') ) and resources['operation'].has_key('Details'):
            resourcObjects = resources['operation']['Details']
            self.logger.debug("returned resource objects: " + str(resourcObjects))
        else:
            msg = 'unable to read resources from PMP. Probably a token problems!  PMP message {0}'
            msg = msg.format(resources)
            self.logger.error(msg)

            raise ValueError, msg
            # TODO: might consider raising an error here.        
        return resourcObjects
    
    def getResourceId(self, resourceName):
        resourceId = None
        self.logger.debug("getting the log id for the resource name:" + str(resourceName))
        resources = self.getResources()
        for resource in resources:
            if resource['RESOURCE NAME'] == resourceName:
                resourceId = resource['RESOURCE ID']
                break
        self.logger.debug("resource id for (" + str(resourceName) + ') is (' + str(resourceId) + ')')
        return resourceId
    
    def getAccounts(self, resourceName='DA-BCGWDLV-WHSE'):
        '''
        Given a resource name, the method will retrieve the resource id 
        for the resource name.  It will then query that resource for a 
        list of all the accounts, and will return a list of dictionaries
        with information about those accounts.
        
        Each element in the returned list will contain the following keys:
            PASSWORD STATUS
            ACCOUNT ID - This is the value that we want to retrieve to query
                         the account for the actual password.
            PASSWDID
            ISREASONREQUIRED
            ISFAVPASS
            ACCOUNT NAME    - This is the text value with the actual account
                              name.
            AUTOLOGONLIST
            AUTOLOGONSTATUS
        
        This is an example of the raw data returned by the query to the 
        rest end point before the account information is extracted
                {u'operation': 
            {u'Details': 
                {u'RESOURCE ID': u'3453',
                 u'RESOURCE NAME': u'SOMERES',
                 u'DNS NAME': u'urlinthegov.bcgov', 
                 u'RESOURCE TYPE': u'Database Computer',
                 u'RESOURCE URL': u'', 
                 u'PASSWORD POLICY': u'a_polcy', 
                 u'LOCATION': u'', 
                 u'RESOURCE DESCRIPTION': u'passwordDescript', 
                 u'RESOURCE OWNER': u'IDIR\\someUser', 
                 u'DEPARTMENT': u'',
                 u'ACCOUNT LIST': [
                                    {u'PASSWORD STATUS': u'****',
                                     u'ACCOUNT ID': u'12375',
                                     u'PASSWDID': u'12375',
                                     u'ISREASONREQUIRED': u'false',
                                     u'ISFAVPASS': u'false',
                                     u'ACCOUNT NAME': u'app_pmp',
                                     u'AUTOLOGONLIST': [],
                                     u'AUTOLOGONSTATUS': u'User is not allowed to automatically logging in to remote systems in mobile'},
                                   
                                    {u'PASSWORD STATUS': u'****', u'ACCOUNT ID': u'12365', u'PASSWDID': u'12365', u'ISREASONREQUIRED': u'false',
                                     u'ISFAVPASS': u'false', u'ACCOUNT NAME': u'AnAccountName', u'AUTOLOGONLIST': [],
                                     u'AUTOLOGONSTATUS': u'dont let this user login from apple device'},
                                    {u'PASSWORD STATUS': u'****', u'ACCOUNT ID': u'12366', u'PASSWDID': u'12366', u'ISREASONREQUIRED': u'false',
                                     u'ISFAVPASS': u'false', u'ACCOUNT NAME': u'AnAcocountName', u'AUTOLOGONLIST': [],
                                     u'AUTOLOGONSTATUS': u'User is not allowed to automatically logging in to remote systems in mobile'},
                                    {u'PASSWORD STATUS': u'****', u'ACCOUNT ID': u'12367', u'PASSWDID': u'12367', u'ISREASONREQUIRED': u'false',
                                     u'ISFAVPASS': u'false', u'ACCOUNT NAME': u'AnAccountName', u'AUTOLOGONLIST': [],
                                     u'AUTOLOGONSTATUS': u'User is not allowed to automatically logging in to remote systems in mobile'},
                                    {u'PASSWORD STATUS': u'****', u'ACCOUNT ID': u'12368', u'PASSWDID': u'12368', u'ISREASONREQUIRED': u'false',
                                     u'ISFAVPASS': u'false', u'ACCOUNT NAME': u'AnAcocountName', u'AUTOLOGONLIST': [],
                                     u'AUTOLOGONSTATUS': u'User is not allowed to automatically logging in to remote systems in mobile'},
                                    {u'PASSWORD STATUS': u'****', u'ACCOUNT ID': u'12332', u'PASSWDID': u'12332', u'ISREASONREQUIRED': u'false',
                                     u'ISFAVPASS': u'false', u'ACCOUNT NAME': u'AnAcocountName', u'AUTOLOGONLIST': [],
                                     u'AUTOLOGONSTATUS': u'User is not allowed to automatically logging in to remote systems in mobile'},
                                    {u'PASSWORD STATUS': u'****', u'ACCOUNT ID': u'12333', u'PASSWDID': u'12333', u'ISREASONREQUIRED': u'false',
                                     u'ISFAVPASS': u'false',u'ACCOUNT NAME': u'SomeAccount', u'AUTOLOGONLIST': [],
                                      u'AUTOLOGONSTATUS': u'User is not allowed to automatically logging in to remote systems in mobile'},
                                    }}

        '''
        print 'resourceName is:', resourceName
        resId = self.getResourceId(resourceName)
        accnts = self.getAccountsForResourceID(resId)
        #self.logger.debug("the accounts for the resource name (" + str(resourceName) + \
        #                  ') are (' + ','.join(accnts) + ')') 
        return accnts
        
    def getAccountsForResourceID(self, resId):
        '''
        Given a resource id will return the accounts
        for that resource id.
        '''
        url = 'https://' + self.baseUrl + self.restDir + \
              'resources' + '/' + str(resId) + '/accounts'
        tokenDict = self.getTokenDict()
        r = requests.get(url, params=tokenDict, verify=False)
        accnts = r.json()
        #print 'account json: ', accnts

        justAccnts = accnts['operation']['Details']['ACCOUNT LIST']
        #print 'justAccnts', justAccnts
        return justAccnts
        
    def getAccountId(self, accntName, resourceId):
        accntId = None
        accnts = self.getAccountsForResourceID(resourceId)
        for accnt in accnts:
            if accnt['ACCOUNT NAME'].lower().strip() == accntName.lower().strip():
                accntId = accnt['ACCOUNT ID']
                break
        self.logger.debug("Account id for the account name (" + \
                          "(" + str(accntName)+") is (" + str(accntId) + ')')
        return accntId
        
    def getAccountPassword(self, accntName, resourceName='DA-BCGWDLV-WHSE'):
        '''
        Given an account name will do a search for the 
        account.  The account search will not consider 
        case. (upper or lower)
        '''
        resId = self.getResourceId(resourceName)
        if not resId:
            msg = 'Unable to retrieve a resource id in pmp for the resource name {0} using the token {1}'
            msg = msg.format(resourceName, self.token)
            raise ValueError, msg
        accntId = self.getAccountId(accntName, resId)
        psswd = None
        try:
            psswd = self.getAccountPasswordWithAccountId(accntId, resId)
        except ValueError:
            msg = 'PMP response for the resource ({0}) and account ({1}) was {2}. ' + \
                  'which indicates the token used ({3}) does not have permissions to ' + \
                  'access this password'
            msg = msg.format(resourceName, accntName, psswd, self.token) 
            raise ValueError, msg
        return psswd
    
    def getAccountPasswordWithAccountId(self, accntId, resourceId):
        url = 'https://' + self.baseUrl + self.restDir + \
              'resources/' + str(resourceId) + '/accounts/' + \
              str(accntId) + '/password'
        tokenDict = self.getTokenDict()
        print 'url:', url
        r = requests.get(url, params=tokenDict, verify=False)
        passwdStruct = r.json()
        psswd =  passwdStruct['operation']['Details']['PASSWORD']
        if psswd.upper() == '[Request]'.upper():
            msg = 'PMP response for the resource ID ({0}) and account ID ({1}) was {2}. ' + \
                  'which indicates the token used ({3}) does not have permissions to ' + \
                  'access this password'
            msg = msg.format(resourceId, accntId, psswd, self.token) 
            raise ValueError, msg
        return psswd
        


