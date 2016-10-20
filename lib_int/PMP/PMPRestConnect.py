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
import pprint
import urlparse

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
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(accnts)
        
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
    
    def getAccountDetails(self, accntId, resourceId):
        # resources/303/accounts/307?AUTHTOKEN=B9A1809A-5BF7-4459-9ED2-8D4F499CB902
        url = 'https://' + self.baseUrl + self.restDir + \
              'resources/' + str(resourceId) + '/accounts/' + \
              str(accntId)
        tokenDict = self.getTokenDict()
        print 'url:', url
        r = requests.get(url, params=tokenDict, verify=False)
        accntDtls = r.json()
        return accntDtls
    
    def getRestAPIPassword(self, accountName, apiUrl, ResourceName):
        '''
        Currently we have a resource in pmp that stores rest api usernames
        and passwords.  The standard for storing them is to store the username
        as the account name.  The path to the rest service then gets stored
        in the "Account Notes".  When passwords are stored in this way this 
        method should be able to retrieve them.
        
        How it works:
          a) accountName received if it includes @http://rest.api...
             the account name and the url are separated
          b) retrieves the accounts for the resourceName
          c) iterates through each account, if the account name includes
             a url it is separated from the username
              - bare usernames are compared for a match, ie just the user
                name supplied as an arg and just the user name in the iteration
                of accounts for the resourceName
              - if usernames match then retrieve the url from the "Account Notes"
                if the url matches the received apiUrl then get the password and 
                return it for that account.
              - if the the "account notes" url does not match and there was a 
                url appended to the end of the username example user@http://blah
                then that url is compared with the supplied url.
                if they match then get th password.
                
              - if neither of the url's match then raises and error saying there
                is no account for the username url combination         
                
        :param  accountName: The name account name who's password we want
        :type accountName: str
        
        :param  apiUrl: The url to the rest api that corresponds with the username.
        :type apiUrl: str
        
        :param  ResourceName: The name of the resource in pmp who's password we are 
                              trying to retrieve.
        :type ResourceName: str
        '''
        self.logger.debug("params are {0}, {1}, {2}".format(accountName, apiUrl, ResourceName))
        
        parsed_uri = urlparse.urlparse( apiUrl )
        apiUrl = parsed_uri.netloc
        self.logger.debug("apiUrl: {0}".format(apiUrl))

        # checking to see if the syntax for the account is
        # username@url or if it is just username
        #   example 'apiuser'
        #   or can come as 'apiuser@https://blah.com/blah/blah/blah'
        if '@' in accountName:
            justUser, apiUrlfromAccntName = accountName.split('@')
            # only comparing the domain right now
            parsed_uri = urlparse.urlparse( apiUrlfromAccntName )
            apiUrlfromAccntName = parsed_uri.netloc
            self.logger.debug("apiUrlfromAccntName: {0}".format(apiUrlfromAccntName))
        else:
            justUser = accountName
            apiUrlfromAccntName = None
        #print 'justUser', justUser
        #print 'apiUrl', apiUrl
        #pp = pprint.PrettyPrinter(indent=4)
        
        # a get the resource id
        resId = self.getResourceId(ResourceName)
        
        # now get all the accounts for that resource using a heuristic 
        # to search for the userName.
        # start by getting all the accounts for the userid
        accnts = self.getAccountsForResourceID(resId)
        # will be where the extracted account name is stored assuming it is found
        extractedAccntId = None
        for accnt in accnts:
            #pp.pprint(accnt)
            # splitting up the received account name and url in case
            # the version stored in pmp is username@url
            if '@' in accnt['ACCOUNT NAME']:
                currAccntName, currAccntUrl = accnt['ACCOUNT NAME'].split('@')
                parsed_uri = urlparse.urlparse( currAccntUrl )
                currAccntUrl = parsed_uri.netloc
                self.logger.debug("currAccntUrl: {0}".format(currAccntUrl))
            else:
                currAccntName = accnt['ACCOUNT NAME']
                currAccntUrl = None
            currAccntId = accnt['ACCOUNT ID']
            # if the usernames match, next we want to check if the 
            # urls matches
            if currAccntName.lower() == justUser.lower():
                # get the details for the account and pull the url 
                # from the details field
                details = self.getAccountDetails(currAccntId, resId)
                # Making sure there is a details field, and if there is
                # putting the contents into urlFromDetails
                if ((details.has_key('operation')) and \
                     details['operation'].has_key('Details') ) and \
                     details['operation']['Details'].has_key('DESCRIPTION'):
                    urlFromDetails = details['operation']['Details']['DESCRIPTION']
                    parsed_uri = urlparse.urlparse( urlFromDetails )
                    urlFromDetails = parsed_uri.netloc
                    self.logger.debug("urlFromDetails: {0}".format(urlFromDetails))

                    # now if the urlFromDetails matches apiUrl provided as an arg
                    # then assume this is the account
                    if urlFromDetails.lower().strip() == apiUrl.lower().strip():
                        extractedAccntId = currAccntId
                        break
                # otherwise check to see if the url in the account name in 
                # pmp matches the url sent as an arg
                elif currAccntUrl:
                    if apiUrl.lower().strip() ==  currAccntUrl.lower().strip():
                        extractedAccntId = currAccntId
                        break
        if not extractedAccntId:
            self.logger.debug("account name: {0}".format(accountName))
            msg = 'unable to find an account that matches the account name: {0} and ' + \
                  'the resource name {1}'
            msg = msg.format(accountName, ResourceName)
            raise AccountNotFound, msg
        
        return self.getAccountPasswordWithAccountId(extractedAccntId, resId)
            
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
        
class AccountNotFound(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)

