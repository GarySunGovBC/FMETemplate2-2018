'''
Created on Jul 28, 2015

@author: kjnether

keyless access to BCDC rest api
'''
import requests
import urlparse

class BCDCRestQuery():
    
    destKeyDict = {'PROD': r'https://catalogue.data.gov.bc.ca', 
                   'TEST': r'https://cat.data.gov.bc.ca', 
                   'DELIV': r'https://cad.data.gov.bc.ca' }
    
    def __init__(self, destKeyWord='PROD'):
        destKeyWord = destKeyWord.upper()
        if not self.destKeyDict.has_key( destKeyWord ):
            msg = 'Invalid destKeyWord provided  "{0}", valid valued include {1}'
            msg = msg.format(destKeyWord, ','.join(self.destKeyDict.keys()))
            raise ValueError, msg
        self.baseUrl = self.destKeyDict[destKeyWord]
        self.apiDir = 'api/3/action'
        
    def executeRequest(self, url, params):
        r = requests.get(url, params=params)
        print 'request status', r.status_code
        print 'request json', r.json()
        retVal = {}
        if r.status_code == 200:
            retVal = r.json()
        else:
            msg = 'Rest call returned a status code of {0}.  url called: ' + \
                  '{1} params are: {2}'
            msg = msg.format(r.status_code, url, str(params))
            # TODO: should either define a better exception, or search and see if 
            #       one is provided with requests.
            raise ValueError, msg
        return retVal
        
    def getResource(self, field, queryString):
        # {{DOMAIN}}api/3/action/resource_search?query=url:nrsmajorprojectstracking.xls
        method = 'resource_search'
        url = urlparse.urljoin(self.baseUrl, self.apiDir) + '/'
        url = urlparse.urljoin(url, method)
        #print 'url', url
        query = {'query': field.strip() + ':' + queryString.strip()}
        #print 'query', query
        response = self.executeRequest(url, query)
        # now verify that the request returned success
        if not response['success']:
            msg = 'Rest Request did not return success, response is: {0}'.format(str(response))
            # TODO: Again define a better error, just getting things working for now
            raise ValueError, msg
        # delete the help from the response
        del response['help']
        return response
        #'resource_search?query=url:nrsmajorprojectstracking.xls
        
    def getRevision(self, revisionID):
        # {{DOMAIN}}api/3/action/revision_show?id=0a5eb345-2ba8-4ad7-9a25-9a864cef5dce
        method = 'revision_show'
        url = urlparse.urljoin(self.baseUrl, self.apiDir) + '/'
        url = urlparse.urljoin(url, method)
        query = {'id': revisionID}
        print 'query', query
        response = self.executeRequest(url, query)
        return response
        # now verify that the request returned success
        