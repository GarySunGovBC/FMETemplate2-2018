'''
Created on Sep 29, 2016

@author: kjnether
'''
import FMELogger
import fme  # @UnresolvedImport
import logging.config
import sys
import os.path
import urlparse
import ParcelMapLib
import PMP.PMPRestConnect

class Constants(object):
    # place to store constants, anything that starts 
    # with fme defines the name of published parameter
    fmeDirectoryKey = 'FME_MF_DIR'
    
    fmeRestAPIBaseURL = 'PARCELMAP_BASE_URL'
    fmeRestAPIUser = 'REST_API_USER'
    
    fmePMPResource = 'PMP_RESOURCE'
    fmeFGDBReadOnly = 'FGDB_FULL_PATH_READONLY'
    fmeDestinationDirectory = 'DEST_DIR'
    
    parcelMapZipFile = 'parcelmap_bc.zip'
    downloadSrid = 'EPSG:3153'
    # populated later.  All macro values will use this prefix
    macroPrefix = None
    
    updateAttribute = 'PARCELMAP_UPDATED'
    
    def updateParamsWithPrefix(self, prefix):
        self.fmeRestAPIBaseURL = '{0}_{1}'.format(prefix,self.fmeRestAPIBaseURL)
        self.fmeRestAPIUser = '{0}_{1}'.format(prefix,self.fmeRestAPIUser)
        self.fmePMPResource = '{0}_{1}'.format(prefix,self.fmePMPResource)
        self.fmeDestinationDirectory = '{0}_{1}'.format(prefix,self.fmeDestinationDirectory)
        self.fmeFGDBReadOnly = '{0}_{1}'.format(prefix,self.fmeFGDBReadOnly)
        
class ParcelMapUpdater(object):
    
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Logging set up in the module: " + str(os.path.basename(__file__)))
        
        self.const = Constants()
        
        # This is the parameter that will get updated if the data has
        # been deemed to have changed and is updated. 
        self.updated = False
        
        self.fmeMacroValues = fme.macroValues
        self.logger.debug("completed the init of {0}".format(self.__class__.__name__))
        self.cnt = 0
        self.updateConstants()

        self.logger.debug("            {0} ".format( self.fmeMacroValues))

        for key in self.fmeMacroValues:

            self.logger.debug("            {0}  :  {1}".format(key, self.fmeMacroValues[key]))
        
        # parameters required to build parcel map connection
        self.logger.debug("retrieving the macrovalue: {0}".format(self.const.fmeRestAPIUser))
        restApiURL = self.fmeMacroValues[self.const.fmeRestAPIBaseURL]

        restApiUser = self.fmeMacroValues[self.const.fmeRestAPIUser]
        self.logger.debug('restApiUser: {0}'.format(restApiUser))
        restApiPswd = self.getRestApiPassword(user=restApiUser)
        destDir = self.fmeMacroValues[self.const.fmeDestinationDirectory]
        destFileName = self.const.parcelMapZipFile
        destFullPath = os.path.join(destDir, destFileName)
        destFullPath = os.path.normcase(os.path.normpath(destFullPath))
        self.logger.debug("creating parcel map api object")
        self.pm = ParcelMapLib.parcelMapAPI(restApiURL, restApiUser, restApiPswd)
        # debugging, uncomment this line and remove the call to requestOrderData
        self.pm.downloadBC(destFullPath)
        #self.pm.requestOrderData(10648, destFullPath)
        # now do the comparison
        fp = ParcelMapLib.FingerPrinting(destFullPath)
        if fp.hasParcelFabricChanged():
            # yes it has changed, need to unzip it
            fgdbPathReadOnly = self.fmeMacroValues[self.const.fmeFGDBReadOnly]
            self.pm.unZipFile(destFullPath, fgdbPathReadOnly)
            self.updated = True
            # finally update the cache
            fp.cacheFingerPrint()
            
    def updateConstants(self):
        # will be used to update constants wiht the prefix used 
        # by the custom transformer
        util = ParcelMapLib.ParcelMapUtil()
        prefix = util.getCustomTransformerPrefix(self.fmeMacroValues)
        self.const.updateParamsWithPrefix(prefix)
        
    def getRestApiPassword(self, user):
        self.logger.debug("getting password for the user {0}".format(user))
        fmwDir = os.path.normcase(os.path.normpath(self.fmeMacroValues[self.const.fmeDirectoryKey]))
        pmpResource = self.fmeMacroValues[self.const.fmePMPResource]
        
        # assemble the full url to the parcelmap api
        pmConst = ParcelMapLib.Constants()
        self.logger.debug("retrieved the parcel map path: {0}".format(pmConst.restPath))
        pmUrl = self.fmeMacroValues[self.const.fmeRestAPIBaseURL]
        pmUrl = urlparse.urljoin(pmUrl, pmConst.restPath)
        self.logger.debug("parcel map api url is {0}".format(pmUrl))
        
        util = ParcelMapLib.ParcelMapUtil()
        pmpToken = util.getPMPDict(self.fmeMacroValues)
        self.logger.debug("Trying to create the pmp object")
        pmp = PMP.PMPRestConnect.PMP(pmpToken)
        self.logger.debug("created the pmp object")
        #password = pmp.getAccountPassword(user, resourceName=pmpResource)
        password = pmp.getRestAPIPassword(user,pmUrl,pmpResource)

        self.logger.debug("got the password for {0}".format(user))
        return password
        
    def input(self, feature):
        self.cnt += 1
        updateAttibuteStr = str(self.updated)
        updateAttibuteStr = updateAttibuteStr.upper()
        self.const.updateAttribute
        feature.setAttribute(self.const.updateAttribute, updateAttibuteStr)
        self.pyoutput(feature)
        
    def close(self):
        self.logger.info('completed the parcelmap update')
    