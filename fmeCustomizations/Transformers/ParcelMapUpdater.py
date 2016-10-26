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
import shutil

class Constants(object):
    # place to store constants, anything that starts 
    # with fme defines the name of published parameter
    fmeDirectoryKey = 'FME_MF_DIR'
    
    fmeRestAPIBaseURL = 'PARCELMAP_BASE_URL'
    fmeRestAPIUser = 'REST_API_USER'
    
    fmePMPResource = 'PMP_RESOURCE'
    fmeFGDBReadOnly = 'FGDB_FULL_PATH_READONLY'
    fmeDestinationDirectory = 'DEST_DIR'
    
    parcelMapZipFile = 'ParcelMapBCExtract_fromLTSA.gdb.zip'
    orderStatusFile = 'ParcelMapBCExtract_status.json'
    downloadSrid = 'EPSG:3153'
    # populated later.  All macro values will use this prefix
    macroPrefix = None
    
    updateAttribute = 'PARCELMAP_UPDATED'
    
    def updateParamsWithPrefix(self, prefix):
        '''
        FME prepend linked transformer published parameter names with the name of the 
        transformer.  This method gets the prepend string and then calculates the 
        published parameter names.  Makes it easier to work with them.  After this
        method has been run we can always retrieve the variable name from this 
        constant object.
        
        Example for fmeRestAPIBaseURL, to get the value we would say something like
        
        fmeMacroValues[constantsObject.fmeRestAPIBaseURL]
        
        :param  prefix: The name of the prefix that fme will attach to references 
                        of the parcel map linked transformer.
        :type prefix: str
        '''
        self.fmeRestAPIBaseURL = '{0}_{1}'.format(prefix,self.fmeRestAPIBaseURL)
        self.fmeRestAPIUser = '{0}_{1}'.format(prefix,self.fmeRestAPIUser)
        self.fmePMPResource = '{0}_{1}'.format(prefix,self.fmePMPResource)
        self.fmeDestinationDirectory = '{0}_{1}'.format(prefix,self.fmeDestinationDirectory)
        self.fmeFGDBReadOnly = '{0}_{1}'.format(prefix,self.fmeFGDBReadOnly)
        
class ParcelMapUpdater(object):
    
    def __init__(self):
        # logging setup
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Logging set up in the module: " + str(os.path.basename(__file__)))
        
        # loading constants
        self.const = Constants()
        
        # This is the parameter that will get updated if the data has
        # been deemed to have changed and is updated. 
        self.updated = False
        
        # get the fme published parameters
        self.fmeMacroValues = fme.macroValues
        self.logger.debug("completed the init of {0}".format(self.__class__.__name__))
        self.cnt = 0
        # use the published parameters to update the constants  
        # see method call for more details
        self.updateConstants()

        self.logger.debug("            {0} ".format( self.fmeMacroValues))

        for key in self.fmeMacroValues:

            self.logger.debug("            {0}  :  {1}".format(key, self.fmeMacroValues[key]))
        
        # extracging values from published parameters required to build parcel map connection
        self.logger.debug("retrieving the macrovalue: {0}".format(self.const.fmeRestAPIUser))
        restApiURL = self.fmeMacroValues[self.const.fmeRestAPIBaseURL]
        
        # get various published parameter values used by the linked transformer.
        restApiUser = self.fmeMacroValues[self.const.fmeRestAPIUser]
        self.logger.debug('restApiUser: {0}'.format(restApiUser))
        restApiPswd = self.getRestApiPassword(user=restApiUser)
        # getting the DEST_DIR parameter passed to the linked transformer
        destDir = self.fmeMacroValues[self.const.fmeDestinationDirectory]
        # once downloaded we will either replace this file with the downloaded
        # version or if they are the same delete the downloaded file
        destFileName = self.const.parcelMapZipFile # just the file name of the zip file
        destFullPath = os.path.join(destDir, destFileName) # calc full path
        destFullPath = os.path.normcase(os.path.normpath(destFullPath)) # normalize it
        
        # creating the parcel map object
        self.logger.debug("creating parcel map api object")
        self.pm = ParcelMapLib.parcelMapAPI(restApiURL, restApiUser, restApiPswd, destDir)
        
        # if a status file exists it indicates that the process never completed
        if self.pm.existsStatusFile():
            util = ParcelMapLib.ParcelMapUtil()
            parcelMapOrder = util.readStatusFile()
            
            orderId = parcelMapOrder.getOrderId()
            expectedCompletionDate = parcelMapOrder.getExpectedDate()
            # extract the data from the statusFileData data
            self.pm.monitorAndCompleteOrder(orderId, expectedCompletionDate)
            self.pm.deleteStatusFile()
        else:
            # this method just will place the order, monitor it and 
            # download.
            self.pm.downloadBC()
        
        newlyDownloadedParcelMapData = self.pm.getDestinationFilePath()
        # once we get here we have two zip files, the one we just downloaded
        # and the one that was previously downloaded.
        # now going to just delete the version that we might already have 
        # and replace it with the new one.
        if os.path.exists(destFullPath):
            os.remove(destFullPath)
        shutil.move(newlyDownloadedParcelMapData, destFullPath)
            
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
        '''
        The published parameters of a linked transformer will have the name of the 
        linked transformer pre-pended to the names of these paraemters to avoid 
        naming conflicts between transformer parameters and the actual fme workspace
        that is using the transformer.  (this is just something that fme does)
        
        This method will extract the prepend string and pass that to the  
        updateParamsWithPrefix method.  That method will then calculate a
        bunch of parameter names with the prepended string, so we don't need 
        to worry about this problem.        
        '''
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
        self.logger.debug("sending {0},{1}, {2}".format(user,pmUrl,pmpResource))
        password = pmp.getRestAPIPassword(user,pmUrl,pmpResource)

        self.logger.debug("got the password for {0}".format(user))
        return password
        
    def input(self, feature):
        '''
        This is the method that is called for every feature in the input data 
        set.  The parcelmap updater is designed to be used with a creator
        that creates a dummy feature.  This method does not actually do 
        anything.  It returns the feature with an additional attribute that 
        could be used to indicate whether the parcel map updater actually 
        updated the dataset, and therefor the feature.
        
        :param  feature: input fme feature
        :type feature: fme.feature (see fme objects for feature object)
        '''
        
        self.cnt += 1
        updateAttibuteStr = str(self.updated)
        updateAttibuteStr = updateAttibuteStr.upper()
        self.const.updateAttribute
        feature.setAttribute(self.const.updateAttribute, updateAttibuteStr)
        self.pyoutput(feature)
        
    def close(self):
        self.logger.info('completed the parcelmap update')
    