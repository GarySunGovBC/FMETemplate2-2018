'''
Created on Sep 29, 2016

@author: kjnether
'''
import logging
import os.path
import shutil
import urlparse

import PMP.PMPRestConnect  # @UnresolvedImport

import ParcelMapLib
import fme  # @UnresolvedImport pylint: disable=import-error


class Constants(object):
    '''
    Defining constants that get used throughout this script.
    '''
    # place to store constants, anything that starts
    # with fme defines the name of published parameter
    fmeDirectoryKey = 'FME_MF_DIR'

    fmeRestAPIBaseURL = 'PARCELMAP_BASE_URL'
    fmeRestAPIUser = 'REST_API_USER'

    fmePMPResource = 'PMP_RESOURCE'
    fmeFGDBReadOnly = 'FGDB_FULL_PATH_READONLY'
    fmeDestinationDirectory = 'DEST_DIR'  # the writeable directory

    parcelMapZipFile = 'ParcelMapBCExtract_fromLTSA.gdb.zip'
    orderStatusFile = 'ParcelMapBCExtract_status.json'
    fingerPrintFile = 'ParcelMapBCExtract.md5'
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
        self.fmeRestAPIBaseURL = '{0}_{1}'.format(prefix, self.fmeRestAPIBaseURL)
        self.fmeRestAPIUser = '{0}_{1}'.format(prefix, self.fmeRestAPIUser)
        self.fmePMPResource = '{0}_{1}'.format(prefix, self.fmePMPResource)
        self.fmeDestinationDirectory = '{0}_{1}'.format(prefix, self.fmeDestinationDirectory)
        self.fmeFGDBReadOnly = '{0}_{1}'.format(prefix, self.fmeFGDBReadOnly)

class ParcelMapUpdater(object):
    '''
    implements the pycaller transformers required api
    '''

    def __init__(self):
        # logging setup
        self.logger = logging.getLogger(__name__)
        self.logger.debug(u"Logging set up in the module: " + str(os.path.basename(__file__)))

        # loading constants
        self.const = Constants()

        # This is the parameter that will get updated if the data has
        # been deemed to have changed and is updated.
        self.updated = False

        # get the fme published parameters
        self.fmeMacroValues = fme.macroValues
        self.logger.debug(u"completed the init of %s", self.__class__.__name__)
        self.cnt = 0
        # use the published parameters to update the constants
        # see method call for more details
        self.updateConstants()

        self.logger.debug(u"            %s", self.fmeMacroValues)

        # for key in self.fmeMacroValues:
        #    self.logger.debug("            {0}  :  {1}".format(key, self.fmeMacroValues[key]))

        # extracting values from published parameters required to build parcel map connection
        self.logger.debug(u"retrieving the macrovalue: %s", self.const.fmeRestAPIUser)
        restApiURL = self.fmeMacroValues[self.const.fmeRestAPIBaseURL]

        # get various published parameter values used by the linked transformer.
        restApiUser = self.fmeMacroValues[self.const.fmeRestAPIUser]
        self.logger.debug(u'restApiUser: %s', restApiUser)
        restApiPswd = self.getRestApiPassword(user=restApiUser)

        # getting the DEST_DIR parameter passed to the linked transformer
        destDir = self.fmeMacroValues[self.const.fmeDestinationDirectory]

        # once downloaded we will either replace this file with the downloaded
        # version or if they are the same delete the downloaded file
        destFileName = self.const.parcelMapZipFile  # just the file name of the zip file
        destFullPath = os.path.join(destDir, destFileName)  # calc full path
        destFullPath = os.path.normcase(os.path.normpath(destFullPath))  # normalize it
        self.logger.debug(u"destFullPath %s", destFullPath)
        # creating the parcel map object
        self.logger.debug(u"creating parcel map api object")
        self.pm = ParcelMapLib.parcelMapAPI(restApiURL, restApiUser, restApiPswd, destDir)

        # if a status file exists it indicates that the process never completed
        # if self.pm.existsStatusFile():
        #    # now check to see if the destination
        #    util = ParcelMapLib.ParcelMapUtil()
        #    statusFile = self.pm.getStatusFile()
        #    self.logger.debug("status file is {0}".format(statusFile))
        #    parcelMapOrder = util.readStatusFile(statusFile)
        #    orderId = parcelMapOrder.getOrderId()
        #    expectedCompletionDate = parcelMapOrder.getExpectedDate()
        #    self.pm.orderId = orderId
        #    file2Download = self.pm.getDestinationFilePath()
        #    if not os.path.exists(file2Download):
        #    # extract the data from the statusFileData data
        #        self.logger.debug("file2Download {0} does not exist".format(file2Download))
        #        self.pm.monitorAndCompleteOrder(orderId, expectedCompletionDate)
        # else:
            # this method just will place the order, monitor it and
            # download.
        #    self.logger.debug("placing a new order for the province")
        #    self.pm.downloadCannedBC()
            # self.pm.downloadBC()
        # now do the comparison.
        self.logger.debug(u"placing a new order for the canned version of the province")
        self.pm.downloadCannedBC()

        self.logger.debug(u"destFullPath %s", destFullPath)
        self.logger.debug(u"fingerPrintFile %s", self.const.fingerPrintFile)

        # currentOrderFile is something like parcelmap_10732.gdb.zip
        currentOrderFile = self.pm.getDestinationFilePath()
        fingerPrintFile = os.path.join(destDir, self.const.fingerPrintFile)
        fp = ParcelMapLib.FingerPrinting(currentOrderFile, fingerPrintFile)
        if fp.hasParcelFabricChanged():
            # yes it has changed!  Delete the orginal version, then
            # call the unzip to recreate it.
            fgdbPathReadOnly = self.fmeMacroValues[self.const.fmeFGDBReadOnly]
            zipFileDownloaded = self.pm.getDestinationFilePath()
            fgdbPathReadWrite = os.path.join(self.pm.destDir, os.path.basename(fgdbPathReadOnly))
            self.logger.debug(u"fgdbPathReadWrite: %s", fgdbPathReadWrite)
            if os.path.exists(fgdbPathReadWrite):
                self.logger.debug(u"Deleting the FGDB %s so it can be " + \
                                  u"re-created from the zip file",
                                  fgdbPathReadWrite)
                shutil.rmtree(fgdbPathReadWrite)
            self.pm.unZipPackagedProduct(zipFileDownloaded, fgdbPathReadWrite)
            self.updated = True
            # finally update the cache
            fp.cacheFingerPrint()
            if os.path.exists(destFullPath):
                self.logger.debug(u"removing %s", destFullPath)
                os.remove(destFullPath)
            shutil.move(zipFileDownloaded, destFullPath)
        else:
            # The versions have not changed so don't bother downloading.
            zipFileDownloaded = self.pm.getDestinationFilePath()
            if os.path.exists(zipFileDownloaded):
                self.logger.debug(u"removing %s", zipFileDownloaded)
                os.remove(zipFileDownloaded)

        # once order has been placed, retrieved, compared, unzipped, then
        # proceed with deleting the statusfile.
        # next step is to delete the status file
        self.pm.deleteStatusFile()

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
        '''
        Retrieves passwords required by the script from pmp using information in
        the constants class
        '''
        self.logger.debug(u"getting password for the user %s", user)
        # fmwDir = os.path.normcase(os.path.normpath(
        # self.fmeMacroValues[self.const.fmeDirectoryKey]))
        pmpResource = self.fmeMacroValues[self.const.fmePMPResource]

        # assemble the full url to the parcelmap api
        pmConst = ParcelMapLib.Constants()
        self.logger.debug(u"retrieved the parcel map path: %s", pmConst.restPath)
        pmUrl = self.fmeMacroValues[self.const.fmeRestAPIBaseURL]
        pmUrl = urlparse.urljoin(pmUrl, pmConst.restPath)
        self.logger.debug(u"parcel map api url is %s", pmUrl)

        util = ParcelMapLib.ParcelMapUtil()
        pmpToken = util.getPMPDict(self.fmeMacroValues)
        self.logger.debug(u"Trying to create the pmp object")
        pmp = PMP.PMPRestConnect.PMP(pmpToken)
        self.logger.debug(u"created the pmp object")
        # password = pmp.getAccountPassword(user, resourceName=pmpResource)
        self.logger.debug(u"sending %s,%s, %s", user, pmUrl, pmpResource)
        password = pmp.getRestAPIPassword(user, pmUrl, pmpResource)

        self.logger.debug(u"got the password for %s", user)
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
        feature.setAttribute(self.const.updateAttribute, updateAttibuteStr)
        # ignore the pyoutput errors as this method will become available when
        # the script is run through a python caller transformer
        self.pyoutput(feature)  # pylint: disable=no-member

    def close(self):
        '''
        Required method for a class that is to be implemented by the pycaller
        transformer
        '''
        self.logger.info(u'completed the parcelmap update')
