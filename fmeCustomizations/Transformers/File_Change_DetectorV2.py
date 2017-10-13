'''
Created on Jan 13, 2016

@author: kjnether

what follows is the code that was extracted from
the transformer:
'''
import logging
import os.path

import ChangeDetectLib2
import DataBCFMWTemplate
import fme  # @UnresolvedImport pylint: disable=import-error


class ChangeFlagFetcher(object):
    '''
    This class implements the abstract structure required by a python executor
    transformer.  It contains the logic that is used in the change detector
    transformer
    '''

    def __init__(self):
        # determine if there are changes:
        # 1 - gets the path to the changelog from the DataBCFMWTemplate lib
        # 2 - reads the sources for the current fmw by looking for
        #     src_dataset% in the macro values
        # 3 - reads the change log and retrieves the last time the
        #     file changed
        # 4 - compares with current file date, if current is more
        #     recent then marks for update.
        #
        # setting up logging
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Logging set up in the module: " + \
                          str(os.path.basename(__file__)))

        # get macro values
        self.fmeMacroValues = fme.macroValues

        # get template constants
        self.const = DataBCFMWTemplate.TemplateConstants

        # name of the fmw being run
        self.fmwFileName = self.fmeMacroValues[self.const.FMWMacroKey_FMWName]

        # make sure there is a change detection parameter, and then
        # capture it
        self.fileChngKey = self.const.FMWParams_FileChangeEnabledParam
        if not self.fmeMacroValues.has_key(self.fileChngKey):
            msg = 'File change detection requires you to define a parameter called ' + \
                  '{0}.  This parameter does not exist.  Add it as a published parameter ' + \
                  'to your FMW.\nType:   Choice\nprompt:  Whether to use file change ' + \
                  'detection or not\nConfiguration:   TRUE%FALSE\nDefault Value:  TRUE'
            msg = msg.format(self.fileChngKey)
            self.logger.error(msg)
            raise ValueError, msg
        self.changeDetectionEnabledParam = self.fmeMacroValues[self.fileChngKey]
        self.logger.debug("changeDetectionEnabledParam: %s", self.changeDetectionEnabledParam)

        # create ChangeLogFilePath object
        #   is a databc node?
        self.paramObj = DataBCFMWTemplate.TemplateConfigFileReader(
            self.fmeMacroValues[self.const.FMWParams_DestKey])
        changeLogFileName = self.paramObj.getChangeLogFile()
        if self.paramObj.isDataBCNode():
            changeLogRootDir = self.paramObj.getChangeLogsDirFullPath()
        else:
            changeLogRootDir = self.fmeMacroValues[self.const.FMWMacroKey_FMWDirectory]
        # This parameter looks strange: self.changeDetectionEnabledParam  should
        # probably be
        self.changeLogFilePath = ChangeDetectLib2.ChangeLogFilePath(
            changeLogRootDir, changeLogFileName, self.fmwFileName,
            self.const.ConfFileSection_global_changeLogDir)

        # create change detect object, all future transactions should
        # be managed through this object.  Should be no need
        # for any other change detection objects
        self.chng = ChangeDetectLib2.ChangeDetect(self.changeLogFilePath)
        self.chng.setDateStrFormat(self.const.FMELogDateFormatString)

        self.logger.debug("changelib object created")

        # get the run environment (DLV|TST|PRD) from DEST_DB_ENV_KEY
        self.destDbEnvKey = None
        if self.const.FMWParams_DestKey in self.fmeMacroValues:
            self.destDbEnvKey = self.fmeMacroValues[self.const.FMWParams_DestKey]
        self.logger.debug("completed the init of the ChangFlagFetcher")

    def input(self, feature):
        '''
        Required by the fme pythonCaller.  This method is where the code should
        go that does the actual work associated with the transformer.
        '''
        # making sure that the file change detection global parameter has
        # been created.  This parameter can be used to turn change detection
        # on and off each time the job is run.  This parameter must exist
        # self.logger.debug("input called")

        # atribNames = feature.getAllAttributeNames()
        # fmeDatasetRaw = feature.getAttribute('fme_dataset')
        # fmeDataset = self.chng.formatDataSet(fmeDatasetRaw)
        # self.changeCache.addFeatureIn(fmeDataset)

        # get the run environment (dlv|tst|prd) if it exists / is defined
        # in the fmw macro values
        fmeDatasetRaw = feature.getAttribute('fme_dataset')
        # print 'fmeDatasetRaw', fmeDatasetRaw

        # indicates that change detection has been disabled
        if self.changeDetectionEnabledParam.upper() == 'FALSE':
            feature.setAttribute('CHANGE_DETECTED', 'TRUE')
            self.chng.addFeatureChange(fmeDatasetRaw, self.destDbEnvKey)
            # self.logger.debug("CHANGE DISABLED")
        elif self.chng.hasChanged(fmeDatasetRaw, self.destDbEnvKey):
            feature.setAttribute('CHANGE_DETECTED', 'TRUE')
            self.chng.addFeatureChange(fmeDatasetRaw, self.destDbEnvKey)
            # self.logger.debug("CHANGE: TRUE")
        else:
            feature.setAttribute('CHANGE_DETECTED', 'FALSE')
            # self.logger.debug("CHANGE: FALSE")
        # ignore this problem, of pyoutput not being defined.  It is available
        # when the code is run through the pythonCaller transformer
        self.pyoutput(feature)  # pylint: disable=no-member

    def close(self):
        '''
        Required method by the pythonCaller transformer.  Code in here is the
        last code to be executed by the transformer.
        '''
        fileChngKey = self.const.FMWParams_FileChangeEnabledParam

        # fileChngKey = self.changeCache.const.FMWParams_FileChangeEnabledParam
        changeDetectionEnabledParam = self.fmeMacroValues[fileChngKey]
        self.logger.debug("changeDetectionEnabledParam: %s",
                          changeDetectionEnabledParam)

        # for each input feature, check the
        self.logger.debug("closing the file change detector")

        # fmwName = self.fmeMacroValues[FMWMacroKey_FMWName
        self.chng.updateFileChangeLog(self.fmwFileName)
