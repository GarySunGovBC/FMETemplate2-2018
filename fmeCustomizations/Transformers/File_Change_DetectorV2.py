'''
Created on Jan 13, 2016

@author: kjnether

what follows is the code that was extracted from 
the transformer:
'''

import ChangeDetectLib2
import FMELogger
import fme  # @UnresolvedImport
import logging.config
import sys
import os.path
import DataBCFMWTemplate

class ChangeFlagFetcher(object):
    
    def __init__(self):
        # determine if there are changes:
        # 1 - gets the path to the changelog from the DataBCFMWTemplate lib
        # 2 - reads the sources for the current fmw by looking for 
        #     src_dataset% in the macro values
        # 3 - reads the change log and retrieves the last time the 
        #     file changed
        # 4 - compares with current file date, if current is more
        #     recent then marks for update.
        #if FME_MacroValues:
        #    self.fmeMacroValues = FME_MacroValues  # @UndefinedVariable
        #else:
        print 'using this dev version'
        
        #setting up logging
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Logging set up in the module: " + str(os.path.basename(__file__)))
        
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
        print 'changeDetectionEnabledParam', self.changeDetectionEnabledParam
        
        
        # create ChangeLogFilePath object
        #   is a databc node?
        self.paramObj = DataBCFMWTemplate.TemplateConfigFileReader(self.fmeMacroValues[self.const.FMWParams_DestKey])
        changeLogFileName = self.paramObj.getChangeLogFile()
        if self.paramObj.isDataBCNode():
            changeLogRootDir = self.paramObj.getChangeLogsDirFullPath()
        else:
            changeLogRootDir = self.fmeMacroValues[self.const.FMWMacroKey_FMWDirectory]
        self.changeLogFilePath = ChangeDetectLib2.ChangeLogFilePath(changeLogRootDir, changeLogFileName, self.fmwFileName, self.changeDetectionEnabledParam)

        
        # create change detect object, all future transactions should 
        # be managed through this object.  Should be no need
        # for any other change detection objects
        self.chng = ChangeDetectLib2.ChangeDetect(self.changeLogFilePath)
        self.chng.setDateStrFormat(self.const.FMELogDateFormatString)
        
        
        self.logger.debug("changelib object created")
        
        # read the log file
        #self.changeCache = self.chng.readChangeLog()
        #self.logger.debug("returned from reading the log file")
        #self.featuresIn = 0
        #directory = self.fmeMacroValues[self.chng.const.FMWMacroKey_FMWDirectory]
        #directory = os.path.dirname(__file__)
        
        # get the run environment (DLV|TST|PRD) from DEST_DB_ENV_KEY
        self.destDbEnvKey = None
        if self.const.FMWParams_DestKey in self.fmeMacroValues:
            self.destDbEnvKey = self.fmeMacroValues[self.const.FMWParams_DestKey]
        self.logger.debug("completed the init of the ChangFlagFetcher")
        
    def input(self, feature):
        # making sure that the file change detection global parameter has
        # been created.  This parameter can be used to turn change detection
        # on and off each time the job is run.  This parameter must exist
        #self.logger.debug("input called")
        
        #atribNames = feature.getAllAttributeNames()
        #fmeDatasetRaw = feature.getAttribute('fme_dataset')
        #fmeDataset = self.chng.formatDataSet(fmeDatasetRaw)
        #self.changeCache.addFeatureIn(fmeDataset)
        
        # get the run environment (dlv|tst|prd) if it exists / is defined
        # in the fmw macro values
        fmeDatasetRaw = feature.getAttribute('fme_dataset')
        print 'fmeDatasetRaw', fmeDatasetRaw
        
        # indicates that change detection has been disabled
        if self.changeDetectionEnabledParam.upper() == 'FALSE':
            feature.setAttribute('CHANGE_DETECTED', 'TRUE')
            self.chng.addFeatureChange(fmeDatasetRaw, self.destDbEnvKey)
            self.logger.debug("CHANGE DISABLED")
        elif self.chng.hasChanged(fmeDatasetRaw, self.destDbEnvKey):
            feature.setAttribute('CHANGE_DETECTED', 'TRUE')
            self.chng.addFeatureChange(fmeDatasetRaw, self.destDbEnvKey)
            self.logger.debug("CHANGE: TRUE")
        else:
            feature.setAttribute('CHANGE_DETECTED', 'FALSE')
            self.logger.debug("CHANGE: FALSE")
        self.pyoutput(feature)
        
    def close(self):
        fileChngKey = self.const.FMWParams_FileChangeEnabledParam

        #fileChngKey = self.changeCache.const.FMWParams_FileChangeEnabledParam
        changeDetectionEnabledParam = self.fmeMacroValues[fileChngKey]

        # for each input feature, check the
        self.logger.debug("closing the file change detector")
        
        #fmwName = self.fmeMacroValues[FMWMacroKey_FMWName
        self.chng.updateFileChangeLog(self.fmwFileName)
        