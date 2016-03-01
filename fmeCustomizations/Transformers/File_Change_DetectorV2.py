'''


Created on Jan 13, 2016

@author: kjnether


what follows is the code that was extracted from 
the transformer:

# class to retrieve a flag associated with the input feature's dataset
# name that indicates whether or not the dataset has changed since the
# last successful replication
import pyfme
import __main__ # FME_MacroValues are part of the __main__ namespace
import string
from string import upper

global FME_MacroValues

class changeFlagFetcher(object):
    def __init__(self):
        pass
    def input(self, feature):
        
        # dataset name is index to source file dictionary
        fmeDataset = feature.getAttribute('fme_dataset')
        
        # standardize use of \ in full path
        dataset = fmeDataset.replace('/','\\')
        
        # retrieve dictionary
        sourceFileDictionary = __main__.changes
        
        # dictionary is keyed on dataset name; 
        # corresponding value is python list ['True'|'False', File Date]
        sourceList = sourceFileDictionary[dataset]
        
        # first item in the list is 'True'|'False' indicator for change
        changeIndicator = upper(str(sourceList[0]))
        
        # create a new attribute with the true/false value
        feature.setAttribute('CHANGE_DETECTED', changeIndicator)
        
        self.pyoutput(feature)
        
    def close(self):
        pass

'''
import ChangeDetectLib
import FMELogger
import fme  # @UnresolvedImport
import logging.config
import os.path

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
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Logging set up in the module: " + str(os.path.basename(__file__)))
        
        self.fmeMacroValues = fme.macroValues
        self.chng = ChangeDetectLib.ChangeDetect(self.fmeMacroValues)
        self.logger.debug("changelib object created")
        self.changeCache = self.chng.readChangeLog()
        self.featuresIn = 0
        #directory = self.fmeMacroValues[self.chng.const.FMWMacroKey_FMWDirectory]
        directory = os.path.dirname(__file__)

        #logfileName = self.fmeMacroValues[self.chng.const.FMWMacroKey_FMWName]
        #logfileName = os.path.basename(__file__)
        #logSuffix = self.chng.const.FMELogFileSuffix
        #logfileNoSuffix, suffix = os.path.splitext(logfileName)
        
        #logconfigFile = os.path.join(directory, logfileNoSuffix + logSuffix)
        # TODO: add a if exists handler
        #print 'logconfigFile', logconfigFile
        
        self.logger.debug("completed the init of the ChangFlagFetcher")
    
    def input(self, feature):
        # script is currently set up to read the source 
        # macro values to determine what the source data 
        # is.  This is not entirely necessary, could 
        # also just get this from 
        # the feature attributes
        # fme_feature_type --:-- CLAB_INDIAN_RESERVES 
        # fme_dataset --:-- \\data.bcgov\data_staging_ro\BCGW\administrative_boundaries\Federal_IRs.gdb
        # probably a better way of doing it!
        #print 'type of feature', type(feature)
        #print 'feature', feature
        fileChngKey = self.changeCache.const.FMWParams_FileChangeEnabledParam
        changeDetectionEnabledParam = self.fmeMacroValues[fileChngKey]
        
        #self.logger.debug("changeDetectionEnabledParam: {0}".format(changeDetectionEnabledParam))
        
        atribNames = feature.getAllAttributeNames()
        fmeDataset = feature.getAttribute('fme_dataset')
        self.changeCache.addFeatureIn(fmeDataset)
        
        # indicates that change detection has been disabled
        if changeDetectionEnabledParam.upper() == 'FALSE':
            feature.setAttribute('CHANGE_DETECTED', 'TRUE')
            self.changeCache.addFeatureChange(fmeDataset)
        elif self.changeCache.hasChanged(fmeDataset):
            feature.setAttribute('CHANGE_DETECTED', 'TRUE')
            self.changeCache.addFeatureChange(fmeDataset)
        else:
            feature.setAttribute('CHANGE_DETECTED', 'FALSE')
        self.pyoutput(feature)
        
    def close(self):
        fileChngKey = self.changeCache.const.FMWParams_FileChangeEnabledParam
        changeDetectionEnabledParam = self.fmeMacroValues[fileChngKey]

        # for each input feature, check the
        self.logger.debug("closing the file change detector")
        self.changeCache.updateFileChangeLog(changeDetectionEnabledParam)
