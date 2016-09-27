'''
Created on Jan 13, 2016

@author: kjnether

what follows is the code that was extracted from 
the transformer:
'''

import ChangeDetectLib
import FMELogger
import fme  # @UnresolvedImport
import logging.config
import sys
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
        if not self.fmeMacroValues.has_key(fileChngKey):
            msg = 'File change detection requires you to define a parameter called ' + \
                  '{0}.  This parameter does not exist.  Add it as a published parameter ' + \
                  'to your FMW.\nType:   Choice\nprompt:  Whether to use file change ' + \
                  'detection or not\nConfiguration:   TRUE%FALSE\nDefault Value:  TRUE'
            msg = msg.format(fileChngKey)
            self.logger.error(msg)
            raise ValueError, msg
        changeDetectionEnabledParam = self.fmeMacroValues[fileChngKey]
        
        #self.logger.debug("changeDetectionEnabledParam: {0}".format(changeDetectionEnabledParam))
        #self.logger.debug("Changelib location: {0}".format(ChangeDetectLib.__file__))

        atribNames = feature.getAllAttributeNames()
        fmeDatasetRaw = feature.getAttribute('fme_dataset')
        #self.logger.debug("FME_DATASET extracted straight from the feature: {0}".format(fmeDatasetRaw))
        fmeDataset = self.chng.formatDataSet(fmeDatasetRaw)
        #self.logger.debug("FME_DATASET modified / normalized {0}".format(fmeDataset))
        self.changeCache.addFeatureIn(fmeDataset)
        
        # indicates that change detection has been disabled
        if changeDetectionEnabledParam.upper() == 'FALSE':
            feature.setAttribute('CHANGE_DETECTED', 'TRUE')
            self.changeCache.addFeatureChange(fmeDataset)
            #self.logger.debug("CHANGE DISABLED")
        elif self.changeCache.hasChanged(fmeDataset):
            feature.setAttribute('CHANGE_DETECTED', 'TRUE')
            self.changeCache.addFeatureChange(fmeDataset)
            #self.logger.debug("CHANGE: TRUE")
        else:
            feature.setAttribute('CHANGE_DETECTED', 'FALSE')
            #self.logger.debug("CHANGE: FALSE")
        self.pyoutput(feature)
        
    def close(self):
        fileChngKey = self.changeCache.const.FMWParams_FileChangeEnabledParam
        changeDetectionEnabledParam = self.fmeMacroValues[fileChngKey]

        # for each input feature, check the
        self.logger.debug("closing the file change detector")
        self.changeCache.updateFileChangeLog(changeDetectionEnabledParam)

