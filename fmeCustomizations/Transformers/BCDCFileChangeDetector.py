'''
Created on Sep 21, 2016

@author: kjnether
'''
import File_Change_DetectorV2
import ChangeDetectLib
import logging
import os.path

class ChangeFlagFetcher(File_Change_DetectorV2.ChangeFlagFetcher):
    
    def __init__(self):
        File_Change_DetectorV2.ChangeFlagFetcher.__init__(self)
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Logging set up in the module: " + str(os.path.basename(__file__)))
        self.chng = ChangeDetectLib.ChangeDetectBCDC(self.fmeMacroValues)
        self.changeCache = self.chng.readChangeLog()
