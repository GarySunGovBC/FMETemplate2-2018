'''
Created on Sep 21, 2016

@author: kjnether

This module that the BCDCFileChangeDetector transformer is linked to.
The python caller in that linked transformer is tied to the class

BCDCFileChangeDetector.ChangeFlagFetcher

__init__ called on first feature into the transformer
input() method is called per feature
close() when the last feature has exited the transformer

'''
import logging
import os.path

import ChangeDetectLib2
import File_Change_DetectorV2  # @UnresolvedImport


class ChangeFlagFetcher(File_Change_DetectorV2.ChangeFlagFetcher):
    '''
    inheriting from the ChangeFlagFetcher class, but overriding the default
    constructor.

    This allows everything else that happens in the change flag fetcher to
    remain unchanged.  All we need to do is replace the 'chng' property
    with a BCDC change fetcher that implements the same api as the one in the
    default ChangeDetectLib2.ChangeDetect module
    '''

    def __init__(self):
        # inherits from the base class
        File_Change_DetectorV2.ChangeFlagFetcher.__init__(self)

        # setting up the logging
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Logging set up in the module: %s",
                          os.path.basename(__file__))

        # override / replace the changedetect object defined in the
        # File_Change_DetectorV2.ChangeFlagFetcher object to
        # use the BCDC versions. overrides how the change dates are
        # retrieved
        self.chng = ChangeDetectLib2.ChangeDetectBCDC(self.changeLogFilePath)
        self.chng.setDateStrFormat(self.const.FMELogDateFormatString)
        self.logger.debug("BCDC Change detector object created")
