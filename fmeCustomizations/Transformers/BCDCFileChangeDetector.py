'''
Created on Sep 21, 2016

@author: kjnether
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
        File_Change_DetectorV2.ChangeFlagFetcher.__init__(self)

        # modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Logging set up in the module: " + str(os.path.basename(__file__)))

        self.chng = ChangeDetectLib2.ChangeDetectBCDC(self.changeLogFilePath)
        self.chng.setDateStrFormat(self.const.FMELogDateFormatString)
        self.logger.debug("BCDC Change detector object created")
