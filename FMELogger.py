'''
Created on Jan 12, 2016

Purpose is to wrap fme log calls into the python 
logger api, so that log calls that use standard 
python loggin methods will appear in the fme 
log.

@author: kjnether
'''
import logging
import logging.handlers
import fmeobjects

class FMELogHandler(logging.Handler):
    
    def __init__(self):
        logging.Handler.__init__(self)
        self.fmeLogger = fmeobjects.FMELogFile()  # @UndefinedVariable
        #self.fmeLogger.logMessageString('starting the startup object, writing to the log')
        
    def flush(self):
        pass
    
    def emit(self, record):
        try:
            msg = self.format(record)
            self.fmeLogger.logMessageString(record.message)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
            
            