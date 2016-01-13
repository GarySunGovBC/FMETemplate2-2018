'''
Created on Jan 12, 2016

Purpose is to wrap fme log calls into the python 
logger api, so that log calls that use standard 
python loggin methods will appear in the fme 
log.

follows is a couple links on custom loggers:
http://pantburk.info/?blog=77


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
            #msg = self.format(record)
            logMessage = record.getMessage()
            self.fmeLogger.logMessageString(logMessage)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
            
class FMEShutdownLogger(logging.Handler):
    
    def __init__(self, logFileName):
        logging.Handler.__init__(self)
        self.logFileName = logFileName
        self.logFH = open(self.logFileName,'a')
        
    def emit(self, record):
        try:
            #msg = self.format(record.message)
            #self.fmeLogger.logMessageString(record.message)
            logMessage = record.getMessage()

            self.logFH.write(logMessage)
            #logger.close()

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
            
    def close(self):
        self.logFH.close()
        
