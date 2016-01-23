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
    
    def __init__(self, *args, **kwargs):
        logging.Handler.__init__(self)
        self.fmeLogger = fmeobjects.FMELogFile()  # @UndefinedVariable
        #self.fmeLogger.logMessageString('starting the startup object, writing to the log')
        severityDict = {logging.DEBUG: fmeobjects.FME_STATISTIC, 
                        logging.NOTSET: fmeobjects.FME_INFORM}
        
    def flush(self):
        pass
    
    def getSeverity(self, logLevel):
        # next step is to map the severity
        # options for fme severities are:
        #   - fmeobjects.FME_INFORM
        #   - fmeobjects.FME_WARN
        #   - fmeobjects.FME_ERROR
        #   - fmeobjects.FME_FATAL
        #   - fmeobjects.FME_STATISTIC
        #   - fmeobjects.FME_STATUSREPORT

        severity = fmeobjects.FME_INFORM
        if logLevel == logging.DEBUG:
            severity = fmeobjects.FME_INFORM
        elif logLevel == logging.WARNING:
            severity = fmeobjects.FME_WARN
        elif logLevel == logging.ERROR:
            severity = fmeobjects.FME_ERROR
        elif logLevel == logging.CRITICAL:
            severity = fmeobjects.FME_FATAL
        return severity
    
    def emit(self, record):
        try:
            #print 'record', record
            logLevel = record.levelno
            #print 'log level', logLevel
            formattedRecord = self.format(record)
            severity = self.getSeverity(logLevel)
            #print 'severity', severity
            self.fmeLogger.logMessageString(formattedRecord, severity)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
            
class FMEShutdownLogger(logging.Handler):
    
    def __init__(self, logFileName, *args, **kwargs):
        logging.Handler.__init__(self)
        print 'logFileName in logger', logFileName
        self.logFileName = logFileName
        self.logFH = open(self.logFileName,'a')
        
    def emit(self, record):
        try:
            #msg = self.format(record.message)
            #self.fmeLogger.logMessageString(record.message)
            formattedRecord = self.format(record)

            #logMessage = record.getMessage() + '\n'
            self.logFH.write(formattedRecord + "\n")
            #logger.close()

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
            
    def close(self):
        self.logFH.close()
        
