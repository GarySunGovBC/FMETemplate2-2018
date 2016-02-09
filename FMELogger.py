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
import os.path
import fmeobjects

class FMELogHandler(logging.Handler):
    
    def __init__(self, *args, **kwargs):
        print 'started init fo fmelog handler'
        logging.Handler.__init__(self)
        self.fmeLogger = fmeobjects.FMELogFile()  # @UndefinedVariable
        #self.fmeLogger.logMessageString('starting the startup object, writing to the log')
        severityDict = {logging.DEBUG: fmeobjects.FME_STATISTIC, 
                        logging.NOTSET: fmeobjects.FME_INFORM}
        print 'finished'
        
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
    '''
    Trying to centralize the logging setup in a logging config file.
    The result is that the template will init the logging at a stage
    in the fmw where it should be using the FMELogHandler.  Only once
    the script has finished and is in the shutdown mode should this
    handler get used.
    
    Thus the init doesn't do much except cache the log file that 
    it should be writing to.  Then in the emit method the handler
    will look to see if the filehandle has been created.  If it has
    then it gets re-used otherwise it gets created.
    
    This should allow the fileconfig to initialized this logger but 
    won't create the possible dangerous situation where there are 
    two filehandles on the same file.
    
    '''
    
    def __init__(self, *args, **kwargs):
        self.logFileName = logging.logFileName  # @UndefinedVariable
        self.logFH = None
        logging.Handler.__init__(self)
        
    def emit(self, record):
        try:
            if not self.logFH:
                if os.path.exists(self.logFileName):
                    self.logFH = open(self.logFileName,'a')
                    print 'opened the log file'
            if self.logFH:
                #msg = self.format(record.message)
                formattedRecord = self.format(record)
                #logMessage = record.getMessage() + '\n'
                self.logFH.write(formattedRecord + "\n")

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
            
    def close(self):
        if self.logFH:
            self.logFH.close()
        
