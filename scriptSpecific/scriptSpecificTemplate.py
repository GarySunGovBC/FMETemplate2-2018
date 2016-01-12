'''
 
Steps for customized startup / shutdown

a) copy this script and rename to the same name as the 
   fmw but with a .py suffix
b) if you want to only customize the startup then delete
   the Shutdown class, if you only want a custom shutdown 
   then delete the Start class
c) put the customized steps for your startup in the
   Start.startup method, cutomized shutdown in the 
   Shutdown.shutdown() method.
 
'''
import inspect
import sys
import os
import logging

class Start():
    
    def __init__(self, fme):
        self.fme = fme
        self.__initLogging()
        # put any specific class instantiation here.
        pass
        
    def Start(self):
        # This is the code that will get called by the startup 
        # routine for this script
        pass
    
    def __initLogging(self):
        curFile = inspect.getfile(inspect.currentframe())  
        if curFile == '<string>':
            curFile = sys.argv[0]
        logName = os.path.splitext(os.path.basename(curFile))[0] + '.' + self.__class__.__name__
        # and this line creates a log message
        logger = logging.getLogger(logName)
        logger.debug("new debug message from dependant module")
          
class Shutdown():
    
    def __init__(self, fme):
        self.fme = fme
        pass
    
    def __initLogging(self):
        curFile = inspect.getfile(inspect.currentframe())  
        if curFile == '<string>':
            curFile = sys.argv[0]
        logName = os.path.splitext(os.path.basename(curFile))[0] + '.' + self.__class__.__name__
        # and this line creates a log message
        logger = logging.getLogger(logName)
        logger.debug("new debug message from dependant module")
    
    def shutdown(self):
        pass
    