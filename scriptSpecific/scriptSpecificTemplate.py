'''
 
Steps for customized startup / shutdown

a) copy this script and rename to the same name as the 
   fmw but with a .py suffix
b) if you want to only customize the startup then delete
   the Shutdown class, if you only want a custom shutdown 
   then delete the Start class
c) put the customized steps for your startup in the
   Start.startup method, customized shutdown in the 
   Shutdown.shutdown() method.
 
'''
import DataBCFMWTemplate
import DBCFMEConstants
import sys
import os
import logging

class Start(DataBCFMWTemplate.DefaultStart):
    
    def __init__(self, fme):
        # inheriting from the default startup
        DataBCFMWTemplate.DefaultStart.__init__(self, fme)
        
        # constants available in self.const. then using to contruct the extended
        # startup logger.  Name of the logger is important as the name is what 
        # ties it to the log configuration in the log config file.
        self.const= DBCFMEConstants.TemplateConstants()
        self.logger = logging.getLogger(self.const.LoggingExtendedLoggerName)
        
        self.fme = fme

        # put any custom class instantiation here
        pass
        
    def Start(self):
        # start off by calling the default start up routine
        self.logger.info("running the default startup routine")
        super(Start, self).startup()
        
        # here is where you can put your extended startup routine
        pass
          
class Shutdown():
    
    def __init__(self, fme):
        DataBCFMWTemplate.DefaultShutdown.__init__(self, fme)
        
        # constants available in self.const. then using to contruct the extended
        # startup logger.  Name of the logger is important as the name is what 
        # ties it to the log configuration in the log config file.
        self.const = DBCFMEConstants.TemplateConstants()
        self.logger = logging.getLogger(self.const.LoggingExtendedLoggerName)
        
        self.fme = fme
        
        # any custom shutdown initiation code goes here
        pass

    def shutdown(self):
        # custom shutdown methods go here.
        # you probably want to make sure that you are calling the DWM 
        # writer.  This could be easily enabled by inheriting from 
        # the default shutdown and then call the super classes
        # shutdown.  You can then append your functionality onto the end.
        self.logger.info("running the default startup routine")
        super(Shutdown, self).shutdown()
        # custom code goes below here
        self.logger.info("running custom shutdown routine")
        pass
    