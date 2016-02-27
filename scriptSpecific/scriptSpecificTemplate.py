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
#import DataBCFMWTemplate

class Start():
    
    def __init__(self, fme):
        self.fme = fme
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        # put any specific class instantiation here.
        pass
        
    def Start(self):
        # This is the code that will get called by the startup 
        # routine for this script, put any custom startup routines
        # that apply to your specific script here
        pass
          
class Shutdown():
    
    def __init__(self, fme):
        self.fme = fme
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        # any custom shutdown initiation code goes here

    def shutdown(self):
        # custom shutdown methods go here.
        # you probably want to make sure that you are calling the DWM 
        # writer.  This could be easily enabled by inheriting from 
        # the default shutdown and then call the super classes
        # shutdown.  You can then append your functionality onto the end.
        pass
    