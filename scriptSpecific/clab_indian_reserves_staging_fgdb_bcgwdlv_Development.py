'''
This module contains various classes that 
if inserted into the correct directory will 
allow for overriding of default functionality
provided in the DataBCFMWTemplate

To get your FMW to use this script place it 
either in the directory described in the 
config files global section in the
parameter customizescriptdir

or in the current directory.  If the override
exists in both directories the fmw directory
will take precedence.

@author: kjnether
'''
import DataBCFMWTemplate
import logging


# 
class Start(DataBCFMWTemplate.DefaultStart):
    
    def __init__(self, fme):
        # inheriting from the default startup
        DataBCFMWTemplate.DefaultStart.__init__(self, fme)
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        # line above inherits the functionality of the default startup.
        # this way if you don't implement a required method it will 
        # default to the default version.
        self.logger.debug("using the custom startup for the module {0}".format(__file__))
        
    # override default startup
    def startup(self):
        '''
        This is where the custom startup code would go, 
        
        '''
        # this method is currently overriding the default class
        
        # if you wanted to call the default startup routine 
        # you could do that with the following line:
        # super(Start, self).startup()
        self.logger.debug("Runing a custom startup for {0}".format(__file__))

        
        
        
    