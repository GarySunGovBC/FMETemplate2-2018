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


# 
class Start(DataBCFMWTemplate.DefaultStart):
    
    def __init__(self, fme):
        DataBCFMWTemplate.DefaultStart.__init__(self, fme)
        # line above inherits the functionality of the default startup.
        # this way if you don't implement a required method it will 
        # default to the default version.
        print 'using this script', __file__
        
    # override default startup
    def startup(self):
        '''
        This is where the custom startup code would go
        '''
        print 'using this startup!'
        
        
        
    