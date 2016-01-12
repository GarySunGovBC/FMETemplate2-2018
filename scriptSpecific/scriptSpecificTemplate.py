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

class Start():
    
    def __init__(self, fme):
        self.fme = fme
        # put any specific class instantiation here.
        pass
        
    def Start(self):
        # This is the code that will get called by the startup 
        # routine for this script
        pass
    
class Shutdown():
    
    def __init__(self, fme):
        self.fme = fme
        pass
    
    def shutdown(self):
        pass
    