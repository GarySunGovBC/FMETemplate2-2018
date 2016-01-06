import pyfme
import __main__ # FME_MacroValues are part of the __main__ namespace
import string
import sys
import os
sys.path.append(r".\bin")

import fmeDataBCWhse

global FME_MacroValues

# workbench file name is required for logging
FME_MacroValues['FMW'] = FME_MappingFileId

try:
   os.stat(FME_MacroValues['FME_MF_DIR']+'\outputs')
except:
    os.mkdir(FME_MacroValues['FME_MF_DIR']+'\outputs')
  

try:
   os.stat(FME_MacroValues['FME_MF_DIR']+'\outputs\\audit')
except:
    os.mkdir(FME_MacroValues['FME_MF_DIR']+'\outputs\\audit')  
  
# instantiate a python helper with workbench attributes and log location
myWorkbench = fmeDataBCWhse.workbench(FME_MacroValues,FME_MacroValues['FME_MF_DIR']+'\outputs\\audit')


# detect changes in source files (option for file-based sources)
myWorkbench.detectFileChanges()

# add resulting change detection attributes to the __main__ namespace
__main__.changes = myWorkbench.sourceFilesChanged