import os.path
import os
import sys
print 'running local usercustomize'
curDir = os.path.dirname(__file__)
libDir = os.path.join(curDir, '..', '..', 'lib')
libDir = os.path.normpath(libDir)
sys.path.insert(0, libDir)
