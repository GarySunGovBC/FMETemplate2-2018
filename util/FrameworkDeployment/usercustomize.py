import os.path
import os
import sys
print 'running local usercustomize'
curDir = os.path.dirname(__file__)
print 'curDir', curDir
libDir = os.path.join(curDir, '..', '..', 'lib')
libDir = os.path.normpath(libDir)
rootDir = os.path.join(curDir, '..', '..')
rootDir = os.path.normpath(rootDir)
print 'libdir', libDir
print 'rootDir', rootDir
sys.path.insert(0, rootDir)
sys.path.insert(0, libDir)
