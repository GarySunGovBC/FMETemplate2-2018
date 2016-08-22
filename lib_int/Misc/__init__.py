import os.path
import sys
import site

externalPath = os.path.join(os.path.dirname(__file__), '..', 'externalLibs')
externalPath = os.path.normpath(externalPath)
site.addsitedir(externalPath)
sys.path.insert(0, sys.path[len(sys.path) - 1])