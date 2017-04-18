'''
Created on Dec 14, 2015

@author: kjnether
'''
import site
import platform
import os.path
import sys
import getpass
import json

# notify that usercustomize is being run
print 'RUNNING USERCUSTOMIZE {0}'.format(__file__)

# get the devpaths.json file
curDir = os.path.dirname(__file__)
configDirName = 'config'
devpathsFileName = 'devpaths.json'
path2devpathsJson = os.path.join(curDir, configDirName, devpathsFileName)

expectedKeys = ['template_dev_directory', 'host', 'username']
dir2Add = None
if os.path.exists(path2devpathsJson):
    fh = open(path2devpathsJson, 'r')
    struct = json.load(fh)
    fh.close()
    
    proceed = False
    # make sure the expected keys exist
    for expectKey in expectedKeys:
        if not expectKey in struct:
            proceed = False
            break
        
    currentUser = getpass.getuser()
    node = (platform.node()).lower()
    if node.lower() == struct['host'].lower() and \
       currentUser.lower() == struct['username']:
        dir2Add = struct['template_dev_directory']
        dir2Add = os.path.realpath(dir2Add)

if dir2Add:
    # subdirs 
    #subDirs = ['lib_ext', 'lib_int', 'fmeCustomizations/Transformers', '']
    subDirs = ['lib', 'fmeCustomizations/Transformers', '']
    for subDir in subDirs:
        pth = os.path.join(dir2Add, subDir)
        sys.path.insert(0, pth)
        print 'added path: {0}'.format(pth)
else:
    # now make sure these are at the start of the path list
    #subDirs = ['lib_ext', 'lib_int', 'fmeCustomizations/Transformers', '']
    #lib_intPath = os.path.join(curDir, 'lib_int')
    pathsToAdd = [curDir, os.path.join(curDir, 'lib'), os.path.join(curDir, 'fmeCustomizations', 'Transformers')]
    for pth2Rerder in pathsToAdd:
        pthCntr = 0
        pth2Rerder = os.path.realpath(pth2Rerder)
        sys.path.insert(0, pth2Rerder)
        print 'inserted', pth2Rerder, 'to the front of the pathlist'
        #for curPth in sys.path:
        #    if pth2Rerder == curPth:
        #        del sys.path[pthCntr]
        #        sys.path.insert(0, pth2Rerder)
        #        print 'inserted', pth2Rerder, 'to the front of the pathlist'
        #        break
        #    pthCntr += 1
print 'pathlist:', sys.path

