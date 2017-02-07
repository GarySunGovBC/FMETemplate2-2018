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
#else:
#    dir2Add = os.path.dirname(__file__)
    #['lib_ext', 'lib_int', 'fmeCustomizations/Transformers', '']

if dir2Add:
    # subdirs 
    subDirs = ['lib_ext', 'lib_int', 'fmeCustomizations/Transformers', '']
    for subDir in subDirs:
        pth = os.path.join(dir2Add, subDir)
        sys.path.insert(0, pth)
        print 'added path: {0}'.format(pth)
    




# # delete all this once this is working
# rootDir = os.path.dirname(__file__)
# if platform.node().lower() == 'matar':    
#     # comment these out once development is complete
#     devPath = r'\\data.bcgov\work\Workspace\kjnether\proj\FMETemplateRevisions_Python'
#     #devPath = r'Z:\Workspace\kjnether\proj\FMETemplateRevisions_Python'
#     lib_ext = os.path.join(devPath, 'lib_ext')
#     lib_int = os.path.join(devPath, 'lib_int')
#     #matarPath = r'\\data.bcgov\work\scripts\python\DataBCPyLib'
#     
#     #site.addsitedir(devPath)
#     #site.addsitedir(lib_ext)
#     #site.addsitedir(lib_int)
#     sys.path.insert(0, devPath)
#     sys.path.insert(1, lib_ext)
#     sys.path.insert(2, lib_int)
#     
#     #print '{0} path has been added'.format(devPath)
#     print 'sys.path', sys.path
#     pass

else:
    # now make sure these are at the start of the path list
    #subDirs = ['lib_ext', 'lib_int', 'fmeCustomizations/Transformers', '']
    #lib_intPath = os.path.join(curDir, 'lib_int')
    pathsToAdd = [curDir, os.path.join(curDir, 'lib_int'), os.path.join(curDir, 'lib_ext'), os.path.join(curDir, 'fmeCustomizations', 'Transformers')]
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
#print 'pathlist:', sys.path


#sys.path.insert(0, sys.path.pop())
#sys.path.insert(0, sys.path.pop())
#sys.path.insert(0, sys.path.pop())
#print sys.path

# development should be commented out later
# pth2Remove = r'\\data.bcgov\work\scripts\python\DataBCFmeTemplate2'   
# # While debugging remove the path to \\data.bcgov\work\scripts\python\DataBCFmeTemplate2
# number2Remove = 0
# for cnt in range(0, len(sys.path)):
#     pth = sys.path[cnt]
#     #print pth
#     if pth == pth2Remove:
#         number2Remove += 1
# print '----------------------------'
# if number2Remove:
#     for i in range(0, number2Remove):
#         sys.path.remove(pth2Remove)
#         #print 'PATH REMOVED', pth2Remove


    
#print 'END OF THE USERCUSTOMIZE'
