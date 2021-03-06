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
# does the devpaths.json file exist if so read it and get the dev paths
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
    subDirs = ['lib', 'fmeCustomizations/Transformers', '']
    for subDir in subDirs:
        pth = os.path.join(dir2Add, subDir)
        sys.path.insert(0, pth)
        # print 'added path: {0}'.format(pth)
    # While developing kirk!  Once dev is complete this module will become part of
    # the dependencies.
    tmpKirkUtilPath = r'Z:\Workspace\kjnether\proj\DataBCPyLib\KirkUtil'
    sys.path.insert(0, pth)
    if platform.architecture()[0] == '64bit':
        pth = os.path.join(dir2Add, '64bit')
        sys.path.insert(0, pth)
        print 'datapaths adding', pth

    # setting env vars for log directory, log for this run will be placed in here
    # in theory
    os.environ['DBC_FRAMEWORK_LOGDIR'] = os.path.join(dir2Add,
                                                      'outputs',
                                                      'log')
    os.environ['DBC_FRAMEWORK_CNGCNTRL'] = os.path.join(dir2Add,
                                                        'outputs',
                                                        'changelogs')
    print 'setting the env var for logging to: ', os.environ['DBC_FRAMEWORK_LOGDIR']
    print 'setting the env var for change logs to: ', os.environ['DBC_FRAMEWORK_CNGCNTRL']

else:
    # now make sure these are at the start of the path list
    # subDirs = ['lib_ext', 'lib_int', 'fmeCustomizations/Transformers', '']
    # lib_intPath = os.path.join(curDir, 'lib_int')
    libdirs = ['lib']
    if platform.architecture()[0] == '64bit':
        libdirs.append('lib64')
        # libdirs.insert(0, 'lib64')
    pathsToAdd = [curDir, os.path.join(curDir, 'fmeCustomizations', 'Transformers')]
    pathsToAdd.extend(libdirs)
    # pathsToAdd = [curDir, os.path.join(curDir, 'lib'), os.path.join(curDir, 'fmeCustomizations', 'Transformers')]
    for pth2Rerder in pathsToAdd:
        pthCntr = 0
        # get the directory this file is in to this file
        rootPath = os.path.dirname(__file__)
        pth2Rerder = os.path.join(rootPath, pth2Rerder)
        # pth2Rerder = os.path.realpath(pth2Rerder)

        sys.path.insert(0, pth2Rerder)
        print 'inserted', pth2Rerder, 'to the front of the pathlist'
        # for curPth in sys.path:
        #    if pth2Rerder == curPth:
        #        del sys.path[pthCntr]
        #        sys.path.insert(0, pth2Rerder)
        #        print 'inserted', pth2Rerder, 'to the front of the pathlist'
        #        break
        #    pthCntr += 1
# print 'pathlist:', sys.path
# print '\n'.join(os.environ.keys())
