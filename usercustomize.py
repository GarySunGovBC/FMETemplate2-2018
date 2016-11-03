'''
Created on Dec 14, 2015

@author: kjnether
'''
import site
import platform
import os.path
import sys
print 'RUNNING USERCUSTOMIZE {0}'.format(__file__)
#print 'platform.node()', platform.node()
rootDir = os.path.dirname(__file__)
#print 'ROOTDIR: {0}'.format(rootDir)
if platform.node().lower() == 'matar':
    #print 'adding matar internal lib paths...'
    #Z:\scripts\python\DataBCPyLib\PMP
    #matarPath = r'\\data.bcgov\work\scripts\python\DataBCPyLib'
    #site.addsitedir(matarPath)
    #print '{0} path has been added'.format(matarPath)
    pass

lib_extPath = os.path.join(rootDir, 'lib_ext')
lib_intPath = os.path.join(rootDir, 'lib_int')
requestsPackages = os.path.join(rootDir, 'lib_ext', 'requests', 'packages' )
requestsPackages2 = os.path.join(rootDir, 'lib_ext', 'requests', 'packages' , 'urllib3', 'packages')

pathsToAdd = [rootDir, lib_intPath, lib_extPath]

# now add the paths
for pth in pathsToAdd:
    site.addsitedir(pth)

# now make sure these are at the start of the path list
for pth2Rerder in pathsToAdd:
    pthCntr = 0
    for curPth in sys.path:
        if pth2Rerder == curPth:
            del sys.path[pthCntr]
            sys.path.insert(0, pth2Rerder)
            print 'inserted', pth2Rerder, 'to the front of the pathlist'
            break
        pthCntr += 1


#print 'lib_extPath', lib_extPath
#print 'lib_intPath', lib_intPath
#site.addsitedir(lib_extPath)
#site.addsitedir(lib_intPath)
#site.addsitedir(rootDir)

#newPaths = [lib_extPath, lib_intPath, rootDir]

# Grab the last three paths that were added to the end of the path list
# and stick them in the front of the path list.


#print sys.path
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
