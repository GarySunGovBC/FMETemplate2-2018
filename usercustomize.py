'''
Created on Dec 14, 2015

@author: kjnether
'''
import site
import platform
import os.path
import sys
print 'running usercustomize...'
print 'platform.node()', platform.node()
rootDir = os.path.dirname(__file__)
print 'ROOTDIR: {0}'.format(rootDir)
if platform.node().lower() == 'matar':
    #print 'adding matar internal lib paths...'
    #Z:\scripts\python\DataBCPyLib\PMP
    #matarPath = r'\\data.bcgov\work\scripts\python\DataBCPyLib'
    #site.addsitedir(matarPath)
    #print '{0} path has been added'.format(matarPath)
    pass
    
# adding the lib_ext path
lib_extPath = os.path.join(rootDir, 'lib_ext')
lib_intPath = os.path.join(rootDir, 'lib_int')
site.addsitedir(lib_extPath)
site.addsitedir(lib_intPath)
site.addsitedir(rootDir)

# Grab the last three paths that were added to the end of the path list
# and stick them in the front of the path list.
sys.path.insert(0, sys.path.pop())
sys.path.insert(0, sys.path.pop())
sys.path.insert(0, sys.path.pop())

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
