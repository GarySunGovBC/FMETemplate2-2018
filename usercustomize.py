'''
Created on Dec 14, 2015

@author: kjnether
'''
import site
import platform
import os.path
print 'running usercustomize...'
if platform.node() == 'matar':
    print 'adding matar internal lib paths...'
    matarPath = r'\\data.bcgov\work\scripts\python\DataBCPyLib'
    site.addsitedir(matarPath)
    print '{0} path has been added'.format(matarPath)
    
# adding the lib_ext path
rootDir = os.path.dirname(__file__)
lib_extPath = os.path.join(rootDir, 'lib_ext')
site.addsitedir(lib_extPath)