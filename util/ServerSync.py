'''
Created on May 9, 2016


idea is this script can be run to take any changes that have been 
implemented in the template code and deploy it to server.

@author: kjnether
'''

import os.path
import posixpath
import FMEUtil.PyFMEServerV2
import sys
import time
import logging

class ConfigLogging(object):
    
    def __init__(self):
        basename = os.path.basename(__file__)
        fileName = '{0}.log'.format(os.path.splitext(basename)[0])
        print 'fileName', fileName
        logFormatString= r'%(asctime)s - %(lineno)s - %(name)s - %(levelname)s - %(message)s'
#         logging.basicConfig(level=logging.DEBUG,
#                     format=logFormatString,
#                     handlers=[logging.FileHandler(fileName),
#                               logging.StreamHandler()])
        logging.basicConfig(
             filename=fileName,
             level=logging.DEBUG, 
             format= logFormatString,
             datefmt='%H:%M:%S'
         )
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.addFilter(LogFilter())
        formatter = logging.Formatter(logFormatString)
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        #logging.basicConfig(filename=fileName,level=logging.DEBUG)
#         rootLogger = logging.getLogger('root')
#         rootHandler = logging.StreamHandler()
#         rootHandler.setLevel(logging.DEBUG)
#         rootFormatter = logging.Formatter()
#         rootHandler.setFormatter(rootFormatter)
#         rootLogger.addHandler(rootHandler)
        logger = logging.getLogger(__name__)
        logger.debug("TEST TEST TEST")
        logger.info("TEST INFO TEST INFO")
        
class LogFilter(logging.Filter):
    
    def filter(self, record):
        retVal = True
        srchStrings = [r'requests.packages.urllib3.connectionpool']
        for srchStr in srchStrings:
            if srchStr in str(record):
                retVal = False
        return retVal

class Params(object):
    
    FmeServerUrl = 'http://fenite.dmz'
    FmeServerAuthKey = '93978b32c457984ac4bb1912ed27e7fcade6e3ec'
    FmeServerRepository = 'GuyLaFleur'
    
    FmeServerResources_EngineDirType = 'FME_SHAREDRESOURCE_ENGINE'
    
    #SourceDirectory = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\templateImplementation\BCGW_REP_SCHEDULED\acdf_ownership_codes_staging_csv_bcgw'
    pythonDirs = ["Plugins", 'python', 'python27']
    templateSourceDir = r'\\data.bcgov\work\scripts\python\DataBCFmeTemplate2'
    
    depsDir = r'lib_ext'
    
    dataBCModulesDir = r'\\data.bcgov\work\scripts\python\DataBCPyLib'
    dataBCModules2Sync = ['DB', 'Misc', 'PMP']
    
class Deploy(object):
    
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        pass
    
class FMWDeployment(object):
    '''
    Methods to copy FMW's to FME server.
    '''
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.params = Params()
        self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
    
    def CopyFMW(self, fmwFile, repoName):
        fmwPath, fmw = os.path.split(fmwFile)
        if not os.path.exists(fmwFile):
            msg = 'The fmw file {0} that you are trying to upload to FME Server does not exist'
            msg = msg.format(fmwFile)
            raise ValueError, msg
        repo = self.fmeServ.getRepository()
        wrkspcs = repo.getWorkspaces(repoName)
        if wrkspcs.exists(fmw):
            repo.updateRepository(repoName, fmwFile)
            wrkspcs.registerWithJobSubmitter(fmw)
        else:
            repo.copy2Repository(repoName, fmwFile)
            wrkspcs.registerWithJobSubmitter(fmw)

class BaseDeployment(object):
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        
    def deploy(self, deploymentList, overWrite):
        for deployment in deploymentList:
            writeFile = False
            
            if overWrite:
                writeFile = True
            elif not self.resource.exists(deployment.dirType, deployment.destFileFullPathList):
                writeFile = True
            else:
                if deployment.isSourceNewer():
                    writeFile = True
            if writeFile:
                deployment.deploy()
            
            
            # set this up so that it does the evaluation for overwriting or not as well
        
class Deployment(object):
    
    def __init__(self, dirType, dirList, srcFile):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.params = Params()
        
        self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
        self.resource = self.fmeServ.getResources()


        # fme resource type example FME_SHAREDRESOURCE_ENGINE
        self.destDirType = dirType
        # fme directory list including file reference
        self.destFileFullPathList = dirList
        self.destFileFullPathStr = '/'.join(self.destFileFullPathList)
        # source file path (windows path)
        self.srcFile = srcFile
        # fme directory list without the file reference
        self.destDirList = self.destFileFullPathList[:-1]
        self.destDirStr = '/'.join(self.destDirList)
        
    def deploy(self):
        self.resource.copyFile(self.destDirType, 
                               self.destDirList, 
                               self.srcFile, 
                               overwrite=True
                               )
        
    def isSourceNewer(self):
        retVal = False
        destDirInfo = self.resource.getResourceInfo(self.dirType, self.destFileFullPathList)
        destModTime = time.strptime(destDirInfo['date'], '%Y-%m-%dT%H:%M:%S')
        srcModTime = time.ctime(os.path.getmtime(self.srcFile))
        if srcModTime > destModTime:
            retVal = True
        return retVal
        
class PythonDeployment(object):
    
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.logger.debug("LOGGING in {0}".format(modDotClass))
        self.params = Params()
        
        self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
        self.resource = self.fmeServ.getResources()
        # specific python modules to be synced to the root directory of fme server
        # resources
        self.templatePythonFiles = ['DataBCFMWTemplate.py','ChangeDetectLib.py','FMELogger.py']
        
        self.templateDestDir = str(posixpath.sep).join(self.params.pythonDirs)
        self.logger.debug("template is {0}".format(self.templateDestDir))
        print 'self.templateDestDir', self.templateDestDir
        
        # external dependencies dir to be synced to fme server
        self.depsDir = os.path.join(self.params.templateSourceDir, self.params.depsDir)
        
    def DeployPython(self):
        '''
        Deploys all the python code required by the fme template.  
        This method will call all the other submethods necessary to 
        copy the template module, external dependencies, dataBC library 
        python modules so they are globally available to python on 
        the fme server that they are deployed to.
        '''
        self.CopyPythonFiles()
        self.CopyPythonDependencies()
        self.CopyDataBCModules()
        
    def DeleteRootDir(self):
        print 'deleting the plugins dir...'
        # should not have to use this so commented out this line, 
        # only implmeented this for debugging.  FME Server should 
        # automatically create may of the directories that are being 
        # deleted which may cause problems, even after they are 
        # recreated.
        #self.resource.deleteDirectory(self.params.FmeServerResources_EngineDirType, ['Plugins'])
        print 'done'
        
    def CopyPythonFiles(self, overwrite=False):
        '''
        Copies the files described in the list self.templatePythonFiles
        to fme server.
        
        Checks to see if the files already exist, and if they do they do not 
        get copied.
        
        :param  overwrite: This boolean parameter can be set to true to overwrite
                           if the files already exist.
        :type overwrite: enter type
        
        '''
        '''
        Describe your method and what it does here ( if multiple 
        lines make sure they are aligned to this margin)
        
        
        '''
        logging.debug("got here now in {0}".format(__name__))
        self.CheckForDirectories()
        for pyFile in self.templatePythonFiles:
            writeFile = False

            src = os.path.join(self.params.templateSourceDir, pyFile)
            dest = posixpath.join(self.templateDestDir, pyFile)
            if overwrite:
                writeFile = True
            elif not self.resource.exists(self.params.FmeServerResources_EngineDirType, dest.split('/')):
                writeFile = True
            else:
                # otherwise if its been updated on the source side since the destination 
                # dump.
                destDirInfo = self.resource.getResourceInfo(self.params.FmeServerResources_EngineDirType, dest.split('/'))
                destModTime = time.strptime(destDirInfo['date'], '%Y-%m-%dT%H:%M:%S')
                srcModTime = time.ctime(os.path.getmtime(src))
                if srcModTime > destModTime:
                    writeFile = True
            if writeFile:
                self.logger.info("Updating the file: {0}".format(os.path.basename(src)))
                self.resource.copyFile(self.params.FmeServerResources_EngineDirType, 
                                       self.templateDestDir.split('/'), 
                                       src, 
                                       overwrite=True
                                       )
                
    def CopyPythonDependencies(self, overwrite=False):
        
        ignoreList = ['.gitignore', 'requirements.txt']
        self.logger.info("files that are configured to be skipped: {0}".format(ignoreList))
        for dirName, subdirList, fileList in os.walk(self.depsDir):
            for curFile in fileList:
                if curFile not in ignoreList:
                    writeFile = False
                    
                    srcFile = os.path.join(dirName, curFile)
                    relpath = os.path.relpath(dirName, self.depsDir)
                    relpath = relpath.replace('\\', '/')
                    destPath = posixpath.join(self.templateDestDir, relpath)
                    destPathWthFile = posixpath.join(destPath, curFile)
                    destPathList = destPath.split('/')
                    
                    if overwrite:
                        writeFile = True
                    elif not self.resource.exists(self.params.FmeServerResources_EngineDirType, destPathWthFile.split('/')):
                        writeFile = True
                    else:
                        destDirInfo = self.resource.getResourceInfo(self.params.FmeServerResources_EngineDirType, destPathWthFile.split('/'))
                        destModTime = time.strptime(destDirInfo['date'], '%Y-%m-%dT%H:%M:%S')
                        srcModTime = time.ctime(os.path.getmtime(srcFile))
                        if srcModTime > destModTime:
                            writeFile = True
                        else:
                            self.logger.debug("skipping the file: {0}".format(srcFile))
                    if writeFile:
                        self.logger.debug("writing to fme server: {0}".format(srcFile))
                        self.resource.copyFile(self.params.FmeServerResources_EngineDirType, 
                                           destPathList, 
                                           srcFile,
                                           overwrite=True, 
                                           createDirectories=True)
    
    
    def CopyCustomTransformers(self):
        customizationDir = 'fmeCustomizations'
        transformersDir = 'Transformers'
        srcPath = os.path.join(self.params.templateSourceDir, customizationDir, transformersDir)
        for dirName, subdirList, fileList in os.walk(srcPath):
            for file2Copy in fileList:
                srcFile = os.path.join(dirName, file2Copy)
                relPath = os.path.relpath(srcFile, os.path.join(self.params.templateSourceDir, customizationDir))
                relPath = relPath.replace("\\", '/')
                dir = posixpath.dirname(relPath)
                print 'relPath', relPath
                print 'copying {0} to {1} on fme server'.format(srcFile, dir)
                dirList = dir.split('/')
                self.resource.copyFile(self.params.FmeServerResources_EngineDirType, 
                                               dirList, 
                                               srcFile,
                                               overwrite=True, 
                                               createDirectories=True)
                                
    def CopyDataBCModules(self, overwrite=False):
        for DataBCMod in self.params.dataBCModules2Sync:
            modPath = os.path.join(self.params.dataBCModulesDir, DataBCMod)
            for dirName, subdirList, fileList in os.walk(modPath):
                del subdirList
                for curFile in fileList:
                    junk, suffix = os.path.splitext(curFile)
                    del junk
                    if suffix <> '.pyc':
                        srcFile = os.path.join(dirName, curFile)
                        relPath = os.path.relpath(modPath, self.params.dataBCModulesDir)
                        relPath = relPath.replace('\\', '/')
                        destPath = posixpath.join(self.templateDestDir, relPath)
                        destPathWithFile = posixpath.join(destPath, curFile)
                        dirList = destPath.split('/')
                        
                        writeFile = False
                        
                        if overwrite:
                            writeFile = True
                        elif not self.resource.exists(self.params.FmeServerResources_EngineDirType, destPathWithFile.split('/')):
                            writeFile = True
                        else:
                            destDirInfo = self.resource.getResourceInfo(self.params.FmeServerResources_EngineDirType, dirList)
                            destModTime = time.strptime(destDirInfo['date'], '%Y-%m-%dT%H:%M:%S')
                            srcModTime = time.ctime(os.path.getmtime(srcFile))
                            if srcModTime > destModTime:
                                writeFile = True
                        if writeFile:
                            self.logger.debug('writing the file to fmeserver: {0}'.format(srcFile))
                            #self.logger.debug('dstFile: {0}'.format(destPath))
                            del dirList[-1]
                            self.resource.copyFile(self.params.FmeServerResources_EngineDirType, 
                                           dirList, 
                                           srcFile,
                                           overwrite=True, 
                                           createDirectories=True)
        
    def CheckForDirectories(self):
        dirType = self.params.FmeServerResources_EngineDirType
        dir2CreateList = self.templateDestDir.split('/')
        dirs = []
        if not self.resource.exists(dirType, dir2CreateList):
            for curDir in dir2CreateList:
                tmp = dirs[:]
                tmp.append(curDir)
                if not self.resource.exists(dirType, tmp):
                    self.resource.createDirectory(dirType, dirs, curDir)
                    dirs.append(curDir)

class ConfigsDepolyment(object):
    def CopyConfig(self):
        ignoreList = ['dbCreds.json']
        configDirName = 'config'
        srcDir = os.path.join(self.params.templateSourceDir, configDirName)
        destDir = posixpath.join(self.templateDestDir, configDirName)
        dirList = destDir.split('/')
        for dirName, subdirList, fileList in os.walk(srcDir):
            for curFile in fileList:
                if curFile not in ignoreList:
                    srcFile = os.path.join(dirName, curFile)
                    print 'srcFile', srcFile, destDir
                    self.resource.copyFile(self.params.FmeServerResources_EngineDirType, 
                                               dirList, 
                                               srcFile,
                                               overwrite=True, 
                                               createDirectories=True)



if __name__ == '__main__':
    
    ConfigLogging()
    
    
    fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\none2none.fmw'
    #fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\acdf_ownership_codes_staging_csv_bcgw.fmw'
    #fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\og_water_management_basins_sp_ogis_sde_bcgw.fmw'
        
    #repoName = 'GuyLaFleur'
    #fmwDeploy = FMWDeployment()
    #fmwDeploy.CopyFMW( fmwFile, repoName)
    
    
    deploy = PythonDeployment()
    deploy.DeployPython()
    
    
#     deploy.CopyCustomTransformers()
#     #deploy.DeleteRootDir() 
#     ()
#     deploy.CopyDataBCModules()
#     deploy.CopyConfig()
            
        
    