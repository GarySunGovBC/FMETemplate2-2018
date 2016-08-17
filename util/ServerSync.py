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
import pprint

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
    
    FmeServerUrl = 'http://arneb.dmz' #'http://fenite.dmz'
    FmeServerAuthKey = 'd8c7340cc774160cbfec7823607d2cfe682e8813' #'93978b32c457984ac4bb1912ed27e7fcade6e3ec'
    FmeServerRepository = "TemplateV2_TEST" #'GuyLaFleur'
    
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
        
    def deployAll(self):
        deployPy = PythonDeployment()
        deployPy.DeployPython()
        
        deployConf = ConfigsDepolyment()
        deployConf.CopyConfig()
        
        deployFmeCust = FMECustomizationDeployments()
        deployFmeCust.CopyCustomTransformers()
    
class FMWDeployment(object):
    '''
    Methods to copy FMW's to FME server.
    '''
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.params = Params()
        #self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
        
        self.fmwSrcDir = r'\\data.bcgov\work\Workspace\kjnether\proj\FMETemplateRevision\data\templateImplementation\BCGW_REP_SCHEDULED'
        self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)

    def copyFMWs(self, overwrite=False):
        repo = self.fmeServ.getRepository()
        
        wrkspcs = repo.getWorkspaces(self.params.FmeServerRepository)
        wrkspcsNames = wrkspcs.getWorkspaceNames()
        print 'wrkspcsNames', wrkspcsNames
        for dirName, subdirList, fileList in os.walk(self.fmwSrcDir):
            for curFile in fileList:
                if os.path.splitext(curFile)[1].lower() == '.fmw':
                    fullPath2FMW = os.path.join(dirName, curFile)
                    if curFile not in wrkspcsNames:
                        self.logger.debug("uploading {0}".format(fullPath2FMW))
                        repo.copy2Repository(self.params.FmeServerRepository, fullPath2FMW)
                        wrkspcs.registerWithJobSubmitter(curFile)
                    else:
                        wrkSpcInfo = wrkspcs.getWorkspaceInfo(curFile, detail='low')
                        #  u'lastSaveDate': u'2016-04-25T11:48:00',
                        lastSaveDate = wrkSpcInfo['lastSaveDate']
                        destModTime = time.strptime(wrkSpcInfo['lastSaveDate'], '%Y-%m-%dT%H:%M:%S')
                        srcModTime = time.ctime(os.path.getmtime(fullPath2FMW))
                        if srcModTime> destModTime:
                            repo.updateRepository(self.params.FmeServerRepository, fullPath2FMW)
                            wrkspcs.registerWithJobSubmitter(curFile)
    
    def copyFMW(self, fmwFile, repoName):
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
        self.params = Params()
        self.templateDestDir = str(posixpath.sep).join(self.params.pythonDirs)
        
    def deploy(self, deploymentList, overWrite):
        self.logger.debug("deploy overwrite param: {0}".format(overWrite))
        self.logger.debug("deployment list: {0}".format(deploymentList))
        for deployment in deploymentList:
            writeFile = False
            if overWrite:
                self.logger.debug("overwrite parameter set")
                writeFile = True
            elif not deployment.exists():
                self.logger.debug("object {0} does not exist".format(deployment.destFileFullPathStr))
                writeFile = True
            else:
                if deployment.isSourceNewer():
                    self.logger.debug("source is newer! {0}".format(deployment.getSourceFileString()))
                    writeFile = True
            if writeFile:
                deployment.deploy()
                    
class Deployment(object):
    
    def __init__(self, dirType, srcFile, destDirStr=None, destDirList=None):
        # if using destDirStr parameter, MAKE CERTAIN The path delimiter is 
        # UNIX/POSIX forward slash, ala... '/'  NOT \
        
        # logging setup ...
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        
        # global params...
        self.params = Params()
        
        # create fme server rest api obj...
        self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
        self.resource = self.fmeServ.getResources()
        
        # setting the object properties...
        # sanity checking...
        
        if not destDirStr and not destDirList:
            msg = '{0} class instantiation requires you to specify one ' + \
                  'of these parameters: destDirStr | destDirList, you ' + \
                  'did not supply values for either'
            msg = msg.format(self.__class__.__name__)
            raise ValueError, msg
        if destDirStr and destDirList:
            tmpDestDirStr_winPathList = destDirStr.split('\\')
            tmpDestDirStr_posixPathList = destDirStr.split('/')
            if not (tmpDestDirStr_winPathList == destDirList or
               tmpDestDirStr_posixPathList == destDirList):
                msg = '{0} - You supplied values for both destDirList and  ' + \
                      'destDirStr, and they seem to refer to different ' + \
                      'directories, easiest fix is to supply one or the ' + \
                      'the other!  Values Provided are destDirStr : {1} ' + \
                      'and destDirList : {2}'
                msg = msg.format(self.__class__.__name__, 
                                 destDirStr, 
                                 destDirList)
                raise ValueError, msg
            else:
                destDirStr = '/'.join(destDirList)
        elif destDirList:
            destDirStr = '/'.join(destDirStr)
        elif destDirStr:
            destDirList = destDirStr.split('/')
            
        # fme resource type example FME_SHAREDRESOURCE_ENGINE
        self.destDirType = dirType
        # fme directory list including file reference
        self.destFileFullPathList = destDirList
        self.destFileFullPathStr = destDirStr
        # source file path (windows path)
        self.srcFile = srcFile
        # fme directory list without the file reference
        self.destDirList = self.destFileFullPathList[:-1]
        self.destDirStr = '/'.join(self.destDirList)
        
    def exists(self):
        return self.resource.exists(self.getFMEResourceDirectoryType(), self.destFileFullPathList)
        
    def getResource(self):
        ''' returns a python fme server resource object
        '''
        return self.resource

    def getFMEResourceDirectoryType(self):
        return  self.destDirType
        
    def getSourceFileString(self):
        return self.srcFile
        
    def deploy(self):
        self.logger.debug("Writing the source file to fme server {0}".format(self.srcFile))
        self.resource.copyFile(self.destDirType, 
                               self.destDirList, 
                               self.srcFile, 
                               overwrite=True
                               )
        
    def isSourceNewer(self):
        retVal = False
        destDirInfo = self.resource.getResourceInfo(self.destDirType, self.destFileFullPathList)
        destModTime = time.strptime(destDirInfo['date'], '%Y-%m-%dT%H:%M:%S')
        srcModTime = time.ctime(os.path.getmtime(self.srcFile))
        if srcModTime > destModTime:
            retVal = True
        return retVal
        
class PythonDeployment(BaseDeployment):
    
    def __init__(self):
        BaseDeployment.__init__(self)
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("LOGGING in {0}".format(modDotClass))
        
        #self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
        #self.resource = self.fmeServ.getResources()
        # specific python modules to be synced to the root directory of fme server
        # resources
        self.templatePythonFiles = ['DataBCFMWTemplate.py','ChangeDetectLib.py','FMELogger.py']
        
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
        logging.debug("got here now in {0}".format(__name__))
        #self.CheckForDirectories()
        fileDeploymentList = []
        for pyFile in self.templatePythonFiles:
            self.params.FmeServerResources_EngineDirType
            src = os.path.join(self.params.templateSourceDir, pyFile)
            dest = posixpath.join(self.templateDestDir, pyFile)
            self.logger.debug("src: {0}".format(src))
            self.logger.debug("dest: {0}".format(dest))
            deployment = Deployment(
                            self.params.FmeServerResources_EngineDirType, 
                            src, 
                            destDirStr=dest
                            )
            fileDeploymentList.append(deployment)
        self.deploy(fileDeploymentList, overwrite)           
                
    def CopyPythonDependencies(self, overwrite=False):
        
        ignoreList = ['.gitignore', 'requirements.txt']
        self.logger.info("files that are configured to be skipped: {0}".format(ignoreList))
        deploymentList = []
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
                    
                    deployment = Deployment(self.params.FmeServerResources_EngineDirType, 
                                            srcFile, 
                                            destDirStr=destPathWthFile)
                    deploymentList.append(deployment)
        self.deploy(deploymentList, overwrite)           
                                
    def CopyDataBCModules(self, overwrite=False):
        deploymentList = []
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
                        
                        deployment = Deployment(self.params.FmeServerResources_EngineDirType, 
                                                srcFile,
                                                destDirStr=destPathWithFile)
                        deploymentList.append(deployment)
        self.deploy(deploymentList, overwrite)
        
    def CheckForDirectories(self):
        # TODO: rewrite this to use the new deployment object
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

class FMECustomizationDeployments(BaseDeployment):
    
    def __init__(self):
        BaseDeployment.__init__(self)    
    
    def CopyCustomTransformers(self, overwrite=False):
        customizationDir = 'fmeCustomizations'
        transformersDir = 'Transformers'
        deploymentList = []
        srcPath = os.path.join(self.params.templateSourceDir, customizationDir, transformersDir)
        for dirName, subdirList, fileList in os.walk(srcPath):
            for file2Copy in fileList:
                srcFile = os.path.join(dirName, file2Copy)
                relPath = os.path.relpath(srcFile, os.path.join(self.params.templateSourceDir, customizationDir))
                relPath = relPath.replace("\\", '/')
                
                deploy = Deployment(self.params.FmeServerResources_EngineDirType, 
                                    srcFile, 
                                    destDirStr=relPath)
                deploymentList.append(deploy)
        self.deploy(deploymentList, overwrite)

class ConfigsDepolyment(BaseDeployment):
    
    def __init__(self):
        BaseDeployment.__init__(self)
        self.ignoreList = ['dbCreds.json']
        self.srcDir = 'config'
    
    def CopyConfig(self, overwrite=False):
        configDirName = 'config'
        srcDir = os.path.join(self.params.templateSourceDir, self.srcDir)
        destDir = posixpath.join(self.templateDestDir, configDirName)
        dirList = destDir.split('/')
        deploymentList = []
        for dirName, subdirList, fileList in os.walk(srcDir):
            for curFile in fileList:
                if curFile not in self.ignoreList:
                    self.logger.debug("curFile: {0}".format(curFile) )

                    srcFile = os.path.join(dirName, curFile)
                    destFile = posixpath.join(destDir, curFile)
                    destFile = posixpath.normpath(destFile)
                    deployment = Deployment(self.params.FmeServerResources_EngineDirType, 
                                            srcFile, 
                                            destDirStr=destFile)
                    deploymentList.append(deployment)
                    self.logger.debug("srcFile: {0}".format(srcFile) )
                    self.logger.debug("destDir: {0}".format(destFile) )

                    
        self.logger.debug("overwrite param : {0}".format(overwrite))
        self.deploy(deploymentList, overwrite)

if __name__ == '__main__':
    
    ConfigLogging()
    deploy = Deploy()
    #deploy.deployAll()
    deployPy = PythonDeployment()
    #deployPy.CopyPythonFiles(overwrite=True)
    
    deployConf = ConfigsDepolyment()
    deployConf.CopyConfig(overwrite=True)

    #deployPy.CopyDataBCModules(overwrite=True)

    
    fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\none2none.fmw'
    #fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\og_ancillary_other_apps_gov_sp_ogis_sde_bcgw.fmw'

    #fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\acdf_ownership_codes_staging_csv_bcgw.fmw'
    
    #fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\templateImplementation\BCGW_REP_SCHEDULED\fadm_public_sustained_yield_un_geoprd_sde_bcgw\fadm_public_sustained_yield_un_geoprd_sde_bcgw.fmw'
    #fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\templateImplementation\BCGW_REP_SCHEDULED\og_ancillary_other_apps_gov_sp_ogis_sde_bcgw\og_ancillary_other_apps_gov_sp_ogis_sde_bcgw.fmw'
    fmwDeploy = FMWDeployment()
    
    fmwDeploy.copyFMWs()
    #fmwDeploy.copyFMW(fmwFile, Params.FmeServerRepository)
    
    
            
        
    