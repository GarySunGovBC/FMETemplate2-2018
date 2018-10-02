'''
Created on May 9, 2016


idea is this script can be run to take any changes that have been
implemented in the template code and deploy it to server.

@author: kjnether

This version has been modified so that it can use the new FMEUtil

'''

import logging
import os.path
import posixpath
import time
import urllib

# If deploying to fenite you can use V3
import FMEUtil.PyFMEServerV2 as PyFMEServer


class Params(object):
    '''
    This is the deployment configuration, It controls what the deployment
    is going to do.  See individual parameter descriptions to understand
    how it works.

    '''
    fmeServerUrl = 'http://fmeserver'  # 
    fmeServerAuthKey = 'apitoken'
    fmeServerRepository = "TemplateV2_TEST"  # 'GuyLaFleur'

    fmeServerResourcesEngineDirType = 'FME_SHAREDRESOURCE_ENGINE'

    pythonDirs = ["Plugins", 'python', 'python27']
    templateSourceDir = r'source directory'
    
    # For installing java files
    javaDirs = ["Plugins", 'Java']
    jarsSourceDir = r''

    depsDir = r'lib'

    # dataBCModulesDir = r''
    dataBCModulesDir = r'path to dbcpylib modules'
    # TODO, uncomment
    # dataBCModules2Sync = ['DB', 'Misc', 'PMP','BCDCUtil']
    # dataBCModules2Sync = [ 'PMP', 'psutil', 'FMEUtil', 'ipaddress.py',
    #                        'mock', 'pbr', 'setuptools', 'pkg_resources',
    #                        'extern']
    dataBCModules2Sync = ['DB', 'Misc', 'BCDCUtil', 'PMP', 'psutil', 'FMEUtil']
    

class ConfigLogging(object):
    '''
    exists to do the logging setup
    '''

    def __init__(self):
        basename = os.path.basename(__file__)
        fileName = '{0}.log'.format(os.path.splitext(basename)[0])
        print 'fileName', fileName
        logFormatString = r'%(asctime)s - %(lineno)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(filename=fileName,
                            level=logging.DEBUG,
                            format=logFormatString,
                            datefmt='%H:%M:%S')
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.addFilter(LogFilter())
        formatter = logging.Formatter(logFormatString)
        console.setFormatter(formatter)
        logging.getLogger().addHandler(console)

        self.logger = logging.getLogger(__name__)
        self.logger.debug("TEST TEST TEST first log message")

class LogFilter(logging.Filter):
    '''
    Extends logging so we don't have to see the url security cert errors
    '''

    def filter(self, record):
        '''
        Removing any of the requests connectionpool errors.
        '''
        retVal = True
        srchStrings = [r'requests.packages.urllib3.connectionpool']
        for srchStr in srchStrings:
            if srchStr in str(record):
                retVal = False
        return retVal

class Deploy(object):
    '''
    This is the do everything class.  It coordinates the depolyments of the
    various pieces that make of the framework to fme server.

    Pieces include:
    - python files that make up the framework.
    - configuration files,
    - FME customizations, which is a directory created by FME with a strict set
                          of sub directories.  The only content that the
                          framework adds to these directories is the custom
                          tranformers (file change detector, parcel map updater,
                          etc, ...)
    - Binary Deployments, some scripts require external executables in order to
                          work.  Example putty.  This module looks in the
                          frameworks bin directory and copies all the files
                          in there.
    '''

    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__, self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

    def deployAll(self, overwrite=False):
        '''
        Deploys all the content necessary to the fme framework to fme server.
        the parameter overwrite controls whether the existing content will be
        replaced.

        This is the only way at the moment to update existing content.
        :param overwrite: Controls whether existing content on fme server will
                          be overwritten with new content.  Default behavior
                          is to not update.
        '''
        # TODO: add intelligence to the update to do a file comparison and only
        #       update if necessary.
        self.logger.info('doing the python file deployments')
        deployPy = PythonDeployment()
        deployPy.deployPython(update=overwrite)

        self.logger.info('doing the config file deployments')
        deployConf = ConfigsDeployment()
        deployConf.copyConfig(overwrite=overwrite)

        self.logger.info('doing the FME Customizations deployments')
        deployFmeCust = FMECustomizationDeployments()
        deployFmeCust.copyCustomTransformers(overwrite=overwrite)

        self.logger.info('doing the Binary file deployments')
        deployBin = BinaryDeployments()
        deployBin.copyBinaries(overwrite=overwrite)

class FMWDeployment(object):
    '''
    Methods to copy FMW's to FME server.  This is not part of the server
    sync process.  It stays part of this lib as there may be times when
    we want to upload the fmw that tests the framework.
    '''
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__, self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.params = Params()
        # self.fmeServ = PyFMEServer.FMEServer(self.params.fmeServerUrl,
        #                                      self.params.fmeServerAuthKey)

        self.fmwSrcDir = 'src path to fmws'
        self.fmeServ = PyFMEServer.FMEServer(self.params.fmeServerUrl,
                                             self.params.fmeServerAuthKey)

    def copyFMWs(self, overwrite=False):
        '''
        Reads the workspaces defined in the Constants property fmeServerRepository
        and caches them.  Then iterates through the contents of the fmwSrcDir
        property of this object.  If the fmw in question does not exist it
        gets uploaded to the repo (self.params.fmeServerRepository)

        If it does exist it gets the file date and compares with the candiate
        for upload.  If the candiate for upload is newer then that one gets
        uploaded.
        '''
        repo = self.fmeServ.getRepository()

        wrkspcs = repo.getWorkspaces(self.params.fmeServerRepository)
        wrkspcsNames = wrkspcs.getWorkspaceNames()
        self.logger.debug("workspace Names: %s", wrkspcsNames)
        for dirName, subdirList, fileList in os.walk(self.fmwSrcDir):
            del subdirList
            for curFile in fileList:
                if os.path.splitext(curFile)[1].lower() == '.fmw':
                    fullPath2FMW = os.path.join(dirName, curFile)
                    if curFile not in wrkspcsNames:
                        self.logger.debug("uploading %s", fullPath2FMW)
                        repo.copy2Repository(self.params.fmeServerRepository,
                                             fullPath2FMW)
                        wrkspcs.registerWithJobSubmitter(curFile)
                    elif overwrite:
                        wrkSpcInfo = wrkspcs.getWorkspaceInfo(curFile,
                                                              detail='low')
                        #  u'lastSaveDate': u'2016-04-25T11:48:00',
                        # keeping this info here in case want to use dates to
                        # do a proper update process where updates only take
                        # place if there are changes.
                        # lastSaveDate = wrkSpcInfo['lastSaveDate']
                        destModTime = time.strptime(wrkSpcInfo['lastSaveDate'],
                                                    '%Y-%m-%dT%H:%M:%S')
                        srcModTime = time.ctime(os.path.getmtime(fullPath2FMW))
                        if srcModTime > destModTime:
                            repo.updateRepository(self.params.fmeServerRepository,
                                                  fullPath2FMW)
                            wrkspcs.registerWithJobSubmitter(curFile)
                    else:
                        self.logger.debug("File %s already exists and overwrite" + \
                                          "option set to False", curFile)

    def copyFMW(self, fmwFile, repoName):
        '''
        :param fmwFile: The full path to an fmw on the local file system
        :param repoName: The name of the FME Server repository that you want
                         to upload the file to.
        '''
        # fmwPath, fmw = os.path.split(fmwFile)
        fmw = os.path.basename(fmwFile)
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
    '''
    This is the base class for all file deployments, they can
    inherit from this class populate their own properties and then re-use the
    deploy method.
    '''
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__, self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.params = Params()
        self.templateDestDir = str(posixpath.sep).join(self.params.pythonDirs)

    def deploy(self, deploymentList, overWrite):
        '''
        a generic deployment class, gets a list of files that need to be deployed
        to fme server.
        '''
        self.logger.debug("deploy overwrite param: %s", overWrite)
        self.logger.debug("deployment list: %s", deploymentList)
        for deployment in deploymentList:
            writeFile = False
            if overWrite:
                self.logger.debug("overwrite parameter set")
                writeFile = True
            elif not deployment.exists():
                self.logger.debug("object %s does not exist",
                                  deployment.destFileFullPathStr)
                writeFile = True
            else:
                if deployment.isSourceNewer():
                    self.logger.debug("source is newer! %s",
                                      deployment.getSourceFileString())
                    writeFile = True
            if writeFile:
                deployment.deploy()

class Deployment(object):
    '''
    a helper class used by other deployment classes.  This one contains the
    mechanics of how the files get copied up to FME Server.  The other classes
    re-use the same logic to accomplish their tasks.
    '''

    def __init__(self, dirType, srcFile, destDirStr=None, destDirList=None):
        # if using destDirStr parameter, MAKE CERTAIN The path delimiter is
        # UNIX/POSIX forward slash, ala... '/'  NOT \

        # logging setup ...
        modDotClass = '{0}.{1}'.format(__name__, self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        # global params...
        self.params = Params()

        # create fme server rest api obj...
        self.logger.debug("deploying to: %s", self.params.fmeServerUrl)
        self.fmeServ = PyFMEServer.FMEServer(self.params.fmeServerUrl, self.params.fmeServerAuthKey)
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
        '''
        used to determine whether the destFileFullPathList exists in fme server
        :return: indication of existence!
        :rtype: bool
        '''
        return self.resource.exists(self.getFMEResourceDirectoryType(), self.destFileFullPathList)

    def getResource(self):
        ''' returns a python fme server resource object
        '''
        return self.resource

    def getFMEResourceDirectoryType(self):
        '''
        returns the destination directory type
        '''
        return  self.destDirType

    def getSourceFileString(self):
        '''
        gets the source file path
        '''
        return self.srcFile

    def deploy(self):
        '''
        Does the actual deployment of files.  Creates the directories on FME
        Server side if they do not exist, and then copies files to that
        directory.
        '''

        self.logger.debug("Writing the source file to fme server %s",
                          self.srcFile)
        tmp = self.destDirList[0:]
        dir2Create = tmp.pop()
        self.logger.debug("tmp %s", tmp)
        self.logger.debug("dir2Create %s", dir2Create)
        self.logger.debug("self.destDirList %s", self.destDirList)

        if not self.resource.exists(self.destDirType, self.destDirList):
            self.resource.createDirectory(self.destDirType, tmp, dir2Create)
        # srcFile = urllib.quote(self.srcFile)
        self.logger.debug("srcFile: %s", self.srcFile)
        self.resource.copyFile(self.destDirType,
                               self.destDirList,
                               self.srcFile,
                               overwrite=True)

    def isSourceNewer(self):
        '''
        determines if the source side is newer than a version on fme server.
        :return: true or false if file is newer on source side
        :rtype: bool
        '''
        retVal = False
        destDirInfo = self.resource.getResourceInfo(self.destDirType,
                                                    self.destFileFullPathList)
        destModTime = time.strptime(destDirInfo['date'], '%Y-%m-%dT%H:%M:%S')
        srcModTime = time.ctime(os.path.getmtime(self.srcFile))
        if srcModTime > destModTime:
            retVal = True
        return retVal

class PythonDeployment(BaseDeployment):
    '''
    The python deployment class.  Used to deploy the python modules that make
    up the fme framework
    '''

    def __init__(self):
        BaseDeployment.__init__(self)
        modDotClass = '{0}.{1}'.format(__name__, self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("LOGGING in %s", modDotClass)

        # self.fmeServ = PyFMEServer.FMEServer(self.params.fmeServerUrl,
        # self.params.fmeServerAuthKey)
        # self.resource = self.fmeServ.getResources()
        # specific python modules to be synced to the root directory of fme server
        # resources
        # self.templatePythonFiles = \
        # ['DataBCFMWTemplate.py','ChangeDetectLib.py',
        # 'FMELogger.py', 'ParcelMapLib.py', 'ChangeDetectLib2.py',
        # 'DataBCFMWTemplateTesterLib.py', 'FMWExecutionOrderDependencies.py',
        # 'SSHTunnelling.py']
        self.templatePythonFiles = ['DataBCFMWTemplate.py',
                                    'ChangeDetectLib.py',
                                    'FMELogger.py',
                                    'ParcelMapLib.py',
                                    'ChangeDetectLib2.py',
                                    'DataBCFMWTemplateTesterLib.py', 
                                    'DataBCEmailer.py', 
                                    'FMWExecutionOrderDependencies.py', 
                                    'DataBCDbMethods.py', 
                                    'SSHTunnelling.py', 
                                    'InstallPaths.py', 
                                    'CreateSDEConnectionFile.py', 
                                    'FFSReader.py']
        self.logger.debug("template destination dir is %s", self.templateDestDir)
        # print 'self.templateDestDir', self.templateDestDir

        # external dependencies dir to be synced to fme server
        self.depsDir = os.path.join(self.params.templateSourceDir, self.params.depsDir)

    def deployPython(self, update=False):
        '''
        Deploys all the python code required by the fme template.
        This method will call all the other submethods necessary to
        copy the template module, external dependencies, dataBC library
        python modules so they are globally available to python on
        the fme server that they are deployed to.

        :param update: if a python module already exists on fme server the
                       default behaviour is to not overwrite it.  If you need
                       to update the contents of fme server switch this parameter
                       to True.
        '''
        self.copyPythonFiles(overwrite=update)
        self.copyPythonDependencies(overwrite=update)
        self.copyDataBCModules(overwrite=update)

    def copyPythonFiles(self, overwrite=False):
        '''
        Copies the files described in the list self.templatePythonFiles
        to fme server.

        Checks to see if the files already exist, and if they do they do not
        get copied.

        :param  overwrite: This boolean parameter can be set to true to overwrite
                           if the files already exist.
        :type overwrite: enter type

        '''
        logging.debug("got here now in %s", __name__)
        fileDeploymentList = []
        for pyFile in self.templatePythonFiles:
            src = os.path.join(self.params.templateSourceDir, pyFile)
            dest = posixpath.join(self.templateDestDir, pyFile)
            self.logger.debug("src: %s", src)
            self.logger.debug("dest: %s", dest)
            deployment = Deployment(self.params.fmeServerResourcesEngineDirType,
                                    src,
                                    destDirStr=dest)

            fileDeploymentList.append(deployment)
        self.deploy(fileDeploymentList, overwrite)

    def getDirList(self, inputDir):
        '''
        Reads an input directory and assembles the files in that directory
        that need to be copied up to fme server.  Returns a list that is in
        turn passed onto another method to actually do the copying.
        :param inputDir: the input directory to get the file list from.
        :return: a list of files found in the input directory.
        :rtype: list(str)
        '''
        self.logger.info("getting the directory parts for: %s", inputDir)
        allparts = []
        while 1:
            parts = os.path.split(inputDir)
            if parts[0] == inputDir:  # sentinel for absolute paths
                allparts.insert(0, parts[0])
                break
            elif parts[1] == inputDir:  # sentinel for relative paths
                allparts.insert(0, parts[1])
                break
            else:
                inputDir = parts[0]
                allparts.insert(0, parts[1])
        return allparts

    def copyPythonDependencies(self, overwrite=False):
        '''
        Copies the contents of the external depencies directory to fme
        server.  The directory that self.depsDir contains. Copy is
        recursive.

        If unmodified since the writing of this comment its the lib_ext dir

        :param  overwrite: param description
        :type overwrite: enter type
        '''
        ignoreList = ['.gitignore', 'requirements.txt', 'GMT+0']
        directoryIgnore = ['pytz-2016.10.dist-info', 'pytz']
        self.logger.info("files that are configured to be skipped: %s",
                         ignoreList)
        deploymentList = []
        for dirName, subdirList, fileList in os.walk(self.depsDir):
            del subdirList
            dirList = self.getDirList(dirName)
            ignore = False
            for dir2Ignore in directoryIgnore:
                if dir2Ignore in dirList:
                    ignore = True
                    self.logger.debug("skipping %s", dirName)
                    break
            if not ignore:
                for curFile in fileList:
                    if curFile not in ignoreList:
                        # writeFile = False
                        curFileEncoded = urllib.quote(curFile)
                        srcFile = os.path.join(dirName, curFile)
                        relpath = os.path.relpath(dirName, self.depsDir)
                        relpath = relpath.replace('\\', '/')
                        destPath = posixpath.join(self.templateDestDir, relpath)
                        destPathWthFile = posixpath.join(destPath, curFileEncoded)
                        # destPathList = destPath.split('/')

                        deployment = Deployment(self.params.fmeServerResourcesEngineDirType,
                                                srcFile,
                                                destDirStr=destPathWthFile)
                        deploymentList.append(deployment)
        self.deploy(deploymentList, overwrite)

    def copyDataBCModules(self, overwrite=False):
        '''
        Describe your method and what it does here ( if multiple
        lines make sure they are aligned to this margin)

        :param  overwrite: param description
        :type overwrite: enter type
        '''
        deploymentList = []
        for DataBCMod in self.params.dataBCModules2Sync:
            modPath = os.path.join(self.params.dataBCModulesDir, DataBCMod)
            if os.path.isfile(modPath):
                destPathWithFile = posixpath.join(self.templateDestDir, DataBCMod)
                deployment = Deployment(self.params.fmeServerResourcesEngineDirType,
                                        modPath,
                                        destDirStr=destPathWithFile)
                deploymentList.append(deployment)

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
                        # dirList = destPath.split('/')

                        deployment = Deployment(self.params.fmeServerResourcesEngineDirType,
                                                srcFile,
                                                destDirStr=destPathWithFile)
                        deploymentList.append(deployment)
        self.deploy(deploymentList, overwrite)

class FMECustomizationDeployments(BaseDeployment):
    '''
    Methods used to deploy any FME Customizations to fme server.
    '''

    def __init__(self):
        BaseDeployment.__init__(self)

    def copyCustomTransformers(self, overwrite=False):
        '''
        Copies any custom transformers that have been defined as part of the
        fme framework.
        :param overwrite: controls what to do if the file already exists on
                          fme server.  If set to true, the file will always
                          be overwritten, false is never.
        '''
        customizationDir = 'fmeCustomizations'
        transformersDir = 'Transformers'
        pluginsDir = 'Plugins'
        
        deploymentList = []
        srcPath1 = os.path.join(self.params.templateSourceDir,
                               customizationDir, transformersDir)
        srcPath2 = os.path.join(self.params.templateSourceDir,
                               customizationDir, pluginsDir)
        for srcPath in [srcPath1, srcPath2]:
            self.logger.debug("srcpath: {0}".format(srcPath))
            for dirName, subdirList, fileList in os.walk(srcPath):
                del subdirList
                for file2Copy in fileList:
                    srcFile = os.path.join(dirName, file2Copy)
                    relPath = os.path.relpath(srcFile,
                                              os.path.join(self.params.templateSourceDir,
                                                           customizationDir))
                    relPath = relPath.replace("\\", '/')
    
                    deploy = Deployment(self.params.fmeServerResourcesEngineDirType,
                                        srcFile,
                                        destDirStr=relPath)
                    deploymentList.append(deploy)
        self.deploy(deploymentList, overwrite)

class BinaryDeployments(BaseDeployment):
    '''
    used to deploy any binary file requirements.  Need to pay particular care
    when using this method with 64 bit binaries.

    :ivar ignoreList: any files described in here will never be copied to fme
                      server.
    '''

    def __init__(self):
        BaseDeployment.__init__(self)
        self.ignoreList = ['readme.txt']
        self.srcDir = 'bin'

    def copyBinaries(self, overwrite=False):
        '''
        Copies the binary dependencies found in the bin directory to fme server.
        ignores anything in the self.ignoreList parameter

        :param overwrite: Controls whether files that already exist in fme server
                          are overwritten or not
        '''
        binDirName = 'bin'
        srcDir = os.path.join(self.params.templateSourceDir, self.srcDir)
        destDir = posixpath.join(self.templateDestDir, binDirName)
        # dirList = destDir.split('/')
        deploymentList = []
        for dirName, subdirList, fileList in os.walk(srcDir):
            del subdirList
            for curFile in fileList:
                if curFile not in self.ignoreList:
                    self.logger.debug("curFile: %s", curFile)

                    srcFile = os.path.join(dirName, curFile)
                    destFile = posixpath.join(destDir, curFile)
                    destFile = posixpath.normpath(destFile)
                    deployment = Deployment(self.params.fmeServerResourcesEngineDirType,
                                            srcFile,
                                            destDirStr=destFile)
                    deploymentList.append(deployment)
                    self.logger.debug("srcFile: %s", srcFile)
                    self.logger.debug("destDir: %s", destFile)

        self.logger.debug("overwrite param : %s", overwrite)
        self.deploy(deploymentList, overwrite)

class ConfigsDeployment(BaseDeployment):
    '''
    Copies the configurations to fme server.  Class has a property

    :ivar ignoreList: a list of files that will never get copied to fme server
                      even if Trump does get elected.
    '''

    def __init__(self):
        BaseDeployment.__init__(self)
        self.ignoreList = ['dbCreds.json', 'devpaths_Example.json',
                           'devpaths.json']
        self.srcDir = 'config'

    def copyConfig(self, overwrite=False):
        '''
        Copies the configuration files in the config directory that are not
        mentioned in the 'ignoreList' to fme server.  Typically config files
        consist of logging config and the app config.
        '''
        configDirName = 'config'
        srcDir = os.path.join(self.params.templateSourceDir, self.srcDir)
        destDir = posixpath.join(self.templateDestDir, configDirName)
        # dirList = destDir.split('/')
        deploymentList = []
        for dirName, subdirList, fileList in os.walk(srcDir):
            del subdirList
            for curFile in fileList:
                if curFile not in self.ignoreList:
                    self.logger.debug("curFile: %s", curFile)

                    srcFile = os.path.join(dirName, curFile)
                    destFile = posixpath.join(destDir, curFile)
                    destFile = posixpath.normpath(destFile)
                    deployment = Deployment(self.params.fmeServerResourcesEngineDirType,
                                            srcFile,
                                            destDirStr=destFile)
                    deploymentList.append(deployment)
                    self.logger.debug("srcFile: %s", srcFile)
                    self.logger.debug("destDir: %s", destFile)

        self.logger.debug("overwrite param : %s", overwrite)
        self.deploy(deploymentList, overwrite)

if __name__ == '__main__':
    # sets up console based logging for this module
    ConfigLogging()
    
    #deploy = Deploy()
    #deploy.deployAll(overwrite=True)

    deployPy = PythonDeployment()
    deployPy.copyPythonFiles(overwrite=True)
    #deployPy.copyDataBCModules(overwrite=True)
    #deployPy.copyDataBCModules(overwrite=False)
    

    # deployBin = BinaryDeployments()
    # deployBin.copyBinaries()

    #deployFmeCust = FMECustomizationDeployments()
    #deployFmeCust.copyCustomTransformers(overwrite=True)
    #deployFmeCust.copyCustomTransformers(overwrite=False)
    
    
    #deployConf = ConfigsDeployment()
    #deployConf.copyConfig(overwrite=True)

    # fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\templateImplementation\BCGW_REP_SCHEDULED\og_ancillary_other_apps_gov_sp_ogis_sde_bcgw\og_ancillary_other_apps_gov_sp_ogis_sde_bcgw.fmw'
    # fmwFile = r'\\data.bcgov\work\Projects\FYE2017\DWACT-492_ParcelMapAPI\fmws\pmbc_parcel_fabric_poly_staging_gdb_bcgw.fmw'
    # fmwDeploy = FMWDeployment()
    # fmwDeploy.copyFMW(fmwFile, Params.fmeServerRepository)
    