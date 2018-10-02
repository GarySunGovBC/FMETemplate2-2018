'''
Created on Sep 26, 2018

This is a rework of the deployment scripts, This version will replace the
version defined in the modules:
 - DeployFramework_Ascella.py
 - DeployFramework_Gienah.py
 - DeployFramework.py

Changes will see:
 - configuration will move to a json config file and be extracted from the
   code.
 - all copying to fme server will be based on detected deltas
 - secrets will be retrieved using the secrets api.

Overview of how development / deployment will work:
  - Development:
      - Code will be stored in the gogs repository
      - secrets will be managed as a submodule.
      - updates will get made to the repository in GOGS
      - when a new version is ready it will get tagged with a version tag
        This version can now be deployed to workbench and fme server

  - Deployment To Dev (FMWWorkbench):
      - The home directory on matar / marsic for the framework is:
        scripts/
         - Z:\scripts\python\DataBCFmeTemplate2
      - to deploy the version of code that exists in the repository go
        to that directory and do a:
          `git pull`
          `git checkout <release tag> --recurse-submodule`
          `pip install -t lib -r lib/requirements.txt

  - Deployment To Prod (FMEServer):
      - Run this script which will copy everything associated with
        the framework to __



Deployment steps:
a) identify where the destination directory is
b) identify the release tag
c) will do a pull from the git repo
d) assemble the dependencies
e) fetch the secrets, which for the framework will be the config
   file.
f)


Issues to keep track of:
 - deployment from 32bit to 64 bit

@author: kjnether
'''

import json
# import logging.config @IgnorePep8
import logging
import os.path
import posixpath
import time
import urllib
import datetime
import pytz

import DBCSecrets.GetSecrets

from DBCFMEConstants import TemplateConstants
# import DataBCFMWTemplate @IgnorePep8
import DeployConstants
import FMEUtil.PyFMEServerV2 as PyFMEServer
# import DeployConfigReader.DeploymentConfig as DeploymentConfig
import DeployConfigReader
# Move this to its own module


class DirectoryCache(object):
    '''
    used to keep track of FME resource cache, to reduce the number of times
    the deploy needs to communicate with fme server.
    '''

    def __init__(self, deployObj):
        self.logger = logging.getLogger(__name__)
        self.fmeResources = deployObj.resource

        self.resourceCache = None
        self.rootDirList = []
        self.load(deployObj)

    def loadDeployment(self, deployObj):
        # fmeResources = dplyObj.resource
        # dplyObj.destDirList[0] is grabbing the first directory in
        # the list, so if the list was ['plugins', 'python', 'python27']
        # the directory passed will be plugins
        # doing this cause don't want to load the root directory
        # as it would contain too much data.

        resourceData = self.fmeResources.getDirectory(deployObj.destDirType,
                                                      [deployObj.destDirList[0]])
        self.parseResourceData(resourceData, deployObj.destDirType)
        self.logger.debug("resourceData: %s", resourceData)
        rootDir = resourceData['name']
        rootDir = self.fixPath(rootDir)

        self.logger.debug("root dir: %s", rootDir)

        if rootDir not in self.rootDirList:
            self.logger.debug("adding the root dir: %s", rootDir)
            self.rootDirList.append(rootDir)

    def isSourceNewer(self, deployObj):
        self.load(deployObj)
        retVal = True
        srcModTime = time.ctime(os.path.getmtime(deployObj.srcFile))
        self.logger.debug("srcModTime: %s", srcModTime)

        dirType = deployObj.destDirType
        fullFilePath = deployObj.destFileFullPathStr
        fullFilePath = self.fixPath(fullFilePath)
        if dirType in self.resourceCache:
            self.logger.debug('dirType: %s', dirType)
            self.logger.debug("fullFilePath: %s", fullFilePath)
            if fullFilePath in self.resourceCache[dirType]:
                self.logger.debug("found it")
                destDateTime = self.resourceCache[dirType][fullFilePath]
        destTimeStr = destDateTime.strftime('%Y-%m-%dT%H:%M:%S')
        destModTime = time.strptime(destTimeStr, '%Y-%m-%dT%H:%M:%S')
        self.logger.debug("destModTime: %s", destModTime)
        self.logger.debug("srcModTime: %s", srcModTime)

        if srcModTime < destModTime:
            retVal = False
        return retVal

    def load(self, deployObj):
        '''
        makes sure the data has been loaded to memory
        '''
        rootDir = deployObj.destDirList[0]
        rootDir = self.fixPath(rootDir)
        
        # make sure the root dir was loaded.
        if not rootDir in self.rootDirList:
            msg = 'The root dir: {0} is not in the list {1} loading the dir'
            self.logger.debug(msg.format(rootDir, self.rootDirList))
            self.loadDeployment(deployObj)

    def fixPath(self, inPath):
        '''
        makes sure the path starts with a posix sep
        '''
        if inPath[0] != posixpath.sep:
            inPath = posixpath.sep + inPath
        return inPath

    def exists(self, deployObj):
        self.load(deployObj)
        dirType = deployObj.destDirType
        fullFilePath = deployObj.destFileFullPathStr
        fullFilePath = self.fixPath(fullFilePath)
        exists = False
        
        # make sure the root dir was loaded.
        if dirType in self.resourceCache:
            self.logger.debug("found dir type: %s", dirType)
            self.logger.debug("looking for %s in %s", fullFilePath, self.resourceCache[dirType])
            if fullFilePath in self.resourceCache[dirType]:
                exists = True
        return exists

    def parseResourceData(self, resourceData, dirType):
        '''
        expecting a recursive structure that looks something like this:
        {u'contents': [
            {u'contents': [
               {u'date': u'2018-09-27T18:21:32',
                u'name': u'sqljdbc41.jar',
                u'path': u'/Plugins/Java/',
                u'size': 593632,
                u'type': u'FILE'}],
            u'date': u'2018-09-27T18:21:32',
            u'name': u'Java',
            u'path': u'/Plugins/',
            u'size': 0,
            u'type': u'DIR'}, ...
        '''
        if self.resourceCache is None:
            self.resourceCache = {dirType: {}}
        pntr = self.resourceCache[dirType]
        if isinstance(resourceData, dict) and resourceData['type'] == 'DIR':
            if 'contents' in resourceData:
                self.parseResourceData(resourceData['contents'], dirType)
        elif isinstance(resourceData, dict) and resourceData['type'] == 'FILE':
            fullPath = posixpath.join(resourceData['path'],
                                      resourceData['name'])
            fileDate = datetime.datetime.strptime(resourceData['date'],
                                                  '%Y-%m-%dT%H:%M:%S')
            pacific = pytz.timezone('Canada/Pacific')
            fileDate = fileDate.replace(tzinfo=pacific)
            verifyDate = fileDate.strftime('%Y-%m-%dT%H:%M:%S')
            #self.logger.debug("original date / new date: %s/%s",
            #                  resourceData['date'], verifyDate)
            self.resourceCache[dirType][fullPath] = fileDate
        elif isinstance(resourceData, list):
            for nextStruct in resourceData:
                self.parseResourceData(nextStruct, dirType)


class BaseDeployment(object):
    '''
    This is the base class for all file deployments, they can
    inherit from this class populate their own properties and then re-use the
    deploy method.
    '''

    def __init__(self, deployConfig=None):
        self.logger = logging.getLogger(__name__)
        fmeParams = DeployConstants.FMEResourcesParams()
        self.templateDestDir = str(posixpath.sep).join(fmeParams.pythonDirs)
        self.dplyConfig = DeployConfigReader.DeploymentConfig(configFile=deployConfig)
        self.resourceCache = None

    def deploymentExists(self, dplyObj):
        '''
        :param dplyObj: The Deployment object who's existence is to be
                        tested
        :type dplyObj: Deployment
        '''
        # resource cache is what is used to determine if the deployment
        # exists
        if self.resourceCache is None:
            # load the initial resource
            fmeResources = dplyObj.resource
            # dplyObj.destDirList[0] is grabbing the first directory in
            # the list, so if the list was ['plugins', 'python', 'python27']
            # the directory passed will be plugins
            # doing this cause don't want to load the root directory
            # as it would contain too much data.
            
            #contents = fmeResources.getDirectory(dplyObj.destDirType,
            #                                     [dplyObj.destDirList[0]])
            self.resourceCache = DirectoryCache(dplyObj)
        return self.resourceCache.exists(dplyObj)

    def isSourceNewer(self, dplyObj):
        '''
        '''
        return self.resourceCache.isSourceNewer(dplyObj)

    def deploy(self, deploymentList, overWrite):
        '''
        a generic deployment class, gets a list of files that need to be
        deployed to fme server.
        '''
        self.logger.debug("deploy overwrite param: %s", overWrite)
        self.logger.debug("deployment list: %s", deploymentList)
        for deployment in deploymentList:

            writeFile = False
            if overWrite:
                self.logger.debug("overwrite parameter set")
                writeFile = True
            # elif not deployment.exists():
            elif not self.deploymentExists(deployment):
                self.logger.debug("object %s does not exist",
                                  deployment.destFileFullPathStr)
                writeFile = True
            else:
                # if deployment.isSourceNewer():
                if self.isSourceNewer(deployment):
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

    def __init__(self, dirType, srcFile, dplyConfig, destDirStr=None,
                 destDirList=None):
        # if using destDirStr parameter, MAKE CERTAIN The path delimiter is
        # UNIX/POSIX forward slash, ala... '/'  NOT \

        self.dplyConfig = dplyConfig

        # logging setup ...
        self.logger = logging.getLogger(__name__)

        fmeServerUrl = self.dplyConfig.getFMEServerUrl()
        fmeServerToken = self.dplyConfig.getFMEServerToken()
        # create fme server rest api obj...
        self.logger.debug("deploying to: %s", fmeServerUrl)
        self.fmeServ = PyFMEServer.FMEServer(fmeServerUrl, fmeServerToken)

        self.resource = self.fmeServ.getResources()
        # setting the object properties...
        # sanity checking...

        if not destDirStr and not destDirList:
            msg = '{0} class instantiation requires you to specify one ' + \
                  'of these parameters: destDirStr | destDirList, you ' + \
                  'did not supply values for either'
            msg = msg.format(self.__class__.__name__)
            raise ValueError(msg)
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
                raise ValueError(msg)
            else:
                destDirStr = '/'.join(destDirList)
        elif destDirList:
            destDirStr = '/'.join(destDirStr)
        elif destDirStr:
            destDirList = destDirStr.split('/')
            self.logger.debug("destDirList: %s", destDirList)

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
        self.logger.debug('self.destFileFullPathList: %s',
                          self.destFileFullPathList)
        return self.resource.exists(
            self.getFMEResourceDirectoryType(), self.destFileFullPathList)

    def getResource(self):
        ''' returns a python fme server resource object
        '''
        return self.resource

    def getFMEResourceDirectoryType(self):
        '''
        returns the destination directory type
        '''
        return self.destDirType

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
            self.logger.debug("dir2Create: %s", dir2Create)
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

    def __init__(self, deployConfig=None):
        BaseDeployment.__init__(self, deployConfig=deployConfig)
        self.logger = logging.getLogger(__name__)

        self.templatePythonFiles = self.dplyConfig.getFrameworkPythonFiles()

        self.logger.debug("template destination dir is %s",
                          self.templateDestDir)

        # external dependencies dir to be synced to fme server
        # self.depsDir = os.path.join(self.params.templateSourceDir,
        #                             self.params.depsDir)

    def deployPython(self, update=False):
        '''
        Deploys all the python code required by the fme template.
        This method will call all the other submethods necessary to
        copy the template module, external dependencies, dataBC library
        python modules so they are globally available to python on
        the fme server that they are deployed to.

        :param update: if a python module already exists on fme server the
                       default behaviour is to not overwrite it.  If you need
                       to update the contents of fme server switch this
                       parameter to True.
        '''
        self.copyPythonDependencies(overwrite=update)
        self.copyDataBCModules(overwrite=update)

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
        ignoreList = self.dplyConfig.getDependencyFileIgnoreList()
        depsDir = self.dplyConfig.getDBCFMEFrameworkDependencyDirectory()
        depsDir64 = depsDir + u'64'
        is64 = self.dplyConfig.isDeploy64Bit()
        directoryIgnore = self.dplyConfig.getDependencyDirectoryIgnoreList()
        FME_SHAREDRESOURCE_ENGINE = \
            DeployConstants.DeploymentConstants.FME_SHAREDRESOURCE_ENGINE.name

        self.logger.info("files that are configured to be skipped: %s",
                         ignoreList)
        self.logger.debug("depsDir: %s", depsDir)
        deploymentList = []
        for dirName, subdirList, fileList in os.walk(depsDir):
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
                        # if 64 is enabled and same file exists in 64 then
                        # deploy it
                        dirName64 = dirName.replace(depsDir, depsDir64)
                        curFile64 = os.path.join(dirName64, curFile)
                        if os.path.exists(curFile64) and is64:
                            srcFile = curFile64
                            relpath = os.path.relpath(dirName64, depsDir64)
                        else:
                            srcFile = os.path.join(dirName, curFile)
                            relpath = os.path.relpath(dirName, depsDir)
                        self.logger.debug("srcFile: %s", srcFile)
                        self.logger.debug("relpath: %s", relpath)

                        curFileEncoded = urllib.quote(curFile)
                        relpath = relpath.replace('\\', '/')
                        destPath = posixpath.join(self.templateDestDir,
                                                  relpath)
                        destPathWthFile = posixpath.join(destPath,
                                                         curFileEncoded)
                        # destPathList = destPath.split('/')
                        self.logger.debug("adding %s", srcFile)
                        deployment = Deployment(FME_SHAREDRESOURCE_ENGINE,
                                                srcFile,
                                                dplyConfig=self.dplyConfig,
                                                destDirStr=destPathWthFile)
                        deploymentList.append(deployment)
        self.deploy(deploymentList, overwrite)

    def copyDataBCModules(self, overwrite=False):
        '''
        Describe your method and what it does here ( if multiple
        lines make sure they are aligned to this margin)

        :param  overwrite: Be default the method will only overwrite if the
        file on the source side is newer then the destination, but setting
        this parameter to true the modules will always be overwritten.
        :type overwrite: bool
        '''
        dataBCModules = self.dplyConfig.getDataBCPyModules()
        dataBCModulesDir = \
            self.dplyConfig.getDBCFMEFrameworkDependencyDirectory()
        FME_SHAREDRESOURCE_ENGINE = \
            DeployConstants.DeploymentConstants.FME_SHAREDRESOURCE_ENGINE.name
        deploymentList = []
        for DataBCMod in dataBCModules:
            modPath = os.path.join(dataBCModulesDir, DataBCMod)
            if os.path.isfile(modPath):
                destPathWithFile = posixpath.join(self.templateDestDir,
                                                  DataBCMod)
                deployment = Deployment(FME_SHAREDRESOURCE_ENGINE,
                                        modPath,
                                        dplyConfig=self.dplyConfig,
                                        destDirStr=destPathWithFile)
                deploymentList.append(deployment)

            for dirName, subdirList, fileList in os.walk(modPath):
                del subdirList
                for curFile in fileList:
                    junk, suffix = os.path.splitext(curFile)
                    del junk
                    if suffix != '.pyc':
                        srcFile = os.path.join(dirName, curFile)
                        relPath = os.path.relpath(modPath, dataBCModulesDir)
                        relPath = relPath.replace('\\', '/')
                        destPath = posixpath.join(self.templateDestDir,
                                                  relPath)
                        destPathWithFile = posixpath.join(destPath, curFile)
                        # dirList = destPath.split('/')

                        deployment = Deployment(FME_SHAREDRESOURCE_ENGINE,
                                                srcFile,
                                                dplyConfig=self.dplyConfig,
                                                destDirStr=destPathWithFile)
                        deploymentList.append(deployment)
        self.deploy(deploymentList, overwrite)


class FMECustomizationDeployments(BaseDeployment):
    '''
    Methods used to deploy any FME Customizations to fme server.
    '''

    def __init__(self, deployConfig=None):
        BaseDeployment.__init__(self, deployConfig=deployConfig)

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

        templateSourceDir = self.dplyConfig.getFrameworkRootDirectory()
        srcPath1 = os.path.join(templateSourceDir,
                                customizationDir,
                                transformersDir)
        srcPath2 = os.path.join(templateSourceDir,
                                customizationDir,
                                pluginsDir)
        fmeServerResourcesEngineDirType = \
            DeployConstants.DeploymentConstants.FME_SHAREDRESOURCE_ENGINE.name
        for srcPath in [srcPath1, srcPath2]:
            self.logger.debug("srcpath: {0}".format(srcPath))
            for dirName, subdirList, fileList in os.walk(srcPath):
                del subdirList
                for file2Copy in fileList:
                    srcFile = os.path.join(dirName, file2Copy)
                    relPath = os.path.relpath(
                        srcFile,
                        os.path.join(templateSourceDir, customizationDir))
                    relPath = relPath.replace("\\", '/')
                    self.logger.debug('srcFile: %s', srcFile)
                    deploy = Deployment(fmeServerResourcesEngineDirType,
                                        srcFile,
                                        dplyConfig=self.dplyConfig,
                                        destDirStr=relPath)
                    deploymentList.append(deploy)
        self.deploy(deploymentList, overwrite)


class GenericFileDeployment(BaseDeployment):

    def __init__(self, deployConfig=None):

        BaseDeployment.__init__(self, deployConfig=deployConfig)
        # srcDir is 'config'
        self.srcDir = None
        self.destDirList = None
        self.files2Deploy = self.dplyConfig.getConfigFileList()
        self.fmeObjType = \
            DeployConstants.DeploymentConstants.FME_SHAREDRESOURCE_ENGINE.name

    def setSourceDirectory(self, srcDir):
        '''
        :param srcDir: The source directory where the files that are to be
                       deployed are located
        '''
        self.srcDir = srcDir

    def setDestinationFMEServerDirectoryList(self, fmeServDirList):
        '''
        :param fmeServDirList: the fme server destination directory path,
                               this is the directory path that is to be
                               specified in the fme server resources section.
        :type fmeServDirList: list
        '''
        if not isinstance(fmeServDirList, list):
            msg = 'The parameter fmeServDirList has a type of %s, however ' + \
                  'it should be a type %s'
            msg = msg.format(type(fmeServDirList), type([]))
            raise ValueError(msg)

        self.destDirList = fmeServDirList

    def setFileListToDeploy(self, fileList):
        '''
        :param fileList: a list of files that are to be deployed to fme
                         server.  These are the files that exist in the
                         provided destination directory
        :type fileList: list
        '''
        if not isinstance(fileList, list):
            msg = 'The parameter fileList has a type of %s, however ' + \
                  'it should be a type %s'
            msg = msg.format(type(fileList), type([]))
            raise ValueError(msg)
        self.files2Deploy = fileList

    def setConfigFileSection(self, sectionName):
        '''
        :param sectionName: This is the section name in the config file
                            'FMEServerDeployment.json' that describes the
                            deployment.

        The section name must include the following parameters:
            DeployConstants.DeploySubKeys.files.name
            DeployConstants.DeploySubKeys.sourceDirectory.name
            DeployConstants.DeploySubKeys.destinationFMEServerDirectory.name

        This method will read that section and extract the values from the
        following subsections into the properties of the class necessary to
        proceed with a deployment.  This method sees all the parameters get
        extracted from the config file.  You only need to specify the section.

        self.files2Deploy <- DeployConstants.DeploySubKeys.files
        self.destDirList <- DeployConstants.DeploySubKeys.destinationFMEServerDirectory  # @IgnorePep8
        self.srcDir <- DeployConstants.DeploySubKeys.sourceDirectory
        '''
        if not self.dplyConfig.paramExists(sectionName):
            msg = 'The section: %s does not exist in the deployment config ' + \
                  'file %s'

            msg = msg.format(sectionName, self.dplyConfig.confFile)
            raise ValueError(msg)
        self.deploySection = sectionName

        # now verify that the required sub sections exist
        # currently expecting it to be files and sourceDirectory
        expectedSubSections = [
            DeployConstants.DeploySubKeys.files.name,
            DeployConstants.DeploySubKeys.sourceDirectory.name,
            DeployConstants.DeploySubKeys.destinationFMEServerDirectory.name]
        for expectedSubSection in expectedSubSections:
            if not self.dplyConfig.subParamExists(sectionName,
                                                  expectedSubSection):
                msg = 'The expected sub section %s is not defined in the ' + \
                      'section %s in the config file %s'
                msg = msg.format(expectedSubSection, self.deploySection,
                                 self.dplyConfig.configFile)
                raise ValueError(msg)
        # having verified that sub sections exist we can now populate the
        # parameters

        self.destDirList = self.dplyConfig.getSectionSubSectionValue(
            sectionName,
            DeployConstants.DeploySubKeys.destinationFMEServerDirectory.name)
        self.srcDir = self.dplyConfig.getSectionSubSectionValue(
            sectionName,
            DeployConstants.DeploySubKeys.sourceDirectory.name)
        self.files2Deploy = self.dplyConfig.getSectionSubSectionValue(
            sectionName,
            DeployConstants.DeploySubKeys.files.name)

    def verifyParams(self):
        '''
        verifies that the properties that need to be populated in order to
        proceed, are actually populated with values.
        '''
        if not self.srcDir:
            msg = 'The source directory has not been set, assuming the ' + \
                  "source directory is the framework root directory " + \
                  "if this is incorrect use the method " + \
                  "'setSourceDirectory()' to define the correct source"
            self.logger.warning(msg)
        if not self.files2Deploy:
            msg = 'you have not defined the list of files that need to be' + \
                  'deployed.  You can define these files through the ' + \
                  "method 'setFileListToDeploy()'"
            raise ValueError(msg)
        if not self.destDirList:
            msg = 'you have not defined the fme server destination ' + \
                  'directory list.  You can set the destination directory ' + \
                  "list using the method " + \
                  "'setDestinationFMEServerDirectory()'"
            raise ValueError(msg)

    def copyFiles(self, overwrite=False):
        '''
        Does the actual copying to fme server
        '''
        self.verifyParams()
        deploymentList = []
        srcDirAbs = os.path.join(self.dplyConfig.getFrameworkRootDirectory(),
                                 self.srcDir)
        for curFile in self.files2Deploy:
            self.logger.debug("self.srcDir: %s", self.srcDir)
            self.logger.debug("absolute srcdir: %s", srcDirAbs)
            self.logger.debug("curFile: %s", curFile)
            srcFile = os.path.join(srcDirAbs, curFile)
            destDir = posixpath.join(*self.destDirList)
            destFile = posixpath.join(destDir, curFile)
            deployment = Deployment(self.fmeObjType,
                                    srcFile,
                                    dplyConfig=self.dplyConfig,
                                    destDirStr=destFile)
            deploymentList.append(deployment)
            self.logger.debug("srcFile: %s", srcFile)
            self.logger.debug("destDir: %s", destFile)

        self.logger.debug("overwrite param : %s", overwrite)
        self.deploy(deploymentList, overwrite)


class GenericDirectoryDeployment(GenericFileDeployment):

    def __init__(self, deployConfig=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig)
        self.destDirList = None
        self.ignoreFileList = []
        self.ignoreDirList = []
        self.srcDirList = None
        self.srcRootDir = self.dplyConfig.getFrameworkRootDirectory()
        self.dirType = \
            DeployConstants.DeploymentConstants.FME_SHAREDRESOURCE_ENGINE.name
        # self.destDirList

    def setConfigFileSection(self, sectionName):
        '''
        define a section name that describes the deployment, a directory
        deployment must have the following subsection in it.

        optional subsections:
          - ignoreFilesList
          - ignoreDirectories

        required subsections:
          - sourceDirectoryList
          - destinationFMEServerDirectory
        '''
        # make sure that the section exists
        if not self.dplyConfig.paramExists(sectionName):
            msg = 'The section: %s does not exist in the deployment config ' + \
                  'file %s'

            msg = msg.format(sectionName, self.dplyConfig.confFile)
            raise ValueError(msg)
        self.deploySection = sectionName

        # now verify that the required sub sections exist
        # currently expecting it to be files and sourceDirectory
        expectedSubSections = [
            DeployConstants.DeploySubKeys.destinationFMEServerDirectory.name,
            DeployConstants.DeploySubKeys.sourceDirectoryList.name]
        for expectedSubSection in expectedSubSections:
            if not self.dplyConfig.subParamExists(sectionName,
                                                  expectedSubSection):
                msg = 'The expected sub section %s is not defined in the ' + \
                      'section %s in the config file %s'
                msg = msg.format(expectedSubSection, self.deploySection,
                                 self.dplyConfig.configFile)
                raise ValueError(msg)
        # Getting required values
        # --------------------------------------------------
        self.srcDirList = self.dplyConfig.getSectionSubSectionValue(
            sectionName,
            DeployConstants.DeploySubKeys.sourceDirectoryList.name)
        self.destDirList = self.dplyConfig.getSectionSubSectionValue(
            sectionName,
            DeployConstants.DeploySubKeys.destinationFMEServerDirectory.name)

        # Getting optional values
        # --------------------------------------------------
        # if the ignoreFilesList exists then get its value
        if self.dplyConfig.subParamExists(
            sectionName,
                DeployConstants.DeploySubKeys.ignoreFilesList.name):
            self.ignoreFileList = self.dplyConfig.getSectionSubSectionValue(
                sectionName,
                DeployConstants.DeploySubKeys.ignoreFilesList.name)

        # if the ignoreDirectories exists then get its value
        if self.dplyConfig.subParamExists(
            sectionName,
                DeployConstants.DeploySubKeys.ignoreDirectories.name):
            self.ignoreDirList = self.dplyConfig.getSectionSubSectionValue(
                sectionName,
                DeployConstants.DeploySubKeys.ignoreDirectories.name)

    def verifyParams(self):
        '''
        performing checks that the provided parameters make sense
        '''
        if self.srcDirList is None:
            msg = 'You have not specified the source directory, source ' + \
                  'directory can be specified using the ' + \
                  ' setConfigFileSection() method'
            raise ValueError(msg)
        # making sure the specified source directories exist
        for srcDir in self.srcDirList:
            fullSrcDirPath = os.path.join(self.srcRootDir, srcDir)
            if not os.path.isdir(fullSrcDirPath):
                msg = 'The source directory %s specified in the source ' + \
                      'directory list does not exist, in the root directory %s'
                msg = msg.format(srcDir, fullSrcDirPath)
                raise ValueError(msg)

    def isDirectoryInIgnoreList(self, inDir):
        '''
        Gets a in directory, breaks it up into its components if any of the
        directories is in the ignore list then it returns true
        :param inDir: the input directory
        :type inDir: str (a file path)
        '''
        retVal = False
        inDir = os.path.normpath(inDir)
        dirList = inDir.split(os.sep)
        for curDir in dirList:
            if curDir in self.ignoreDirList:
                retVal = True
                break
        return retVal

    def copyFiles(self, overwrite=False):
        '''

        :param overwrite:
        :type overwrite:
        '''
        self.verifyParams()
        destDir = self.destDirList[0:]
        deployList = []
        for srcDir in self.srcDirList:
            srcDirFullPath = os.path.join(self.srcRootDir,
                                          srcDir)
            self.logger.debug('srcDirFullPath: %s', srcDirFullPath)
            destDir = self.destDirList[0:]
            self.logger.debug('destDir: %s', destDir)

            for dirName, subdirList, fileList in os.walk(srcDirFullPath):
                destDirInLoop = destDir[0:]
                del subdirList
                for file2Copy in fileList:
                    srcFile = os.path.join(dirName, file2Copy)
                    # self.logger.debug('srcDirFullPath: %s', srcDirFullPath)
                    # self.logger.debug('destDir: %s', destDir)
                    # self.logger.debug('destDirInLoop: %s', destDirInLoop)
                    # self.logger.debug('dirName: %s', dirName)
                    # relPath is the directory relative to the input
                    # source directory,  now cutting up the directory into
                    # a list so we can verify that it is not in the ignore
                    # list
                    relPath = os.path.dirname(os.path.relpath(srcFile, srcDirFullPath))
                    if not self.isDirectoryInIgnoreList(relPath):
                        if file2Copy not in self.ignoreFileList:
                            # copy file
                            destDirForDeploy = os.path.join(*destDirInLoop)
                            destDirForDeploy = os.path.join(destDirForDeploy, relPath, file2Copy)
                            destDirForDeploy = destDirForDeploy.replace(os.sep, '/')
                            # destDirForDeploy = posixpath.normpath(destDirForDeploy)
                            self.logger.debug("destDirForDeploy: %s", destDirForDeploy)
                            self.logger.debug('srcFile: %s', srcFile)
                            self.logger.debug('self.dirType: %s', self.dirType)
                            deploy = Deployment(self.dirType,
                                                srcFile,
                                                dplyConfig=self.dplyConfig,
                                                destDirStr=destDirForDeploy)
                            deployList.append(deploy)

        self.deploy(deployList, overwrite)


class PythonDependencies(GenericDirectoryDeployment):

    def __init__(self, deployConfig=None):
        GenericDirectoryDeployment.__init__(self, deployConfig=deployConfig)
        self.setConfigFileSection(
            DeployConstants.DeploySections.pythonDependencies.name)


class SecretsDeployment(GenericFileDeployment):

    def __init__(self, deployConfig=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig)
        self.setConfigFileSection(
            DeployConstants.DeploySections.secretFiles.name)


class PythonGeneric(GenericFileDeployment):

    def __init__(self, deployConfig=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig)
        self.setConfigFileSection(
            DeployConstants.DeploySections.frameworkPython.name)


class ConfigsDeployment(GenericFileDeployment):

    def __init__(self, deployConfig=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig)
        self.setConfigFileSection(
            DeployConstants.DeploySections.configFiles.name)


class BinaryDeployments(GenericFileDeployment):

    def __init__(self, deployConfig=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig)
        self.setConfigFileSection(
            DeployConstants.DeploySections.binaries.name)

