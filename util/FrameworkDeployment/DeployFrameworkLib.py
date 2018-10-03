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

import logging
import os.path
import posixpath
import time
import datetime
import pytz

import util.FrameworkDeployment.DeployConstants as DeployConstants
import util.FrameworkDeployment.DeployConfigReader as DeployConfigReader
import FMEUtil.PyFMEServerV2 as PyFMEServer

# pylint: disable=no-self-use, invalid-name


class DirectoryCache(object):
    '''
    FME Resources (where we want to deploy to) has a rest api that is used
    to manage files associated with the framework.)

    This class caches the contents of what exists already in the resources
    section of FME Server.  The cache is used to determine if a file
    already exists on FME server.  Using this cache speeds up the process
    significantly compared to previous approach which saw a query for each
    file that was to be uploaded.
    '''

    def __init__(self, deployObj):
        self.logger = logging.getLogger(__name__)
        self.fmeResources = deployObj.resource

        self.resourceCache = None
        self.rootDirList = []
        self.load(deployObj)

    def loadDeployment(self, deployObj):
        '''
        Called by the load method.  load() determines if the data has
        already been loaded.  If it has not then this method is called.

        It will do the actual loading of information from fme server that
        corresponds with the given deployObj

        :param deployObj: loads the directories defined by this object
        :type deployObj: Deployment
        '''
        # fmeResources = dplyObj.resource
        # dplyObj.destDirList[0] is grabbing the first directory in
        # the list, so if the list was ['plugins', 'python', 'python27']
        # the directory passed will be plugins
        # doing this cause don't want to load the root directory
        # as it would contain too much data.

        resourceData = self.fmeResources.getDirectory(
            deployObj.destDirType, [deployObj.destDirList[0]])
        self.parseResourceData(resourceData, deployObj.destDirType)
        self.logger.debug("resourceData: %s", resourceData)
        rootDir = resourceData['name']
        rootDir = self.fixPath(rootDir)

        self.logger.debug("root dir: %s", rootDir)

        if rootDir not in self.rootDirList:
            self.logger.debug("adding the root dir: %s", rootDir)
            self.rootDirList.append(rootDir)

    def isSourceNewer(self, deployObj):
        '''
        :param deployObj: reads the properties in this object to identify
                          the source file and the destination fme server
                          resource.  Using the cached information gets
                          the file change date from fme server and compares
                          with the modification time stamp of the source.
                          returns true of source is newer.
        :type deployObj: Deployment
        :return: is the source newer?
        :rtype: bool
        '''
        self.load(deployObj)
        retVal = True
        self.logger.debug("src file: %s", deployObj.srcFile)
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
        the cache does not load every single directory that exists on
        FME server.  Instead it loads each top level directory of the
        'Engine' directory  With
        a default configuration on fme server this would be:
            - CoordinateSystemExceptions
            - CoordinateSystemGridOverrides
            - CoordinateSystems
            - CsmapTransformationExceptions
            - Formats
            - Plugins
            - Transformers

        The contents of each of those top level directories is loaded on
        an as needed basis.  This method verifies that the information
        required by the provided deploy object has already been loaded,
        and updates the data structure so that it can keep track of what
        has been loaded and what has not

        :param deployObj: the deployment object, read the information in
                          this object to ensure the resource cache associated
                          with this object has been loaded
        :type deployObj: Deployment
        '''
        rootDir = deployObj.destDirList[0]
        rootDir = self.fixPath(rootDir)

        # make sure the root dir was loaded.
        if rootDir not in self.rootDirList:
            msg = 'The root dir: {0} is not in the list {1} loading the dir'
            msg = msg.format(rootDir, self.rootDirList)
            self.logger.debug(msg)
            self.loadDeployment(deployObj)

    def fixPath(self, inPath):
        '''
        makes sure the path starts with a posix sep
          - ie converts somepath/another into /somepath/another

        :param inPath: the input path that should start with a posix path
                       separator
        :type inPath: str (path)
        '''
        if inPath[0] != posixpath.sep:
            inPath = posixpath.sep + inPath
        return inPath

    def exists(self, deployObj):
        '''
        Used to test if a deploy object already exists on fme server

        :param deployObj: tests if the given deployment object already
                          exists on fme server
        :type deployObj: Deployment
        '''
        self.load(deployObj)
        dirType = deployObj.destDirType
        fullFilePath = deployObj.destFileFullPathStr
        fullFilePath = self.fixPath(fullFilePath)
        exists = False

        # make sure the root dir was loaded.
        if dirType in self.resourceCache:
            self.logger.debug("found dir type: %s", dirType)
            self.logger.debug("looking for %s in %s",
                              fullFilePath, self.resourceCache[dirType])
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

        parses that information into the property self.resourceCache.  Further
        operations like getting the modification or existence of an object
        then extracts that information from the self.resourceCache property

        :param resourceData: a resource directory object returned from fme
                             server.  Corresponds with the schema sample
                             above.
        :type resourceData: str
        :param dirType: FME Server resources have directory type.  All the
                        interactions so far are FME_SHAREDRESOURCE_ENGINE,
                        however there are different types available
        :type dirType: str
        '''
        if self.resourceCache is None:
            self.resourceCache = {dirType: {}}
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
            # verifyDate = fileDate.strftime('%Y-%m-%dT%H:%M:%S')
            # self.logger.debug("original date / new date: %s/%s",
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

    def __init__(self, deployConfig=None, env=None):
        self.logger = logging.getLogger(__name__)
        fmeParams = DeployConstants.FMEResourcesParams()
        self.templateDestDir = str(posixpath.sep).join(fmeParams.pythonDirs)
        self.dplyConfig = DeployConfigReader.DeploymentConfig(
            configFile=deployConfig, env=env)
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
            # fmeResources = dplyObj.resource
            # dplyObj.destDirList[0] is grabbing the first directory in
            # the list, so if the list was ['plugins', 'python', 'python27']
            # the directory passed will be plugins
            # doing this cause don't want to load the root directory
            # as it would contain too much data.

            # contents = fmeResources.getDirectory(dplyObj.destDirType,
            #                                     [dplyObj.destDirList[0]])
            self.resourceCache = DirectoryCache(dplyObj)
        return self.resourceCache.exists(dplyObj)

    def isSourceNewer(self, dplyObj):
        '''
        Identifies if the source in the given deployment object is newer
        then the version on fme server.

        :param dplyObj: the deployment object that contains the source and
                        destination information
        :type dplyObj: Deployment
        '''
        return self.resourceCache.isSourceNewer(dplyObj)

    def deploy(self, deploymentList, overWrite):
        '''
        a generic deployment class, gets a list of Deployment objects that
        describe the files that need to be deployed to fme server.

        :param deploymentList: list of deployment objects to be evaluated
        :type deploymentList: list(Deployment)
        :param overWrite: identifies whether to force overwrite on destination
        :type overWrite: bool
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
        self.posixSep = posixpath.sep
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
            tmpDestDirStr_winPathList = destDirStr.split(os.path.sep)
            tmpDestDirStr_posixPathList = destDirStr.split(self.posixSep)
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
                destDirStr = self.posixSep.join(destDirList)
        elif destDirList:
            destDirStr = self.posixSep.join(destDirStr)
        elif destDirStr:
            destDirList = destDirStr.split(self.posixSep)
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
        self.destDirStr = self.posixSep.join(self.destDirList)

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
        '''
        :return: a python fme server resource object
        '''
        return self.resource

    def getFMEResourceDirectoryType(self):
        '''
        :return: the destination directory type
        '''
        return self.destDirType

    def getSourceFileString(self):
        '''
        :return: the source file path
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


class FMECustomizationDeployments(BaseDeployment):
    '''
    Methods used to deploy any FME Customizations to fme server.
    This one is a little different from the other deployments.  It
    copies to a different locations.  Its working so not messing with it
    for now.
    '''

    def __init__(self, deployConfig=None, env=None):
        BaseDeployment.__init__(self, deployConfig=deployConfig, env=env)

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
            self.logger.debug("srcpath: %s", srcPath)
            for dirName, subdirList, fileList in os.walk(srcPath):
                del subdirList
                for file2Copy in fileList:
                    srcFile = os.path.join(dirName, file2Copy)
                    relPath = os.path.relpath(
                        srcFile,
                        os.path.join(templateSourceDir, customizationDir))
                    relPath = relPath.replace(os.path.sep, posixpath.sep)

                    self.logger.debug('srcFile: %s', srcFile)
                    deploy = Deployment(fmeServerResourcesEngineDirType,
                                        srcFile,
                                        dplyConfig=self.dplyConfig,
                                        destDirStr=relPath)
                    deploymentList.append(deploy)
        self.deploy(deploymentList, overwrite)


class GenericFileDeployment(BaseDeployment):
    '''
    A base class that gets inherited.  This class supports deployment of
    named individual files
    '''

    def __init__(self, deployConfig=None, env=None):

        BaseDeployment.__init__(self, deployConfig=deployConfig, env=env)
        # srcDir is 'config'
        self.srcDir = None
        self.destDirList = None
        self.files2Deploy = None
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
        self.destDirList <- DeployConstants.DeploySubKeys.destinationFMEServerDirectory  # @IgnorePep8  #pylint: disable=line-too-long
        self.srcDir <- DeployConstants.DeploySubKeys.sourceDirectory
        '''
        # ideally this and the same named method in the deploy directories
        # should be in a subclass that this class inherits from, allowing
        # more code re-use
        if not self.dplyConfig.paramExists(sectionName):
            msg = 'The section: %s does not exist in the deployment config ' + \
                  'file %s'

            msg = msg.format(sectionName, self.dplyConfig.configFile)
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
    '''
    functionality to deploy directories.  Supports the following sub
    sections in the config file:
     - ignoreFilesList: list of specific files to ignore
     - ignoreDirectories: list of directories to ignore
     - ignoreSuffixes: list of file suffixes to ignore

    :ivar destDirList: populated with a list that describes where in the
                       fme server resources paths the files should be
                       deployed to.
    :ivar ignoreFileList: populated with a list of files that should
                          be ignored
    :ivar ignoreDirList: list of files that will will be ignored
    :ivar ignoreDirList: list of directories that will be ignored
    :ivar ignoreSuffixes: you get the picture
    :ivar srcDirList: list of source directories that should be copied to
                      fme server
    :ivar deploySection: the section in the config file that describes
                      what needs to be deployed, the source of most of this
                      classes properties
    :ivar srcRootDir: the source directory for where files are to be
                      sourced from. Default value is calculated relative
                      to the location of this file.
    :ivar dirType: the directory type, that is used when copying to fme
                   server.  Currently this remains static
    '''

    def __init__(self, deployConfig=None, env=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig, env=env)
        self.destDirList = None
        self.ignoreFileList = []
        self.ignoreDirList = []
        self.ignoreSuffixes = []
        self.srcDirList = None
        self.deploySection = None
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
            msg = msg.format(sectionName, self.dplyConfig.configFile)
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

        # if the ignoreSuffixes exists then get its value
        if self.dplyConfig.subParamExists(
                sectionName,
                DeployConstants.DeploySubKeys.ignoreSuffixes.name):
            self.ignoreSuffixes = self.dplyConfig.getSectionSubSectionValue(
                sectionName,
                DeployConstants.DeploySubKeys.ignoreSuffixes.name)

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
            fullSrcDirPath = os.path.normpath(fullSrcDirPath)
            if not os.path.isdir(fullSrcDirPath):
                msg = 'The source directory: {0} (full path: {1}) ' + \
                      'does not exist, in the root ' + \
                      'directory {1}'
                msg = msg.format(srcDir, fullSrcDirPath, self.srcRootDir)
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

    def isSrcFileInIgnoreSuffixes(self, srcFile):
        '''
        evaluates the srcFile to determine whether the suffix associated with
        the source file is in the ignore list
        :param srcFile: the source file to be tested
        :type srcFile:str
        :return: is the source file a type that should be ignored?
        :rtype: bool
        '''
        retVal = False
        self.logger.debug("ignoreSuffixes list: %s", self.ignoreSuffixes)
        if self.ignoreSuffixes:
            # evalute case insensitive
            ignoreLowerCaseList = [x.lower() for x in self.ignoreSuffixes]
            srcFileSuffix = os.path.splitext(srcFile)[1].lower()
            if srcFileSuffix in ignoreLowerCaseList:
                retVal = True
                msg = "Ignoring the file: {0} due to suffix being in " + \
                      "list {1}"
                msg = msg.format(srcFile, self.ignoreSuffixes)
                self.logger.debug(msg)
        return retVal

    def copyFiles(self, overwrite=False):
        '''
        iterates over all the files defined in the configuration,
        created deployment objects for each of them,
        puts deployment objects in a list,
        does the actual deploy, which does the actual copying.

        :param overwrite: if the file already exists and the source is older
                          then the destination, normally no copy will take
                          place.  If the overwrite parameter is set to tru
                          then the file will be copied regardless.
        :type overwrite: bool
        '''
        self.verifyParams()
        destDir = self.destDirList[0:]
        deployList = []
        for srcDir in self.srcDirList:
            srcDirFullPath = os.path.join(self.srcRootDir,
                                          srcDir)
            srcDirFullPath = os.path.normpath(srcDirFullPath)
            self.logger.debug('srcDirFullPath: %s', srcDirFullPath)
            destDir = self.destDirList[0:]
            self.logger.debug('destDir: %s', destDir)

            for dirName, subdirList, fileList in os.walk(srcDirFullPath):
                destDirInLoop = destDir[0:]
                del subdirList
                for file2Copy in fileList:
                    srcFile = os.path.normpath(os.path.join(dirName,
                                                            file2Copy))
                    # self.logger.debug('srcDirFullPath: %s', srcDirFullPath)
                    # self.logger.debug('destDir: %s', destDir)
                    # self.logger.debug('destDirInLoop: %s', destDirInLoop)
                    # self.logger.debug('dirName: %s', dirName)
                    # relPath is the directory relative to the input
                    # source directory,  now cutting up the directory into
                    # a list so we can verify that it is not in the ignore
                    # list
                    relPath = os.path.normpath(
                        os.path.dirname(os.path.relpath(srcFile,
                                                        srcDirFullPath)))
                    if not self.isDirectoryInIgnoreList(relPath) and \
                            not self.isSrcFileInIgnoreSuffixes(srcFile):
                        if file2Copy not in self.ignoreFileList:
                            # copy file
                            destDirForDeploy = os.path.join(*destDirInLoop)
                            destDirForDeploy = os.path.normpath(
                                os.path.join(destDirForDeploy, relPath,
                                             file2Copy))
                            destDirForDeploy = destDirForDeploy.replace(
                                os.sep, posixpath.sep)
                            self.logger.debug("destDirForDeploy: %s",
                                              destDirForDeploy)
                            self.logger.debug('srcFile: %s', srcFile)
                            self.logger.debug('self.dirType: %s', self.dirType)
                            deploy = Deployment(self.dirType,
                                                srcFile,
                                                dplyConfig=self.dplyConfig,
                                                destDirStr=destDirForDeploy)
                            deployList.append(deploy)
        self.deploy(deployList, overwrite)


class PythonDependencies(GenericDirectoryDeployment):
    '''
    reads the deployment config file, extracts the section that defines
    deployments for Secret files that make up the FME Framework
    '''

    def __init__(self, deployConfig=None, env=None):
        GenericDirectoryDeployment.__init__(self, deployConfig=deployConfig,
                                            env=env)
        self.setConfigFileSection(
            DeployConstants.DeploySections.pythonDependencies.name)


class DBCPyLibDependencies(GenericDirectoryDeployment):
    '''
    reads the deployment config file, extracts the section that defines
    deployments for individual DBCPyLib packages that are used by FME
    Framework.

    All the files that get deployed by this module are also depolyed by the
    PythonDependencies class.  This class depolys on the DBCpyLib subset.

    Kept the code here in case we need to do quick deployment of these
    dependencies
    '''

    def __init__(self, deployConfig=None, env=None):
        GenericDirectoryDeployment.__init__(self, deployConfig=deployConfig,
                                            env=env)
        self.setConfigFileSection(
            DeployConstants.DeploySections.dataBCModules.name)


class SecretsDeployment(GenericFileDeployment):
    '''
    reads the deployment config file, extracts the section that defines
    deployments for Secret files that make up the FME Framework

    Secrets are deployed as a git submodule, on the location where the
    framework gets deployed.
    '''

    def __init__(self, deployConfig=None, env=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig,
                                       env=env)
        self.setConfigFileSection(
            DeployConstants.DeploySections.secretFiles.name)


class PythonFMEFramework(GenericFileDeployment):
    '''
    reads the deployment config file, extracts the section that defines
    deployments for python files that make up the FME Framework
    '''

    def __init__(self, deployConfig=None, env=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig,
                                       env=env)
        self.setConfigFileSection(
            DeployConstants.DeploySections.frameworkPython.name)


class ConfigsDeployment(GenericFileDeployment):
    '''
    reads the deployment config file, extracts the section that defines
    deployments for config files
    '''

    def __init__(self, deployConfig=None, env=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig,
                                       env=env)
        self.setConfigFileSection(
            DeployConstants.DeploySections.configFiles.name)


class BinaryDeployments(GenericFileDeployment):
    '''
    reads the deployment config file, extracts the section that defines
    deployments for binary executable files that make up the FME Framework

    This is not used at the moment, was implemented to support ssh and thus
    the requirement to have putty.exe available.  In the end did not
    implement ssh tunnels as part of the framework.
    '''

    def __init__(self, deployConfig=None, env=None):
        GenericFileDeployment.__init__(self, deployConfig=deployConfig,
                                       env=env)
        self.setConfigFileSection(
            DeployConstants.DeploySections.binaries.name)
