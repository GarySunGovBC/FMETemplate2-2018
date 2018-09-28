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

import Secrets.GetSecrets

from DBCFMEConstants import TemplateConstants
# import DataBCFMWTemplate @IgnorePep8
import DeployConstants
import FMEUtil.PyFMEServerV2 as PyFMEServer


# Move this to its own module
class DeploymentConfig(object):
    '''
    provides a wrapper around the deployment config file usually stored
    in ../../config/FMEServerDeployment.json

    Also uses the framework config file wrapper
    config file is used to get the following info:
     - the source path where files should be deployed from is relative
       to the directory where this .py file is found.
    '''

    def __init__(self, configFile=None):
        self.logger = logging.getLogger(__name__)

        self.configFile = configFile
        if not self.configFile:
            self.configFile = os.path.join(
                os.path.dirname(__file__),
                '..',
                '..',
                TemplateConstants.AppConfigConfigDir,
                TemplateConstants.FrameworkFMEServDeploymentFile)
            self.logger.debug("using the deployment config file: %s",
                              self.configFile)
        self.configFile = os.path.normpath(self.configFile)

        with open(self.configFile) as f:
            self.conf = json.load(f)
        # caching these parameter so they don't have to be retrieved from
        # pmp every time they are requested
        self.fmeUrl = None
        self.fmeToken = None

    def getFrameworkPythonFiles(self):
        '''
        Returns the list of files that make up the framework.
        '''
        sectionKey = DeployConstants.DeploySections.frameworkPython.name
        subSection = DeployConstants.DeploySubKeys.files.name
        retVal = self.getSectionSubSectionValue(sectionKey, subSection)
        return retVal

    def getConfigFileList(self):
        '''
        :return: the config files that need to be deployed to fme server
        '''
        sectionKey = DeployConstants.DeploySections.configFiles.name
        subSection = DeployConstants.DeploySubKeys.files.name
        retVal = self.getSectionSubSectionValue(sectionKey, subSection)
        return retVal

    def getConfigSourceDirectory(self):
        '''
        :return: the source directory for where the config file is expected
                 to be
        '''
        sectionKey = DeployConstants.DeploySections.configFiles.name
        subSection = DeployConstants.DeploySubKeys.sourceDirectory.name
        retVal = self.getSectionSubSectionValue(sectionKey, subSection)
        return retVal

    def getDependencyFileIgnoreList(self):
        '''
        :return: the list of files from the dependencies directory that
                 should be ignored, ie not copied to fme server.
        '''
        sectionKey = DeployConstants.DeploySections.pythonDependencies.name
        subSection = DeployConstants.DeploySubKeys.ignoreFilesList.name
        retVal = self.getSectionSubSectionValue(sectionKey, subSection)
        return retVal

    def getDependencyDirectoryIgnoreList(self):
        '''
        :return: the sub directories in the dependency directroy that should
                 be ignored, ie not copied to fme server.
        '''
        sectionKey = DeployConstants.DeploySections.pythonDependencies.name
        subSection = DeployConstants.DeploySubKeys.ignoreDirectories.name
        retVal = self.getSectionSubSectionValue(sectionKey, subSection)
        return retVal

    def getFrameworkRootDirectory(self):
        '''
        based on the relative location of this __file__ calculates where
        the various python files used by the framework are located

        :return: the path to the root directory calculated based on the
                 the relative location of this file
        '''
        curPath = os.path.dirname(__file__)
        rootDirForFramework = os.path.join(curPath, '..', '..')
        rootDirForFramework = os.path.normpath(rootDirForFramework)
        return rootDirForFramework

    def isDeploy64Bit(self):
        '''
        :return: a boolean value that indictes whether the deployment should
                 deploy 64 bit versions of libraries if they exist.
        '''
        USE64BIT = DeployConstants.DeploymentConstants.USE64BIT.name
        retVal = self.conf[USE64BIT]
        self.logger.debug('retVal: %s %s', retVal, type(retVal))
        return retVal

    def getDBCFMEFrameworkDependencyDirectory(self):
        '''
        :return: the directory where the dbcPyLib modules used by the
        FME Framework, are located.  This information is extracted from
        the config file
        '''
        curPath = os.path.dirname(__file__)
        dplyConfigKey = DeployConstants.DeploymentConstants.DEPENDENCYDIR.name
        libDir = self.conf[dplyConfigKey]
        rootDirForFramework = os.path.join(curPath, '..', '..', libDir)
        rootDirForFramework = os.path.normpath(rootDirForFramework)
        return rootDirForFramework

    def getFMEServerUrl(self):
        '''
        communicates with pmp and retrieves the fme server url associated
        with the FMESERVERLABEL which is described in the
        FMEServerDeployment.json file.

        :return: the url to fme server that is configured to be deployed to
        '''
        if self.fmeUrl is None:
            # comes from the secrets
            paramName = DeployConstants.DeploymentConstants.DESTFMESERVER.name
            label = self.conf[paramName]
            creds = Secrets.GetSecrets.CredentialRetriever()
            secrets = creds.getSecretsByLabel(label)
            host = secrets.getHost()
            url = 'http://{0}'.format(host)
            self.fmeUrl = url
        return self.fmeUrl

    def getFMEServerToken(self):
        '''
        communicates with pmp retrieving the fme server token that will be
        used to communicate with fme server through its rest api.
        :return: fme server api token
        '''
        if self.fmeToken is None:
            paramName = DeployConstants.DeploymentConstants.DESTFMESERVER.name
            label = self.conf[paramName]
            creds = Secrets.GetSecrets.CredentialRetriever()
            secrets = creds.getSecretsByLabel(label)
            token = secrets.getAPI()
            self.fmeToken = token
        return self.fmeToken

    def getDataBCPyModules(self):
        '''
        reads from the config file extracting the names of the packages
        in the dbcPyLib repository that should be included in the deploy.
        :return: dbcpylib packages used by the FME Framework
        '''
        sectionKey = DeployConstants.DeploySections.dataBCModules.name
        subSection = DeployConstants.DeploySubKeys.moduleList.name
        retVal = self.getSectionSubSectionValue(sectionKey, subSection)
        return retVal

    def getBinaryExecutableDirectory(self):
        '''
        :return: a directory where the framework stores any binary executable
                 files that it requires
        Binary executable directory / option was created when we were thinking
        about including the ability to support ssh tunnels with the framework
        later on we decieded that the tunnels would not be encorporated into
        the framework.  Keeping the option open as it was defined, not using
        it at the moment
        '''
        curPath = os.path.dirname(__file__)
        dplyConfigKey = DeployConstants.DeploymentConstants.BINARYDIR.name
        binDir = self.conf[dplyConfigKey]
        rootDirForFramework = os.path.join(curPath, '..', '..', binDir)
        rootDirForFramework = os.path.normpath(rootDirForFramework)
        return rootDirForFramework

    def paramExists(self, paramToTest):
        '''
        :param paramToTest: verifies if this parameter exists as a section
                            in the deployment config file
        :type paramToTest: bool
        '''
        retVal = False
        if paramToTest in self.conf:
            retVal = True
        else:
            msg = 'There is no section %s in the conf file %s'
            msg = msg.format(paramToTest, self.configFile)
            self.logger.warning(msg)
        return retVal

    def subParamExists(self, section, subsection):
        '''
        :param section: defines the section to test for
        :param subsection: defined the sub section to test for

        searches the content of the config file for the keys:
          section->subsection
        '''
        retVal = False
        if self.paramExists(section):
            if subsection in self.conf[section]:
                retVal = True
            else:
                msg = 'The section %s exists however there is no ' + \
                      'subsection named %s under that section in the file' + \
                      ' %s'
                msg = msg.format(section, subsection, self.configFile)
                self.logger.warning(msg)
        return retVal

    def getSectionSubSectionValue(self, section, subsection):
        retVal = None
        if self.subParamExists(section, subsection):
            sectionKey = DeployConstants.DeploySections.frameworkPython.name
            subSection = DeployConstants.DeploySubKeys.files.name
            retVal = self.conf[sectionKey][subSection]
        else:
            msg = "Looking for the section: %s subsection: %s in the file %s" + \
                  ' however it does not exist.'
            msg = msg.format(section, subsection, self.configFile)
            raise ValueError(msg)
        return retVal

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
        self.dplyConfig = DeploymentConfig(configFile=deployConfig)

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
        self.copyPythonFiles(overwrite=update)
        self.copyPythonDependencies(overwrite=update)
        self.copyDataBCModules(overwrite=update)

    def copyPythonFiles(self, overwrite=False):
        '''
        Copies the files described in the list self.templatePythonFiles
        to fme server.

        Checks to see if the files already exist, and if they do they do not
        get copied.

        :param  overwrite: This boolean parameter can be set to true to
                           overwrite if the files already exist.
        :type overwrite: enter type

        '''
        templateSourceDir = self.dplyConfig.getFrameworkRootDirectory()
        logging.debug("got here now in %s", __name__)
        fileDeploymentList = []
        FME_SHAREDRESOURCE_ENGINE = \
            DeployConstants.DeploymentConstants.FME_SHAREDRESOURCE_ENGINE.name
        for pyFile in self.templatePythonFiles:
            src = os.path.join(templateSourceDir, pyFile)
            dest = posixpath.join(self.templateDestDir, pyFile)
            self.logger.debug("src: %s", src)
            self.logger.debug("dest: %s", dest)
            deployment = Deployment(FME_SHAREDRESOURCE_ENGINE,
                                    src,
                                    dplyConfig=self.dplyConfig,
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


class BinaryDeployments(BaseDeployment):
    '''
    used to deploy any binary file requirements.  Need to pay particular care
    when using this method with 64 bit binaries.

    :ivar ignoreList: any files described in here will never be copied to fme
                      server.
    '''

    def __init__(self, deployConfig=None):
        BaseDeployment.__init__(self, deployConfig=deployConfig)
        self.ignoreList = ['readme.txt']
        # self.srcDir = 'bin'
        self.dplyConfig = DeploymentConfig(configFile=deployConfig)
        self.srcDir = self.dplyConfig.getBinaryExecutableDirectory()

    def copyBinaries(self, overwrite=False):
        '''
        Copies the binary dependencies found in the bin directory to fme
        server. ignores anything in the self.ignoreList parameter

        :param overwrite: Controls whether files that already exist in fme
                          server are overwritten or not
        '''
        binDirName = os.path.basename(self.srcDir)
        destDir = posixpath.join(self.templateDestDir, binDirName)
        fmeServerResourcesEngineDirType = \
            DeployConstants.DeploymentConstants.FME_SHAREDRESOURCE_ENGINE.name
        deploymentList = []
        for dirName, subdirList, fileList in os.walk(self.srcDir):
            del subdirList
            for curFile in fileList:
                if curFile not in self.ignoreList:
                    self.logger.debug("curFile: %s", curFile)

                    srcFile = os.path.join(dirName, curFile)
                    destFile = posixpath.join(destDir, curFile)
                    destFile = posixpath.normpath(destFile)
                    deployment = Deployment(fmeServerResourcesEngineDirType,
                                            srcFile,
                                            dplyConfig=self.dplyConfig,
                                            destDirStr=destFile)
                    deploymentList.append(deployment)
                    self.logger.debug("srcFile: %s", srcFile)
                    self.logger.debug("destDir: %s", destFile)

        self.logger.debug("overwrite param : %s", overwrite)
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

        ie the values that those constants resolve to must exist in the
        deployment json config file.
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
            DeployConstants.DeploySubKeys.sourceDirectory.name]
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
        self.destDirList = self.dplyConfig. 

    def verifyParams(self):
        '''
        verifies that the properties that need to be populated in order to
        proceed, are actually populated with values.
        '''
        if not self.srcDir:
            msg = 'The source directory has not been set, define the source' + \
                  "directory using the method 'setSourceDirectory()'"
            raise ValueError(msg)
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
        for curFile in self.files2Deploy:
            srcFile = os.path.join(self.srcDir, curFile)
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


class SecretsDeployment(GenericDeployment):

    def __init__(self, deployConfig=None):
        GenericDeployment.__init__(self, deployConfig=deployConfig)
        self.calcParams()

    def calcParams(self):
        self.srcDir = self.dplyConfig.getConfigSourceDirectory()
        self.files2Deploy = self.dplyConfig.getConfigFileList()


class ConfigsDeployment(BaseDeployment):
    '''
    Copies the configurations to fme server.  Class has a property

    :ivar ignoreList: a list of files that will never get copied to fme server
                      even if Trump does get elected.
    '''

    def __init__(self, deployConfig=None):
        BaseDeployment.__init__(self, deployConfig=deployConfig)
        # srcDir is 'config'
        self.srcDir = self.dplyConfig.getConfigSourceDirectory()
        self.files2Deploy = self.dplyConfig.getConfigFileList()

    def copyConfig(self, overwrite=False):
        '''
        Copies the configuration files in the config directory that are not
        mentioned in the 'ignoreList' to fme server.  Currently this only
        contains the logging config, may contain more at some other time.
        '''
        # configDirName = 'config'
        templateSourceDir = self.dplyConfig.getFrameworkRootDirectory()
        srcDir = os.path.join(templateSourceDir, self.srcDir)
        destDir = posixpath.join(self.templateDestDir, self.srcDir)
        fmeServerResourcesEngineDirType = \
            DeployConstants.DeploymentConstants.FME_SHAREDRESOURCE_ENGINE.name
        deploymentList = []
        for curFile in self.files2Deploy:
            self.logger.debug("curFile/dir: %s/%s", curFile, srcDir)

            srcFile = os.path.join(srcDir, curFile)
            destFile = posixpath.join(destDir, curFile)
            destFile = posixpath.normpath(destFile)
            self.logger.debug('srcFile: %s', srcFile)
            self.logger.debug('destFile: %s', destFile)
            deployment = Deployment(fmeServerResourcesEngineDirType,
                                    srcFile,
                                    dplyConfig=self.dplyConfig,
                                    destDirStr=destFile)
            deploymentList.append(deployment)
            self.logger.debug("srcFile: %s", srcFile)
            self.logger.debug("destDir: %s", destFile)

        self.logger.debug("overwrite param : %s", overwrite)
        self.deploy(deploymentList, overwrite)
