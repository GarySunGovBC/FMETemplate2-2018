'''
Created on Sep 28, 2018

@author: kjnether
'''

import DBCSecrets.GetSecrets
import logging
import os.path
import json
from DBCFMEConstants import TemplateConstants
import DeployConstants


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
            creds = DBCSecrets.GetSecrets.CredentialRetriever()
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
            creds = DBCSecrets.GetSecrets.CredentialRetriever()
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
                msg = 'The section {0} exists however there is no ' + \
                      'subsection named {1} under that section in the file' + \
                      ' {2}'
                msg = msg.format(section, subsection, self.configFile)
                self.logger.warning(msg)
        return retVal

    def getSectionSubSectionValue(self, section, subsection):
        retVal = None
        if self.subParamExists(section, subsection):
            # sectionKey = DeployConstants.DeploySections.frameworkPython.name
            # subSection = DeployConstants.DeploySubKeys.files.name
            retVal = self.conf[section][subsection]
        else:
            msg = "Looking for the section: %s subsection: %s in the file %s" + \
                  ' however it does not exist.'
            msg = msg.format(section, subsection, self.configFile)
            raise ValueError(msg)
        return retVal
