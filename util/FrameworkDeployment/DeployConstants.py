'''
Created on Sep 26, 2018

@author: kjnether
'''
from enum import Enum


class DeploySections(Enum):
    frameworkPython = 1
    pythonDependencies = 2
    dataBCModules = 3
    configFiles = 4
    fmeCustomizations = 5
    binaries = 6
    secretFiles = 7


class DeploySubKeys(Enum):
    files = 1
    ignoreFilesList = 2
    ignoreDirectories = 3
    ignoreSuffixes = 4
    dependencyDirectory32 = 5
    dependencyDirectory64 = 6
    directoryList = 7
    customizationRootDirectory = 8
    transformersDirectory = 9
    pluginsDirectory = 10
    sourceDirectory = 11
    destinationFMEServerDirectory = 12
    sourceDirectoryList = 13


class FMEResourcesParams():
    pythonDirs = ["Plugins", 'python', 'python27']
    javaDirs = ["Plugins", 'Java']
    # moved to DeploymentConstants
    #fmeServerResourcesEngineDirType = 'FME_SHAREDRESOURCE_ENGINE'

class DeploymentConstants(Enum):
    '''
    Keys in the config file that are used to specify global parameters
    associated with the deployment
    '''
    DESTFMESERVER = 1
    USE64BIT = 2
    DEPENDENCYDIR = 3
    FME_SHAREDRESOURCE_ENGINE = 4
    BINARYDIR = 5