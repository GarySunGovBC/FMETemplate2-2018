'''
Created on Feb 7, 2018

@author: kjnether

This module was created to try to make it easier for
the framework to find arcgis install paths on different
computers.

This is necessary as the framework needs to be able to
find different paths depending on computer it is run on

Specifically used at the moment by the script that
creates the .sde connection files
'''

import _winreg
import numbers
import re
import logging
import os.path
import sys


class RegistryReader(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def getKeyValues(self, keys):
        self.logger.debug("keys: %s", keys)
        keyStr = '\\'.join(str(e) for e in keys)
        # keyStr = '\\'.join(keys)
        self.logger.debug('keyStr: %s', keyStr)
        explorer = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, keyStr, 0, _winreg.KEY_ALL_ACCESS)
        subKeys = []
        try:
            i = 0
            while 1:
                asubkey = _winreg.EnumKey(explorer, i)
                subKeys.append(asubkey)
                i += 1
        except WindowsError:
            self.logger.debug('Iteration is now complete')
        return subKeys

    def getKeyItems(self, keys):
        # keyStr = '\\'.join(keys)
        keyStr = '\\'.join(str(e) for e in keys)
        self.logger.debug('keyStr: %s', keyStr)
        explorer = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, keyStr, 0, _winreg.KEY_ALL_ACCESS)
        values = []
        try:
            cnt = 0
            while True:
                t = (_winreg.EnumValue(explorer, cnt))
                values.append(t)
                cnt += 1
        except WindowsError:
            self.logger.debug('Iteration is now complete')
        return values


class ArcGisInstallPaths(RegistryReader):

    def __init__(self):
        RegistryReader.__init__(self)

        self.logger = logging.getLogger(__name__)
        self.startKeys = ['SOFTWARE']
        self.rootKeyStr = 'HKEY_LOCAL_MACHINE'
        self.registryRoot = eval('_winreg.{0}'.format(self.rootKeyStr))
        self.installDirItemName = 'InstallDir'

        # this regex is used to find the desktop entry which is generally
        # formatted like: Desktop10.2
        self.desktopString = 'Desktop'
        self.desktopRegex = re.compile('^' + self.desktopString + '\d{1,2}\.\d+$', re.IGNORECASE)

    def getInstallDir(self):
        esriKey = 'ESRI'
        msg = 'searching for {2} arcgis desktop install dir in registry {0}->{1}'
        msg = msg.format(self.rootKeyStr, '->'.join(self.startKeys), esriKey)
        self.logger.info(msg)
        currentKeys = self.getKeyValues(self.startKeys)
        self.logger.debug("found the keys: {0} in {1}".format(currentKeys, self.startKeys))
        keyPath = self.startKeys
        retVal = None
        if esriKey in currentKeys:
            keyPath.append(esriKey)
            subKeys = self.getKeyValues(keyPath)
            self.logger.debug("sub keys in {0}->{1} are {2}".format(self.rootKeyStr, keyPath, subKeys))

            # arcgis install dir is frequently incorrect.  Instead want to find
            # the desktop entry here
            desktopEntries = []
            next((desktopEntries.append(subKey) for subKey in subKeys
                                 if self.desktopRegex.match(subKey)), None)
            self.logger.debug('found this desktop entry: {0}'.format(desktopEntries))
            desktopEntry = None
            if desktopEntries:
                desktopEntry = self.getLatestDesktopRelease(desktopEntries)

            # for subKey in subKeys:
            if desktopEntry:
                keyPath.append(desktopEntry)
                subItems = self.getKeyItems(keyPath)
                print 'desktop entries:', subItems
                # subItems: is a 3 item tuple,
                # 0=the entry name,
                # 1=The entry value
                # 2=?? not sure what this is, its always 1
                installDirEntry = next((subItem for subItem in subItems
                                        if subItem[0] == self.installDirItemName), None)
                installDir = installDirEntry[1]
                print 'installDir', installDir
                if installDir:
                    retVal = installDir
                    print 'The install dir is: {0}'.format(retVal)
            else:
                # no desktop entry
                msg = 'Cannot find a "Desktop#.#" entry under {0}->{1} Returned values include {2}'
                msg = msg.format(self.rootKeyStr, '->'.join(self.startKeys), subKeys)
                raise ESRIArcGISInstallDirNotFound(msg)
        else:
            msg = 'Looking in the registry for the ESRI Install path.  Looked in ' + \
                  '{2}->{0} for the item {1}.  Can\'t find this ' + \
                  'item.  Returned values are: {3}'
            msg = msg.format('->'.join(self.startKeys), esriKey, self.rootKeyStr, currentKeys)
            raise ESRIArcGISInstallDirNotFound(msg)
        return retVal

    def getLatestDesktopRelease(self, desktopEntries):
        '''
        :param desktopEntries: A list of desktop entries found in the registry
                               takes the form of something like this
                               [Desktop10.2,Desktop10.5,Desktop11.1]
        :type desktopEntries: list
        :return: The latest relase is returned in the example above that would
                 be Desktop11.1
        :rtype: str
        '''
        retVal = None
        if len(desktopEntries) == 1:
            retVal = desktopEntries[0]
        else:
            latest = 0
            for entry in desktopEntries:
                numberStr = re.sub(self.desktopString, '', entry, flags=re.IGNORECASE)
                numberStr = unicode(numberStr)
                if numberStr.replace('.', '', 1).isdigit():
                    number = float(numberStr)
                else:
                    msg = 'Trying to extract the desktop version from this string: {0} ', \
                          'but doesn\'t meet expected format which should start with {1}'
                    msg = msg.format(entry, self.desktopString)
                    raise ValueError, msg
                if number > latest:
                    latest = number
                    retVal = entry

        self.logger.info('latest desktop key is: {0}'.format(retVal))
        return retVal


class PythonInstallPaths(RegistryReader):

    def __init__(self):
        RegistryReader.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.startKeys = ['SOFTWARE']
        self.rootKeyStr = 'HKEY_LOCAL_MACHINE'
        self.registryRoot = eval('_winreg.{0}'.format(self.rootKeyStr))

    def getInstallDir(self, version=None):
        '''
        searches for HKEY_LOCAL_MACHINE->SOFTWARE->Python->PythonCore->{latest version}->InstallPath
        '''
        keyPath = self.startKeys
        pyKey = 'Python'
        pyCoreKey = 'PythonCore'
        pyInstallKey = 'InstallPath'
        returnedKeys = self.getKeyValues(keyPath)
        returnedKeys.sort()
        self.logger.debug("currentKeys: %s", returnedKeys)
        installPath = None
        if pyKey in returnedKeys:
            keyPath.append(pyKey)
            # now looking for Python
            returnedKeys = self.getKeyValues(keyPath)
            self.logger.debug("looking for pycore in: %s", returnedKeys)
            if pyCoreKey in returnedKeys:
                # now get the various versions
                keyPath.append(pyCoreKey)
                returnedKeys = self.getKeyValues(keyPath)
                self.logger.debug("version keys: %s", returnedKeys)
                versionKey = self.getVersionPath(returnedKeys, version)
                self.logger.debug("returned key: %s", versionKey)
                if versionKey:
                    # now get the install Dir
                    keyPath.append(versionKey)
                    self.logger.debug("key path: {0}".format(keyPath))
                    returnedKeys = self.getKeyValues(keyPath)
                    self.logger.debug("sub items: {0}".format(returnedKeys))
                    if pyInstallKey in returnedKeys:
                        # found it
                        keyPath.append(pyInstallKey)
                        returnedKeys = self.getKeyItems(keyPath)
                        self.logger.debug("items: {0}".format(returnedKeys))
                        installPath = returnedKeys[0][1]
        return installPath

    def getVersionPath(self, pyVersionKeys, version=None):
        '''
        :param version: The installed version of python, on the current machine
                        if this value is not provided will return the release
                        with the highest number.  Otherwise verifies that the
                        specified version exists.
        :return: The python version key in the registry
        '''
        retVal = None
        if version:
            if not isinstance(version, basestring):
                if not isinstance(version, numbers.Number):
                    # not a string and not a number, don't know what to do
                    msg = 'specified a python version to look for which is ' + \
                         'defined as a {0} type, expecting a string or a ' + \
                         'number.  Actual value as a string is {1}'
                    msg = msg.format(type(version), version)
                    raise ValueError, msg
                else:
                    version = unicode(version)
            if version in pyVersionKeys:
                retVal = version
            else:
                msg = 'You specified a python version of {0}. This version could ' + \
                      'not be found.  Versions that were found include {1}'
                msg = msg.format(version, pyVersionKeys)
                self.logger.warning(msg)
        else:
            latest = 0.0
            for version in pyVersionKeys:
                if not version.replace('.', '', 1).isdigit():
                    msg = 'expecting a version string that can be converted to a ' + \
                          'number.  This is the string that was found {0}.  Ignoring ' + \
                          'it as don\'t have the logic to deal with it.  Other keys ' + \
                          'are: {1}'
                    msg = msg.format(version, pyVersionKeys)
                    self.logger.warning(msg)
                else:
                    version = float(version)
                    if version > latest:
                        latest = version
            if latest:
                retVal = latest
        return retVal


class ArcPyPaths(object):
    '''
    mines the registry to find out about where arcgis and arcpy are installed
    then constructs a list of paths that need to be added to the PYTHONPATH
    env var in order to successfully import arcpy
    '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.defaultPyVersion = '2.7'

    def getPaths(self, pythonVersion=None):
        arcGisPaths = self.getArcGisDesktopPaths()
        pythonPaths = self.getPythonPaths(pythonVersion)
        arcGisPaths.extend(pythonPaths)  # merge the two lists
        return arcGisPaths

    def getPythonPaths(self, version=None):
        if not version:
            version = self.defaultPyVersion
        pypath = PythonInstallPaths()
        pythonRootPath = pypath.getInstallDir(version)
        returnPaths = self.ammendPythonPaths(pythonRootPath)
        return returnPaths

    def ammendPythonPaths(self, pythonRootDir):
        '''
        :param rootDir: the root python install directory
        :type rootDir: str

        :return: The supplied python root dir with the
                 following paths ammended:
                   - lib
                   - Lib/site-packages
        '''
        # needed this path on marsic: C:\Windows\system32;^

        sitePaths = os.path.join(pythonRootDir, 'Lib', 'site-packages')
        libPaths = os.path.join(pythonRootDir, 'lib')
        dllPaths = os.path.join(pythonRootDir, 'DLLs')

        returnPaths = []
        returnPaths.append(sitePaths)
        returnPaths.append(libPaths)
        returnPaths.append(dllPaths)
        returnPaths.append(r'C:\Windows\system32')
        return returnPaths

    def getArcGisDesktopPaths(self, desktopRootDir=None):
        '''
        gets the paths associated with the install of arcgis desktop
        '''
        paths2Add = []
        if not desktopRootDir:
            desktop = ArcGisInstallPaths()
            desktopRootDir = desktop.getInstallDir()
        arcpyDir = os.path.join(desktopRootDir, 'arcpy')
        toolboxDir = os.path.join(desktopRootDir, 'ArcToolbox', 'Scripts')
        binPath = os.path.join(desktopRootDir, 'bin')
        scriptsPath = os.path.join(desktopRootDir, 'Scripts')

        paths2Add.append(arcpyDir)
        paths2Add.append(toolboxDir)
        paths2Add.append(binPath)
        paths2Add.append(desktopRootDir)
        paths2Add.append(scriptsPath)
        return paths2Add

    def getPathsAndAddToPYTHONPATH(self, pythonVersion=None):
        paths = self.getPaths(pythonVersion)
        self.logger.debug("arcpypaths: %s", '\n'.join(paths))
        sys.path.extend(paths)
        self.logger.debug("new paths are: %s", '\n'.join(sys.path))


class ESRIArcGISInstallDirNotFound(OSError):

    def __init__(self, *args, **kwargs):
        OSError.__init__(self, *args, **kwargs)


if __name__ == '__main__':

    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    # arc = ArcGisInstallPaths()
    # arc.getInstallDir()

    # py = PythonInstallPaths()
    # installDir = py.getInstallDir('2.7')
    # print 'installDir', installDir
    pyVersion = '2.7'
    arcpath = ArcPyPaths()
    arcpath.getPathsAndAddToPYTHONPATH(pyVersion)

