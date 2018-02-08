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
import re
import logging


class ArcGisInstallPaths(object):

    def __init__(self):
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

    def getKeyValues(self, keys):
        keyStr = '\\'.join(keys)
        print 'keyStr', keyStr
        explorer = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, keyStr, 0, _winreg.KEY_ALL_ACCESS)
        subKeys = []
        try:
            i = 0
            while 1:
                asubkey = _winreg.EnumKey(explorer, i)
                subKeys.append(asubkey)
                i += 1
        except WindowsError:
            print 'done with iteration'
        return subKeys

    def getKeyItems(self, keys):
        keyStr = '\\'.join(keys)
        print 'keyStr', keyStr
        explorer = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, keyStr, 0, _winreg.KEY_ALL_ACCESS)
        values = []
        try:
            cnt = 0
            while True:
                t = (_winreg.EnumValue(explorer, cnt))
                values.append(t)
                cnt += 1
        except WindowsError:
            print 'done getting the values'
        return values
    
class RegistryReader(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def getKeyValues(self, keys):
        keyStr = '\\'.join(keys)
        self.logger.debug( 'keyStr: %s', keyStr)
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
        keyStr = '\\'.join(keys)
        self.logger.debug( 'keyStr: %s', keyStr)
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
    

class PythonInstallPaths(object):

    def __init__(self):
        pass


class ESRIArcGISInstallDirNotFound(OSError):

    def __init__(self, *args, **kwargs):
        OSError.__init__(self, *args, **kwargs)


if __name__ == '__main__':

    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    arc = ArcGisInstallPaths()
    arc.getInstallDir()
    
    

