'''
Created on Mar 1, 2017

@author: kjnether

This is a very simple script that will be loaded into jenkins
creating an end point that can be used to construct an SDE
connection file
'''

import inspect
import logging.config
import os.path
import sys

sys.path.append(r"E:\sw_nt\python27\ArcGIS10.4\Lib\site-packages")
sys.path.append(r'E:\sw_nt\arcgis\Desktop10.4\arcpy')
sys.path.append(r'E:\sw_nt\arcgis\Desktop10.4\ArcToolbox\Scripts')
sys.path.append(r'E:\sw_nt\arcgis\Desktop10.4\bin')
sys.path.append(r"E:\sw_nt\python27\ArcGIS10.4\Lib")


class CreateConnectionFile(object):
    '''
    simple class used to help create arcgis .sde connection files
    '''

    def __init__(self, connFile2Create, host, serviceName, port=None):  # pylint: disable=redefined-outer-name
        self.logger = logging.getLogger(__name__)
        self.connFile2Create = connFile2Create
        self.host = host
        self.port = port
        self.serviceName = serviceName
        self.logger.info('serviceName: %s', serviceName)
        self.logger.info('host: %s', host)
        self.logger.info('connFile2Create: %s', connFile2Create)

    def createConnFile(self):
        '''
        Turns out this is actually much simpler than was originally
        thought.  Don't need a schema or a password to create the
        the sde file.  This makes things much simpler
        '''
        # connFileFullPath = self.secrets.getConnectionFilePath()
        connDirPath, connFilePath = os.path.split(self.connFile2Create)
        self.logger.debug("connDir: %s", connDirPath)
        self.logger.debug("connFile: %s", connFilePath)

        self.logger.debug("Connection file: %s", self.connFile2Create)

        suffix = os.path.splitext(self.connFile2Create)[1]
        if not suffix:
            self.connFile2Create = '{0}.sde'.format(self.connFile2Create)

        if not os.path.exists(self.connFile2Create):
        # if not arcpy.Exists(connFileFullPath):
            self.logger.debug('trying to create conn file ...')
            # for direct connect use:
            # CreateDatabaseConnection_management
            # host:port/serviceName, most of the time its just host/serviceName
            if not self.port:
                easyConnectString = '{0}/{1}'.format(self.host, self.serviceName)
            else:
                easyConnectString = '{0}:{1}/{2}'.format(self.host, self.port, self.serviceName)
            self.logger.debug("easyConnectString: %s", easyConnectString)

            self.logger.debug("importing arcpy...")
            import arcpy  # @UnresolvedImport

            self.logger.debug("arcpy imported")
            arcpy.SetProduct('arcinfo')
            self.logger.debug('arcpy product info %s', arcpy.ProductInfo())

            self.logger.debug("arcpytool report: %s", arcpy.CreateDatabaseConnection_management)
            self.logger.debug('inspect %s',
                              inspect.getargspec(arcpy.CreateDatabaseConnection_management))

            # seems to require a value for username and password even through they are invalid
            arcpy.CreateDatabaseConnection_management(connDirPath,
                                                      connFilePath,
                                                      'ORACLE',
                                                      easyConnectString,
                                                      'DATABASE_AUTH',
                                                      'junk',
                                                      'junk',
                                                      'DO_NOT_SAVE_USERNAME',
                                                      'SDE.DEFAULT')
            self.logger.info('connection file %s has been created', self.connFile2Create)


if __name__ == '__main__':
    # configuring logging
    '''
    curDir = os.path.dirname(__file__)
    logConfigDir = os.path.join(curDir, '..', 'config', 'logging.config')
    logConfigDir = os.path.normpath(logConfigDir)

    outLogDir = os.path.join(curDir, '..', 'log')
    outLogDir = os.path.normpath(outLogDir)

    if not os.path.exists(outLogDir):
        os.mkdir(outLogDir)

    logOutFile = os.path.join(outLogDir, 'createSDEConnFile.log')
    logOutFile = os.path.normpath(logOutFile)

    logging.config.fileConfig(logConfigDir, defaults={'logfilename': logOutFile})
    logger = logging.getLogger()
    logger.debug("first log message! test test test")
    '''

    # debug test parameters
    # sys.argv.append(r'\\data.bcgov\work\Workspace\kjnether\proj\createSDEConnectionFiles\junk1234')
    # sys.argv.append(r'bcgw-i.bcgov')
    # sys.argv.append(r'idwdlvr1.bcgov')

    # logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

    # sys.path.append(r"E:\sw_nt\python27\ArcGIS10.2\Lib\site-packages")
    # sys.path.append(r'E:\sw_nt\arcgis\Desktop10.2\arcpy')
    # sys.path.append(r'E:\sw_nt\arcgis\Desktop10.2\ArcToolbox\Scripts')
    # sys.path.append(r'E:\sw_nt\arcgis\Desktop10.2\bin')
    # sys.path.append(r"E:\sw_nt\python27\ArcGIS10.2\Lib")

    connFile2Create = sys.argv[1]
    host = sys.argv[2]
    servNm = sys.argv[3]
    if len(sys.argv) >= 5:
        port = sys.argv[4]
    else:
        port = None
    connFile = CreateConnectionFile(connFile2Create, host, servNm, port)
    connFile.createConnFile()
    # some test params
