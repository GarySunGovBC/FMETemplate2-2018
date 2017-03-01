'''
--------------------------------------------------
--- THIS IS THE CODE THAT HAS BEEN EXTRACTED FROM
--- THE FMW
--------------------------------------------------
logging in shutdown:
-----------------------
   logger = open(FME_LogFileName,'a')
   logger.write("wow - this message will actually be written to log file")
   logger.close()

logging in 
   - python transformer
   - scripted parameter
   - startup
       logger = fmeobjects.FMELogFile()
       #Send message to the logger
       logger.logMessageString("Hello I am Logging Now")
       pass

import fme
import DataBCFMWTemplate
template = DataBCFMWTemplate(fme)
template.startup()
'''
# for some reason this has to manually added
#import sys

# for fmeobjects to import properly the fme dir has 
# to be closer to the front of the pathlist
# TODO: have a autodetect for 2015 and send an error if
# not running under fme2015
# These lines are only necessary for unit tests as they 
# enable the use of the fmeobjects code
# pathList = os.environ['PATH'].split(';')
# pathList.insert(0, r'E:\sw_nt\FME2015')
# sys.path.insert(0, r'E:\sw_nt\FME2015\fmeobjects\python27')
# sys.path.insert(0, r'\\data.bcgov\work\scripts\python\DataBCPyLib')
# os.environ['PATH'] = ';'.join(pathList)

import importlib
import os
import pprint
import site
import platform
import ConfigParser
import json
import logging.config
import PMP.PMPRestConnect
import DB.DbLib
import datetime
import re
import inspect
import shutil
import sys

class TemplateConstants(object):
    # no need for a logger in this class as its just
    # a list of properties
    #
    # Maps to sections in the config file
    AppConfigFileName = 'templateDefaults.config'
    AppConfigConfigDir = 'config'
    AppConfigOutputsDir = 'outputs'
    AppConfigLogFileExtension = '.log'
    AppConfigSdeConnFileExtension = '.sde'
    AppConfigLogDir = 'log'
    AppConfigAppLogFileName = 'applogconfigfilename'
    
    # parameters relating to template sections
    ConfFileSection_global = 'global'
    ConfFileSection_global_key_rootDir = 'rootscriptdir'
    ConfFileSection_global_key_outDir =  'outputsbasedir'
    ConfFileSection_global_key_govComputers = 'gov_computers'
    ConfFileSection_global_key_govFmeServer = 'gov_fmeservernodes'
    ConfFileSection_global_configDirName = 'configdirname'
    ConfFileSection_global_devCredsFile = 'development_credentials_file'
    ConfFileSection_global_customScriptDir = 'customizescriptdir'
    ConfFileSection_global_changeLogDir = 'changelogsdir'
    ConfFileSection_global_changeLogFileName = 'changelogsfilename'
    ConfFileSection_global_sdeConnFileDir = 'sdeConnectionDir'
    ConfFileSection_global_failedFeaturesDir = 'failedFeaturesDir'
    ConfFileSection_global_failedFeaturesFile = 'failedFeaturesFile'
    ConfFileSection_global_directConnectClientString = 'directconnect_clientstring'

    ConfFileSection_destKeywords = 'dest_param_keywords'
    
    # properties from the config file.
    # server is deprecated, replaced by host
    #ConfFileSection_serverKey = 'server'
    ConfFileSection_hostKey = 'host'
    ConfFileSection_pmpResKey = 'pmpresource'
    ConfFileSection_oraPortKey = 'oracleport'
    ConfFileSection_sdePortKey = 'sdeport'
    #ConfFileSection_instanceKey = 'instance'
    ConfFileSection_serviceNameKey = 'servicename'
    ConfFileSection_instanceAliasesKey = 'instance_aliases'
    
    ConfFileSection_pmpTokens = 'pmptokens'
    
    ConfFileDestKey_Prod = 'prod'
    ConfFileDestKey_Test = 'test'
    ConfFileDestKey_Deliv = 'dlv'
    ConfFileDestKey_Devel = 'dev'
    
    ConfFile_dwm = 'dwm_config'
    ConfFile_dwm_pmpresource = 'pmp_resource'
    ConfFile_dwm_dbuser = 'db_user'
    ConfFile_dwm_dbinstance = 'db_instance'
    ConfFile_dwm_dbserver = 'db_server'
    ConfFile_dwm_dbport = 'db_port'
    ConfFile_dwm_table = 'dwmtable'
    
    
    # published parameters - destination
    FMWParams_DestKey = 'DEST_DB_ENV_KEY'
    FMWParams_DestSchema = 'DEST_SCHEMA'
    FMWParams_DestType = 'DEST_TYPE'
    FMWParams_DestFeatPrefix = 'DEST_FEATURE_'
    FMWParams_DestServer = 'DEST_SERVER'
    FMWParams_DestPort = 'DEST_PORT'
    #FMWParams_DestInstance = 'DEST_INSTANCE'
    FMWParams_DestServiceName = 'DEST_ORA_SERVICENAME'
    FMWParams_DestPassword = 'DEST_PASSWORD'
    
    # published parameters - source
    FMWParams_srcDataSet = 'SRC_DATASET_' # prefix for any file based source dataset
    FMWParams_SrcFGDBPrefix = 'SRC_DATASET_FGDB_'
    FMWParams_SrcXLSPrefix = 'SRC_DATASET_XLS_'
    FMWParams_SrcFeaturePrefix = 'SRC_FEATURE_'
    FMWParams_SrcSchema = 'SRC_ORA_SCHEMA'
    FMWParams_SrcProxySchema = 'SRC_ORA_PROXY_SCHEMA'
    # if there is more than one source schema use the method
    # getSrcInstanceParam to retrieve it
    
    FMWParams_SrcInstance = 'SRC_ORA_INSTANCE'
    FMWParams_SrcServiceName = 'SRC_ORA_SERVICENAME'
    FMWParams_SrcHost = 'SRC_HOST'
    FMWParams_SrcPort = 'SRC_PORT'
    FMWParams_SrcSDEDirectConnectClientStr = 'SRC_ORA_CLIENTSTRING'
    # if there is more than one source instance use the method
    # getSrcInstanceParam to retrieve it
    
    FMWParams_SrcFeatPrefix = 'SRC_FEATURE_'
    
    FMWParams_FileChangeEnabledParam = 'FILE_CHANGE_DETECTION'
    
    # TODO: define the source database parameters
    
    # The fmw macrovalue used to retrieve the directory 
    # that the fmw is in.
    FMWMacroKey_FMWDirectory = 'FME_MF_DIR'
    FMWMacroKey_FMWName = 'FME_MF_NAME'
    
    FMEServerSection='fmeserver'
    FMEServerSection_Host='host'
    #FMEServerSection_RootDir = '/fmerest/v2/'
    FMEServerSection_RootDir = 'rootdir'
    FMEServerSection_Token='token'
    
    # pmp config parameters
    ConfFileSection_pmpConfig = 'pmp_server_params'
    ConfFileSection_pmpConfig_baseurl = 'baseurl'
    ConfFileSection_pmpConfig_restdir = 'restdir'
    ConfFileSection_pmpConfig_alturl = 'alturl'
    
    # section pmp_source_info
    ConfFileSection_pmpSrc = 'pmp_source_info'
    ConfFileSection_pmpSrc_resources = 'sourceresource'
    ConfFileSection_pmpSrc_defaultOraPort = 'default_sourceoraport'
    
    # development mode json database params file
    DevelopmentDatabaseCredentialsFile = 'dbCreds.json'
    DevelopmentDatabaseCredentialsFile_DestCreds = 'destinationCreds'
    DevelopmentDatabaseCredentialsFile_SourceCreds = "sourceCredentials"
    DevelopmentDatabaseCredentialsFile_dbUser = 'username'
    #DevelopmentDatabaseCredentialsFile_dbInst = 'instance' # this parameter is deprecated, using service name now
    DevelopmentDatabaseCredentialsFile_dbServName = 'servicename'
    DevelopmentDatabaseCredentialsFile_dbPswd = 'password'
    
    # TODO: once the distribution svn is known plug it into this 
    # variable.  Then if a developer hasn't created a JSON file 
    # the error message will include this url allowing them 
    # to see an example of the format that should be used.
    svn_DevelopmentJSONFile_Url = r'\\data.bcgov\work\scripts\python\DataBCFmeTemplate2\config\dbCreds.json'
    
    # log format strings
    FMELogShutdownFormatString = '%(asctime)s|   ?.?|  0.0|PYTHON SHUTDOWN| %(levelname)s: %(message)s'
    FMELogStartupFormatString = '%(levelname)s: %(message)s'
    FMELogDateFormatString = '%Y-%m-%d %H:%M:%S'
    FMELogFileSuffix = '.logconfig'
    
    # Local time zone, when dates are converted to strings in the 
    # log file the strings will be in this time zone
    LocalTimeZone = 'US/Pacific'
    
    # Standardized keys that PMP uses when returning data
    # structures
    PMPKey_AccountId = 'ACCOUNT ID'
    PMPKey_AccountName = 'ACCOUNT NAME'
    
    def getSrcInstanceParam(self, position):
        val = self.calcNumVal(self.FMWParams_SrcInstance, position)
        return val
        
    def getSrcServiceNameParam(self, position):
        val = self.calcNumVal(self.FMWParams_SrcServiceName, position)
        return val
    
    def getSrcHost(self, position=None):
        val = self.calcNumVal(self.FMWParams_SrcHost, position)
        return val
    
    def getSrcPort(self, position):
        val = self.calcNumVal(self.FMWParams_SrcPort, position)
        return val
    
    def getOraClientString(self, position):
        val = self.calcNumVal(self.FMWParams_SrcSDEDirectConnectClientStr, position)
        return val
    
    def getSrcSchemaParam(self, position):
        val = self.calcNumVal(self.FMWParams_SrcSchema, position)
        return val
    
    def getSrcSchemaProxyParam(self, position):
        val = self.calcNumVal(self.FMWParams_SrcProxySchema, position)
        return val
        
    def calcNumVal(self, val, position):
        '''
        Sometimes in an FMW there will be multiple sources or destination 
        parameters.  For example when there are multiple destination features
        they can be labelled like this:
        
        DEST_FEATURE_1
        DEST_FEATURE_2
        DEST_FEATURE_3
        etc.
        
        This method recieves two parameters and appends them together 
        like this, example
        
        input: DEST_FEATURE and 3 will return a string equal to DEST_FEATURE_3
        
        '''
        if type(position) is str:
            if not position.isdigit():
                msg = 'Trying to calculate the schema "published parameter" / "fme ' + \
                      'macro value" {3} however you provided an invalid value for the ' + \
                      'position.  Position must be either an int. or a string. ' + \
                      'You specified the value {0} which has a type of {1}'
                msg = msg.format(position, type(position), val)
                raise ValueError, msg
        retValtemplate = '{0}_{1}'
        retVal = retValtemplate.format(val, position )
        return retVal
  
class Start(object):
    
    def __init__(self, fme):
        # getting the app constants
        self.const = TemplateConstants()
        fmwDir = fme.macroValues[self.const.FMWMacroKey_FMWDirectory]
        fmwName = fme.macroValues[self.const.FMWMacroKey_FMWName]
        destKey = fme.macroValues[self.const.FMWParams_DestKey]
        
        # set up logging
        ModuleLogConfig(fmwDir, fmwName, destKey)    
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)

        self.fme = fme
        self.logger.info('running the template startup')
        # Reading the global paramater config file
        self.paramObj = TemplateConfigFileReader(self.fme.macroValues[self.const.FMWParams_DestKey])
        # Extract the custom script directory from config file
        customScriptDir = self.paramObj.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_customScriptDir)
        # Assemble the name of a the custom script
        justScript, ext = os.path.splitext(self.fme.macroValues[self.const.FMWMacroKey_FMWName])
        del ext
        customScriptFullPath = os.path.join(customScriptDir, justScript + '.py')
        customScriptLocal = os.path.join(self.fme.macroValues[self.const.FMWMacroKey_FMWDirectory], justScript + '.py')
        
        # test to see if the custom script exists, if it does import it, and 
        # set the plugin parameter = to the Start() object.
        startupScriptDirPath = None
        if os.path.exists(customScriptLocal):
            startupScriptDirPath = self.fme.macroValues[self.const.FMWMacroKey_FMWDirectory]
        elif os.path.exists(customScriptFullPath):
            startupScriptDirPath = customScriptDir
        
        if startupScriptDirPath:
            # looking for custom module to load.  If one is found that startup will
            # take precidence
            self.logger.debug("adding to pythonpath {0}".format( startupScriptDirPath))
            site.addsitedir(startupScriptDirPath)
            self.logger.debug("python path has been appended successfully")
            self.logger.debug("trying to load the module {0}".format(justScript))
            startupModule = importlib.import_module(justScript)
            
            # is there a start class defined in the startup module
            if Util.isClassInModule(startupModule, 'Start'):
                self.logger.debug("{0} module loaded successfully".format(justScript))
                self.startupObj = startupModule.Start(self.fme)
            else:
                self.logger.debug('using the generic template startup')
                self.startupObj = DefaultStart(self.fme)
        else:
            self.logger.debug('using the generic template startup')
            self.startupObj = DefaultStart(self.fme)
        
    def startup(self):
        # default startup routine
        #self.fme.macroValues[self.const.FMWParams_DestKey]
        # debugging / develeopment - printing the macrovalues.
        # useful for setting up test cases.
        # --------------------------------------------------------
        # will either call the default startup or a customized one
        self.logger.debug("calling the startup method...")
        self.startupObj.startup()
        
class DefaultStart(object):
    def __init__(self, fme):
        self.fme = fme
        
    def startup(self):
        # currently there is no default startup code.
        # if there was it would go here
        pass
        
class Shutdown(object):
    
    def __init__(self, fme):
        # This method will always be called regardless of any customizations.
        self.fme = fme
        self.const = TemplateConstants()
        
        self.params = TemplateConfigFileReader(self.fme.macroValues[self.const.FMWParams_DestKey])
        
        # logging configuration
        fmwDir = self.fme.macroValues[self.const.FMWMacroKey_FMWDirectory]
        fmwName = self.fme.macroValues[self.const.FMWMacroKey_FMWName]
        destKey = self.fme.macroValues[self.const.FMWParams_DestKey]

        ModuleLogConfig(fmwDir, fmwName, destKey)
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("Shutdown has been called...")
        self.logger.debug("log file name: {0}".format(self.fme.logFileName))
        
        # looking for custom script for shutdown
        customScriptDir = self.params.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_customScriptDir)
        justScript, ext = os.path.splitext(self.fme.macroValues[self.const.FMWMacroKey_FMWName])
        del ext
        customScriptFullPath = os.path.join(customScriptDir, justScript + '.py')
        customScriptLocal = os.path.join(self.fme.macroValues[self.const.FMWMacroKey_FMWDirectory], justScript + '.py')
        
        # test to see if the custom script exists, if it does import it, and 
        # set the plugin parameter = to the Shutdown() object.
        shutdownScriptDirPath = None
        if os.path.exists(customScriptLocal):
            shutdownScriptDirPath = self.fme.macroValues[self.const.FMWMacroKey_FMWDirectory]
        elif os.path.exists(customScriptFullPath):
            shutdownScriptDirPath = customScriptDir
            site.addsitedir(shutdownScriptDirPath)
            shutdownModule = importlib.import_module(justScript)
            if Util.isClassInModule(shutdownModule, 'Shutdown'):
                # use the custom shutdown
                self.logger.debug("{0} module loaded successfully".format(justScript))
                self.shutdownObj = shutdownModule.Shutdown(self.fme)
            else:
                self.logger.debug('using the generic template shutdown')
                self.shutdownObj = DefaultShutdown(self.fme)
        else:
            self.logger.debug('using the generic template shutdown')
            self.shutdownObj = DefaultShutdown(self.fme)
        
    def shutdown(self):
        self.shutdownObj.shutdown()
        
# TODO: should define abstract classes that all shutdown and startup classes should inherit from.

class DefaultShutdown(object):

    def __init__(self, fme):
        self.fme = fme
        self.const = TemplateConstants()
        self.params = TemplateConfigFileReader(self.fme.macroValues[self.const.FMWParams_DestKey])
        
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
    def shutdown(self):
        destKey = self.fme.macroValues[self.const.FMWParams_DestKey]
        if not self.params.isDataBCNode():
            # either not being run on a databc computer, or is being run in development mode, either way
            # should not be writing to to the DWM logger.
            msg = "DWM record is not being writen as script is being run external to databc firewalls."
            self.logger.info(msg)
        elif self.params.getDestinationDatabaseKey(destKey) == self.const.ConfFileDestKey_Devel:
            msg = 'DWM record is not being written because the script is being run in development mode'
            self.logger.info(msg)
        else:
            self.logger.info("DWM record is being created")
            dwmWriter = DWMWriter(self.fme)
            #dwmWriter.printParams()
            dwmWriter.writeRecord()
        
class TemplateConfigFileReader(object):
    
    parser = None
    key = None
    
    def __init__(self, key, confFile=None):
        #ModuleLogConfig()
        #modDotClass = '{0}.{1}'.format(__name__, self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)

        self.confFile = confFile
        self.logger.debug("Reading the config file: {0}".format(self.confFile))
        self.const = TemplateConstants()
        self.readConfigFile()
        self.setDestinationDatabaseEnvKey(key)
        msg = "Destination database environment key has been set to ({0}) " + \
              " the raw input key is ({1})"
        msg = msg.format(self.key, key)
        self.logger.debug(msg)
        
    def readConfigFile(self):
        if not self.confFile:
            self.confFile = os.path.dirname(__file__)
            self.confFile = os.path.join(self.confFile, 
                                         self.const.AppConfigConfigDir,
                                         self.const.AppConfigFileName)
            if not os.path.exists(self.confFile):
                msg = 'Can\'t find the application config file: {0} '
                msg = msg.format(self.confFile)
                raise ValueError, msg
            self.logger.debug("config file path that has been calculated is {0}".format(self.confFile))
        if not self.parser:
            self.parser = ConfigParser.ConfigParser()
            self.parser.read(self.confFile)
            self.logger.debug("sections in the config file are: {0}".format(self.parser.sections()))
    
    def getChangeLogsDirFullPath(self):
        #if self.isDataBCNode():
        rootDir = self.getTemplateRootDirectory()
        outputs = self.getOutputsDirectory()
        changeLogDir = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_changeLogDir)
        changeLogFullPath = os.path.join(rootDir, outputs, changeLogDir)
        changeLogFullPath = os.path.realpath(changeLogFullPath)
        return changeLogFullPath
    
    def getChangeLogFile(self):
        changeLogFile = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_changeLogFileName)
        return changeLogFile
    
    def getApplicationLogFileName(self):
        logConfFileName = self.parser.get(self.const.ConfFileSection_global, self.const.AppConfigAppLogFileName)
        return logConfFileName
        
    def validateKey(self, key):
        key = self.getDestinationDatabaseKey(key)
        if not key:
            msg = 'You specified a destination key of {0}. The ' + \
                  'config file does not have any destination database ' + \
                  'parameters configured for that key.  Acceptable keys ' + \
                  'are: {1}'
            validKeys = self.getValidKeys()
            msg = msg.format(key, validKeys)
            raise ValueError, msg
        
    def setDestinationDatabaseEnvKey(self, key):
        self.validateKey(key)
        self.key = self.getDestinationDatabaseKey(key)
        
    def getValidKeys(self):
        '''returns a list of accepted values for keys'''
        items = self.parser.items(self.const.ConfFileSection_destKeywords)
        retVal = []
        for itemTuple in items:
            keyList = itemTuple[1].split(',')
            for indexPos in range(0, len(keyList)):
                keyList[indexPos] = keyList[indexPos].strip()
            retVal.extend(keyList)
        return retVal
            
# SHOULD BE USING HOST.  HOST IS more consistent
#    def getDestinationServer(self):
#        server = self.parser.get(self.key, self.const.ConfFileSection_serverKey)
#        return server
    
    def getDestinationHost(self):
        host = self.parser.get(self.key, self.const.ConfFileSection_hostKey)
        return host
    
    def getFMEServerHost(self):
        host =  self.parser.get(self.const.FMEServerSection, self.const.FMEServerSection_Host)
        return host
    
    def getFMEServerRootDir(self):
        rootdir =  self.parser.get(self.const.FMEServerSection, self.const.FMEServerSection_RootDir)
        return rootdir
    
    def getFMEServerToken(self):
        token = self.parser.get(self.const.FMEServerSection, self.const.FMEServerSection_Token)
        return token
        
    def getSourcePmpResources(self):
        srcPmpResources = self.parser.get(self.const.ConfFileSection_pmpSrc, self.const.ConfFileSection_pmpSrc_resources)
        srcPmpResourcesList = srcPmpResources.split(',')
        # getting rid of leading/trailing spaces on each element in the list
        for cnter in range(0, len(srcPmpResourcesList)):
            srcPmpResourcesList[cnter] = srcPmpResourcesList[cnter].strip()
        return srcPmpResourcesList
    
    def getDefaultOraclePort(self):
        srcDefaultOraPort = self.parser.get(self.const.ConfFileSection_pmpSrc, self.const.ConfFileSection_pmpSrc_defaultOraPort)
        return srcDefaultOraPort
        
    def getDestinationPmpResource(self, dbEnvKey=None):
        self.logger.debug("raw input dbEnv Key is {0}".format(dbEnvKey))
        if not dbEnvKey:
            dbEnvKey = self.key
        else:
            dbEnvKey = self.getDestinationDatabaseKey(dbEnvKey)
        pmpRes = self.parser.get(dbEnvKey, self.const.ConfFileSection_pmpResKey)
        return pmpRes
    
    def getDestinationOraclePort(self):
        oraPort = self.parser.get(self.key, self.const.ConfFileSection_oraPortKey)
        return oraPort

    def getDestinationSDEPort(self):
        sdePort = self.parser.get(self.key, self.const.ConfFileSection_sdePortKey)
        return sdePort
    
    def getDestinationServiceName(self):
        inst = self.parser.get(self.key, self.const.ConfFileSection_serviceNameKey)
        return inst
        
    def getPmpToken(self, computerName):
        try:
            token = self.parser.get(self.const.ConfFileSection_pmpTokens, computerName)
        except ConfigParser.NoOptionError:
            msg = 'Trying to get a PMP token for the computer {0} but ' + \
                  'there are no pmp tokens defined for that machine in ' + \
                  'the app. config file: {1}.'
            msg = msg.format(computerName, self.confFile)
            self.logger.error(msg)
            raise ValueError, msg
        return token
    
    def getPmpBaseUrl(self):
        pmpBaseUrl = self.parser.get(self.const.ConfFileSection_pmpConfig, self.const.ConfFileSection_pmpConfig_baseurl)
        return pmpBaseUrl
    
    def getPmpAltUrl(self):
        pmpAltUrl = self.parser.get(self.const.ConfFileSection_pmpConfig, self.const.ConfFileSection_pmpConfig_alturl)
        return pmpAltUrl
    
    def getPmpRestDir(self):
        restDir = self.parser.get(self.const.ConfFileSection_pmpConfig, self.const.ConfFileSection_pmpConfig_restdir)
        return restDir
    
    def getConfigDirName(self):
        confDirName = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_configDirName)
        return confDirName
    
    def getTemplateRootDirectory(self):
        rootDir = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_key_rootDir)
        return rootDir
    
    def getFailedFeaturesDir(self):
        failedFeatsDir = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_failedFeaturesDir)
        return failedFeatsDir
    
    def getFailedFeaturesFile(self):
        failedFeatsFile = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_failedFeaturesFile)
        return failedFeatsFile
    
    def getOutputsDirectory(self):
        ouptutsDir = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_key_outDir)
        return ouptutsDir
    
    def getDestinationDatabaseKey(self, inkey):
        '''
        receives a value that indicates the destination database and
        returns the authoritative key for that destination.  The 
        authoritative key is necessary to retrieve the associated
        parameters / values.
        '''
        retVal = None
        items = self.parser.items(self.const.ConfFileSection_destKeywords)
        for itemTuple in items:
            authoritativeKey = itemTuple[0]
            if authoritativeKey.lower() == inkey.lower():
                retVal = authoritativeKey
                break
            else:
                otherKeys = itemTuple[1].split(',')
                for otherKey in otherKeys:
                    otherKey = otherKey.strip()
                    if otherKey.lower() == inkey.lower():
                        retVal = authoritativeKey
                        break
        return retVal
    
    def getDevelopmentModeCredentialsFileName(self):
        '''        
        Returns the file name string of the .json credential file
        that the script uses to retrieve database credentials from 
        when its in development mode. 
        
        :returns: name of the json file that is used to store database
                  credentials when the script is being developed.
        :rtype: string
        '''
        credsFileName = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_devCredsFile)
        return credsFileName
    
    def getOracleDirectConnectClientString(self):
        clientStr = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_directConnectClientString)
        return clientStr
   
    def getDataBCNodes(self):
        '''
        Gets a list of the computer / node names that the 
        template considers DataBC machines.
        
        (used primarily to determine if the script should 
         be run in development mode, get passwords from 
         ./creds/dbCreds.json, or production which gets the
         passwords directly from pmp)        
        
        :returns: list of computer names that are internal to 
                  databc firewall and have access to pmp.
        :rtype: list(str)
        '''
        nodeString = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_key_govComputers)
        nodeList = nodeString.split(',')
        return nodeList
    
    def getDataBCFmeServerNodes(self):
        nodeString = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_key_govFmeServer)
        nodeList = nodeString.split(',')
        return nodeList

    def getDWMTable(self):
        dwmTab = self.parser.get(self.const.ConfFile_dwm, self.const.ConfFile_dwm_table )
        return dwmTab
    
    def getDWMDbUser(self):
        return self.parser.get(self.const.ConfFile_dwm, self.const.ConfFile_dwm_dbuser)
    
    def getDWMDbInstance(self):
        return self.parser.get(self.const.ConfFile_dwm, self.const.ConfFile_dwm_dbinstance)
    
    def getDWMDbServer(self):
        return self.parser.get(self.const.ConfFile_dwm, self.const.ConfFile_dwm_dbserver)
    
    def getDWMDbPort(self):
        return self.parser.get(self.const.ConfFile_dwm, self.const.ConfFile_dwm_dbport)

    def getSdeConnFilePath(self):
        # getting the name of the sde conn file directory from template config file
        customScriptDir = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_sdeConnFileDir)
        self.logger.debug("customScriptDir: {0}".format(customScriptDir))
        curDir = os.path.dirname(__file__)
        self.logger.debug("curDir: {0}".format(curDir))
        # calcuting the name of the 
        if self.const.AppConfigSdeConnFileExtension[0] <> '.':
            sdeConnFile = '{0}.{1}'.format(self.key, self.const.AppConfigSdeConnFileExtension)
        else:
            sdeConnFile = '{0}{1}'.format(self.key, self.const.AppConfigSdeConnFileExtension)
        
        sdeDir = os.path.join(curDir, customScriptDir)
        sdeConnFileFullPath = os.path.join(sdeDir, sdeConnFile)
        # creating a list of the sde files that should exist
        dbEnvKeys = self.parser.items(self.const.ConfFileSection_destKeywords)
        sdeConnFiles = []
        for elems in dbEnvKeys:
            sdeConnFiles.append(elems[0] + self.const.AppConfigSdeConnFileExtension)
        
        if not os.path.exists(sdeDir):
            msg = 'Expect the directory {0} to exist ' + \
                  'but it does not!  This directory ' + \
                  'should contain .sde connection files. ' + \
                  'For example: {1}. '
            msg = msg.format(sdeDir, ','.join(sdeConnFiles))
            self.logger.error(msg)
            raise IOError, msg
        if not os.path.exists(sdeConnFileFullPath):
            msg = 'Require the SDE connection file to create an SDE ' + \
                  'connection, however the file we are looking for ({0})'  + \
                  'does not exist.  Create the connection file using ' + \
                  'arc catalog and re-run'
            msg = msg.format(sdeConnFileFullPath)
            self.logger.error(msg)
            raise IOError, msg
        return sdeConnFileFullPath

    def isDestProd(self):
        '''
        checks the currently set destination keyword, and 
        returns true or false if it corresponds with a 
        prod destination.
        
        key is in self.key
        '''
        retVal = False 
        if self.key == self.const.ConfFileDestKey_Prod:
            retVal = True
        return retVal
                
    def isDataBCNode(self):
        nodeList = self.getDataBCNodes()
        for indx in range(0, len(nodeList)):
            nodeList[indx] = nodeList[indx].lower()
            nodeList[indx] = nodeList[indx].strip()
        curMachine = platform.node()
        if '.' in curMachine:
            curMachineList = curMachine.split('.')
            curMachine = curMachineList[0]
        curMachine = curMachine.lower()
        retVal = False
        msg = 'current machine is {0} and node list is {1}'
        msg = msg.format(curMachine, nodeList)
        self.logger.debug(msg)
        if curMachine in nodeList:
            retVal = True
        self.logger.debug("isDataBCNode return val: {0}".format(retVal))
        return retVal
    
    def isFMEServerNode(self):
        nodeList = self.getDataBCFmeServerNodes()
        for indx in range(0, len(nodeList)):
            nodeList[indx] = nodeList[indx].lower()
            nodeList[indx] = nodeList[indx].strip()
        curMachine = platform.node()
        if '.' in curMachine:
            curMachineList = curMachine.split('.')
            curMachine = curMachineList[0]
        curMachine = curMachine.lower()
        retVal = False
        msg = 'current machine is {0} and node list is {1}'
        msg = msg.format(curMachine, nodeList)
        self.logger.debug(msg)
        if curMachine in nodeList:
            retVal = True
        self.logger.debug("isFMEServerNode return val: {0}".format(retVal))
        return retVal
    
    def calcEnhancedLoggingFileOutputDirectory(self, fmwDir, fmwName):
        '''
        If running on fme server, then will output a path 
        relative to the template directory, 
        
        If run on a non fme server node then will output a path
        relative to the actual fmw being run.
        
        '''
        justFmwName, fmwSuffix  = os.path.splitext(fmwName)
        #if not self.isFMEServerNode():
        if not self.isDataBCNode():
            outputsDir = self.getOutputsDirectory()
            logDir = self.const.AppConfigLogDir
            fullPath = os.path.join(fmwDir, outputsDir, logDir, justFmwName)
        else:
            templateDir = self.getTemplateRootDirectory()
            outputsDir = self.getOutputsDirectory()
            fullPath = os.path.join(templateDir,outputsDir,'log', justFmwName )
        fullPath = os.path.normpath(fullPath)
        if not os.path.exists(fullPath):
            os.makedirs(fullPath)
        fullPath = fullPath.replace('\\', '/')
        return fullPath
    
class PMPSourceAccountParser(object):
    
    def __init__(self, accntName):
        #ModuleLogConfig()
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)

        self.accntName = accntName
        self.accntList = self.accntName.split('@')
        if len(self.accntList) == 1:
            msg = 'unexpected account name format: {0}'.format(self.accntName)
            raise ValueError, msg
        
    def getSchema(self):
        return self.accntList[0].strip()
    
    def getInstance(self):
        # deprecated
        inst = self.getServiceName()
        return self.accntList[1].strip()
    
    def getServiceName(self):
        '''
        This is the same as getInstance, however the name was changed
        to reflect the idea that we are moving towards using service 
        names instead of instance names to refer to databases
        '''
        self.logger.debug("accntList obj: {0}".format(self.accntList))
        sn = self.accntList[1].strip()
        snList = sn.split(':')
        return snList[0]
    
    def getInstanceNoDomain(self):
        '''
        returns just the instance which is actually the 
        servicename for this account object
        
        changed recently as we have transitioned from using the 
        ETL-OPERATIONAL-DBLINKS resource to EXTERNAL-DB
        '''
        #noDomain = Util.removeDomain(self.accntList[1])
        # return noDomain
        #accntList = self.accntList[1].strip().split('.')
        #return accntList[0].strip()
        # The source format being used in pmp has changed to 
        # s
        connectionEntryList = self.accntList[1].split(':')
        serviceName = connectionEntryList[0]
        serviceNameList = serviceName.split('.')
        serviceNameNoDomain = serviceNameList[0]
        return serviceNameNoDomain
    
class Util(object):
    @staticmethod
    def getComputerName():
        computerName = platform.node()
        if computerName.count('.'):
            computerNameParsedList = computerName.split('.')
            computerName = computerNameParsedList[0]
        return computerName
    
    @staticmethod
    def removeDomain(inString):
        inString = inString.strip()
        stringList = inString.split('.')
        return stringList[0].strip()
    
    @staticmethod
    def calcLogFilePath(fmwDir, fmwName):
        '''
        This method will recieve the full path to where the current 
        fmw that is being processed is located, and the name of the fmw
        that is being processed.  It will then calculate:
          - the full path to the outputs director
          - the full path to the fmw directore (sub dir of outputs)
          - from these paths it will then calculate the relative directory
            from the fmw directory to the output log file and returns 
            this value.
            
        example if the inputs are:
          fmwDir = 'C:\somedir\myFmws'
          fmwName = 'CopyData.fmw'
          
          Then:
           - outputs directory will be: C:\somedir\myFmws\outputs
               (depending on what the value const.AppConfigOutputsDir 
                is set to)
           - log dir path will be: C:\somedir\myFmws\outputs\CopyData
           - and the relative log directory will be:
             ./outputs/CopyData/CopyData.log
               (Which is the relative path for the fmw but not 
                for the python code that is doing the calcuating)
        
         If returnPathList is set to True then the method will 
         return a list with all these paths, the order will be:
           [0] relative path (./outputs/CopyData/CopyData.log)
           [1] full path to log file (C:\somedir\myFmws\outputs\CopyData\CopyData.log)
           [2] log dir (C:\somedir\myFmws\outputs\CopyData)
           [3] outputs (C:\somedir\myFmws\outputs)
           
        '''
        const = TemplateConstants()
        # fmwName = self.fmeMacroVals[self.const.FMWMacroKey_FMWName])
        # fmwDir = fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        #curDir = self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        outDirFullPath = os.path.join(fmwDir, const.AppConfigOutputsDir)
        logDirFullPath = os.path.join(outDirFullPath, const.AppConfigLogDir)
        fmwFileNoExt, fileExt = os.path.splitext(fmwName)
        logDirFullPath = os.path.join(logDirFullPath, fmwFileNoExt)
        del fileExt
        fmwLogFile = fmwFileNoExt + const.AppConfigLogFileExtension
        fullPath2LogFile = os.path.join(logDirFullPath, fmwLogFile)
        fullPath2LogFile = os.path.realpath(fullPath2LogFile)
        
        retVal = fullPath2LogFile
        return retVal
    
    @staticmethod
    def isClassInModule(module, classname):
        retVal = False
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj):
                if name == classname:
                    retVal = True
        return retVal
    
    @staticmethod
    def isEqualIgnoreDomain(param1 , param2):
        param1 = param1.lower().strip()
        param2 = param2.lower().strip()
        
        if param1 == param2:
            return True
        else:
            param1NoDomain = Util.removeDomain(param1)
            param2NoDomain = Util.removeDomain(param2)
            param1NoDomain = param1NoDomain.lower().strip()
            param2NoDomain = param2NoDomain.lower().strip()
            if param1NoDomain == param2NoDomain:
                return True
        return False
        
    @staticmethod
    def calcEnhancedLoggingFileName(fmwName):
        # strip off the .fmw
        fmwFileNameNoSuffix, suffix = os.path.splitext(fmwName)
        const = TemplateConstants()
        enhancedLogKeyword = '_extra'
        logFileNameTmplt = '{0}{1}.{2}'
        logFileName = logFileNameTmplt.format(fmwFileNameNoSuffix, enhancedLogKeyword, 'log')
        return logFileName
    
    @staticmethod
    def getParamValue(paramNameRaw, fmwMacros):
        '''
        When you link parameters together, the parameter ends up being equal 
        to the name of the parameter it is linked to.  Example:
        
        PARAM1 is linked to PARAM2, then when you retrieve PARAM1 it will 
        be equal to $(PARAM2).  This method will retrieve the actual 
        value.
        '''
        logger = logging.getLogger(__name__)
        paramName = paramNameRaw.strip()

        # start by getting the parameter value
        if not paramName in fmwMacros:
            msg = 'Trying to retrieve the published parameter {0} however it is ' + \
                  'undefined in the FMW.  Current values include: {1}'
            raise KeyError, msg.format(paramName, fmwMacros.keys())
        
        paramValue = fmwMacros[paramName]
        
        logger.debug('input param value/name: -{0}/{1}-'.format(paramName, paramValue))
        isParamNameRegex = re.compile('^\$\((.*?)\)$')
        if isParamNameRegex.match(paramValue):
            justParamName = (isParamNameRegex.search(paramValue)).group(1)
            logger.debug('detected parameter {0}'.format(paramValue))
            paramValue = Util.getParamValue(justParamName, fmwMacros)
            print 'Value extracted from linked parameter {0}'.format(paramValue)
            logger.debug('Value extracted from linked parameter {0}'.format(paramValue))
        return paramValue
        
class CalcParamsBase( object ):
    '''
    This method contains the base functionality which 
    consists of retrieval of parameters that come from 
    the template config file.
    
    Password retrieval is defined in inherited classes.
    functionality differs if the script is being developed
    as oppose to if it is in production.
        
    Development mode assumes the following:
        - PMP is unavailable therefor passwords 
          will be retrieved from a hardcoded json file 
    '''
    def __init__(self, fmeMacroVals):
        self.fmeMacroVals = fmeMacroVals
        self.const = TemplateConstants()

        fmwDir = self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        fmwName = self.fmeMacroVals[self.const.FMWMacroKey_FMWName]
        destKey = self.fmeMacroVals[self.const.FMWParams_DestKey]

        ModuleLogConfig(fmwDir, fmwName, destKey)

        #ModuleLogConfig()
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("CalcParamsBase logger name {0}".format(modDotClass))

        self.paramObj = TemplateConfigFileReader(self.fmeMacroVals[self.const.FMWParams_DestKey])
                
        #self.logger = fmeobjects.FMELogFile()  # @UndefinedVariable       
            
    def addPlugin(self, forceDevel=False):
        if forceDevel:
            self.logger.debug("Template is operating in Development mode.")
            self.plugin = CalcParamsDevelopment(self)
        elif self.paramObj.isDataBCNode():
            self.logger.debug("Template is operating in Production mode.")
            # TODO: Should implement an abstract class to ensure that all plugins impement the required methods.
            self.plugin = CalcParamsDataBC(self)
        else:
            self.logger.debug("Template is operating in Development mode.")
            self.plugin = CalcParamsDevelopment(self)
        
    def getFMWLogFileRelativePath(self, create=True):
        logFileFullPath = Util.calcLogFilePath(self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory], 
                             self.fmeMacroVals[self.const.FMWMacroKey_FMWName])
        # retVal = [relativePath,absFullPath, logDirFullPath, outDirFullPath]
        logFileDir = os.path.dirname(logFileFullPath)
        #outDir = pathList.pop()
        #fmwDir = pathList.pop()
        if not os.path.exists(logFileDir) and create:
            os.makedirs(logFileDir)
        #if not os.path.exists(fmwDir) and create:
        #    os.mkdir(fmwDir)
        self.logger.debug("FME LOG FILE TEST STATEMNET")
        #curDir = self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        #outDirFullPath = os.path.join(curDir, self.const.AppConfigOutputsDir)
        #if not os.path.exists(outDirFullPath) and create:
        #    os.mkdir(outDirFullPath)
        #logDirFullPath = os.path.join(outDirFullPath, self.const.AppConfigLogDir)
        #if not os.path.exists(logDirFullPath) and create:
        #    os.mkdir(logDirFullPath)
        #fmwFileNoExt, fileExt = os.path.splitext(self.fmeMacroVals[self.const.FMWMacroKey_FMWName])
        #fmwLogFile = fmwFileNoExt + self.const.AppConfigLogFileExtension
        #relativePath = os.path.join('.', self.const.AppConfigOutputsDir, self.const.AppConfigLogDir, fmwLogFile)
        #return pathList[0]
        return logFileFullPath
        
#    def getDestinationServer(self):
#        #self.logger.logMessageString('Setting the destination server')
#        self.logger.debug("getting the destination server")
#        server = self.paramObj.getDestinationServer()
#        return server
    
    def getDestinationHost(self):
        self.logger.debug("getting the destination host")
        host = self.paramObj.getDestinationHost()
        return host
    
    def getDestinationServiceName(self):
        serviceName = self.paramObj.getDestinationServiceName()
        return serviceName
    
    def getSrcHost(self, position=None):
        '''
        if the fmw has the source host defined as a published parameter
        this method will return it.  If it does not exist then this 
        method will return none
        '''
        srcHost = None
        srcHostMacroKey = self.const.FMWParams_SrcHost
        if position:
            if type(position) is not int:
                msg = 'the arg passwordPosition you provided is {0} which has a type of {1}.  This ' + \
                      'arg must have a type of int.'
                raise ValueError, msg
            srcHostMacroKey = self.const.getSrcHost(position)
        srcHost = Util.getParamValue(srcHostMacroKey, self.fmeMacroVals)
        #if srcHostMacroKey in self.fmeMacroVals:
        #    srcHost = self.fmeMacroVals[srcHostMacroKey]
        return srcHost
    
    def getDestSDEDirectConnectString(self, position=None):
        destSDEConnectString = None
        destHost = self.getDestinationHost()
        destServName = self.getDestinationServiceName()
        # don't need port currently
        oraClientString = self.paramObj.getOracleDirectConnectClientString()
        dirConnectTemplate = 'sde:{0}:{1}/{2}'
        srcSDEDirectConnectString = dirConnectTemplate.format(oraClientString, destHost, destServName)
        self.logger.info("destination direct connect string: {0}".format(srcSDEDirectConnectString))
        return srcSDEDirectConnectString
        
    def getSrcSDEDirectConnectString(self, position=None):
        '''
        This method will look at:
            - SRC_HOST  
            - SRC_ORA_SERVICENAME
            - SRC_ORA_CLIENTSTRING (optional)
        
        Using those parameters will construct a direct connect string
        that looks like this:
        
            - sde:oracle11g:host/service_name
            
        oracle11g is the default value that is used as a client string.  the
        default client string gets defined in the template configuration 
        file in the global section by the property: directconnect_clientstring
        which is described in the application constants: 
          - ConfFileSection_global_directConnectClientString
            
        The client string can be overriden if you define a parameter 
        called SRC_ORA_CLIENTSTRING.  When this parameter is defined it will
        take precidence over the default version.  Hopefully we will not
        actually have to use this parameter.
        
        currently the direct connect does not use the port, but have 
        included  parameters to retrieve the port in case we later on 
        find a direct connect database that requires us to use the port.
        '''
        srcSDEDirectConnectString = None
        srcHostMacroKey = self.const.FMWParams_SrcHost
        srcServiceNameMacroKey = self.const.FMWParams_SrcServiceName
        srcOraClientStringMacroKey = self.const.FMWParams_SrcSDEDirectConnectClientStr
        # not actually used, but retrieving port in case we need it donw 
        # the road.
        #srcPortMacroKey = self.const.FMWParams_SrcPort
        if position:
            if type(position) is not int:
                msg = 'the arg passwordPosition you provided is {0} which has a type of {1}.  This ' + \
                      'arg must have a type of int.'
                raise ValueError, msg
            srcHostMacroKey = self.const.getSrcHost(position)
            srcServiceNameMacroKey = self.const.getSrcServiceNameParam(position)
            srcPortMacroKey = self.const.getSrcPort(position)
            srcOraClientStringMacroKey = self.const.getOraClientString(position)
            
        # have the macrokeys now to retrieve the values
        msg = 'In order to assemble an ESRI SDE Direct Connect string ' + \
              'the following parameters need to be defined: {0}, {1}.  One or ' +\
              'all of these parameters is not defined in your FMW.  Define ' + \
              'and populate these parameters in your fmw and re-run.'
        if srcHostMacroKey not in self.fmeMacroVals or \
           srcServiceNameMacroKey not in self.fmeMacroVals:
            raise ValueError, msg.format(srcHostMacroKey, srcServiceNameMacroKey)
        
        # now get the actual values for host and service name
        srcServiceName = Util.getParamValue(srcServiceNameMacroKey, self.fmeMacroVals)
        srcHost = Util.getParamValue(srcHostMacroKey, self.fmeMacroVals)
        #srcServiceName = self.fmeMacroVals[srcServiceNameMacroKey]
        #srcHost = self.fmeMacroVals[srcHostMacroKey]
        
        # now get the client string
        if srcOraClientStringMacroKey in self.fmeMacroVals:
            #oraClientString = self.fmeMacroVals[srcOraClientStringMacroKey]
            oraClientString = Util.getParamValue(srcOraClientStringMacroKey, self.fmeMacroVals)
        else:
            # use the default, get from the config properties
            oraClientString = self.paramObj.getOracleDirectConnectClientString()
        
        # now construct the direct connect string
        # - sde:oracle11g:host/service_name
        dirConnectTemplate = 'sde:{0}:{1}/{2}'
        srcSDEDirectConnectString = dirConnectTemplate.format(oraClientString, srcHost, srcServiceName)
        self.logger.info("source direct connect string: {0}".format(srcSDEDirectConnectString))
        return srcSDEDirectConnectString
            
    def getSrcEasyConnectString(self, position=None):
        srcEasyConnectString = None
        # retrieveing the correct macro keys.  If position is defined it 
        # denotes which set of host / port / service name parameter combinations 
        # to use.  Example src_host_1, src_port_1, src_ora_servicename_1, or if 
        # posion was defined as 2, then would retrieve from src_host_2, src_port_2,
        # src_ora_servicename_2.  If no position is defined then will just stick
        # with src_ora_servicename, src_host, src_port
        srcHostMacroKey = self.const.FMWParams_SrcHost
        srcServiceNameMacroKey = self.const.FMWParams_SrcServiceName
        srcPortMacroKey = self.const.FMWParams_SrcPort
        if position:
            if type(position) is not int:
                msg = 'the arg passwordPosition you provided is {0} which has a type of {1}.  This ' + \
                      'arg must have a type of int.'
                raise ValueError, msg
            srcHostMacroKey = self.const.getSrcHost(position)
            srcServiceNameMacroKey = self.const.getSrcServiceNameParam(position)
            srcPortMacroKey = self.const.getSrcPort(position)
        
        msg = 'To assemble an easy connect string you must have defined the following ' + \
              'published parameters: {0}, {1} and {2}. One or more of these parameters is ' +\
              'not currently defined in your source FMW.  Please make sure they all ' + \
              'are defined as published parameters.'
        if not srcHostMacroKey in self.fmeMacroVals or \
           not srcServiceNameMacroKey in self.fmeMacroVals:
            raise ValueError, msg.format(srcServiceNameMacroKey, \
                                         srcHostMacroKey, \
                                         srcPortMacroKey)
        
        # if the port is not defined in a published parameter 
        # then use the default port described in the config file
        # (this is the default oracle port number)
        if not srcPortMacroKey in self.fmeMacroVals:
            srcPort = self.paramObj.getDefaultOraclePort()
        else:
            #srcPort = self.fmeMacroVals[srcPortMacroKey]
            srcPort = Util.getParamValue(srcPortMacroKey, self.fmeMacroVals)
        
        # now retrieve the values:
        srcServiceName = Util.getParamValue(srcServiceNameMacroKey, self.fmeMacroVals)
        srcHost = Util.getParamValue(srcHostMacroKey, self.fmeMacroVals)
        #srcServiceName = self.fmeMacroVals[srcServiceNameMacroKey]
        #srcHost = self.fmeMacroVals[srcHostMacroKey]
        
        # fme easy connect sting reason.bcgov:1521/EWRWPRD1.ENV.GOV.BC.CA
        easyConnectString = '{0}:{1}/{2}'.format(srcHost, srcPort, srcServiceName)
        return easyConnectString
    
    def getSrcPort(self, position=None):
        '''
        if the fmw has the source host defined as a published parameter
        this method will return it.  If it does not exist then this 
        method will return none
        '''
        srcPort = None
        srcPortMacroKey = self.const.FMWParams_SrcPort
        if position:
            if type(position) is not int:
                msg = 'the arg passwordPosition you provided is {0} which has a type of {1}.  This ' + \
                      'arg must have a type of int.'
                raise ValueError, msg
            srcPortMacroKey = self.const.getSrcPort(position)
        if srcPortMacroKey in self.fmeMacroVals:
            #srcPort = self.fmeMacroVals[srcPortMacroKey]
            srcPort = Util.getParamValue(srcPortMacroKey, self.fmeMacroVals)
        return srcPort
    
    def getDestinationSDEPort(self):
        port = self.paramObj.getDestinationSDEPort()
        return 'port:{0}'.format(port)
    
    def getDestinationOraclePort(self):
        port = self.paramObj.getDestinationOraclePort()
        return port 
    
    def getSchemaAndServiceNameForPasswordRetrieval(self, passwordPosition=None):
        # going to just get the instance to re-use that code then 
        # replace the instance with the service name
        srcSchema, srcInst = self.getSchemaForPasswordRetrieval(passwordPosition)
        serviceNameMacroKey = self.const.FMWParams_SrcServiceName
        if passwordPosition:
            if type(passwordPosition) is not int:
                msg = 'the arg passwordPosition you provided is {0} which has a type of {1}.  This ' + \
                      'arg must have a type of int.'
                raise ValueError, msg
            serviceNameMacroKey = self.const.getSrcServiceNameParam(passwordPosition)
        return srcSchema, serviceNameMacroKey
    
    def getSchemaForPasswordRetrieval(self, passwordPosition=None):
        schemaMacroKey = self.const.FMWParams_SrcSchema
        instanceMacroKey = self.const.FMWParams_SrcInstance
        proxySchemaMacroKey = self.const.FMWParams_SrcProxySchema
        if passwordPosition:
            if type(passwordPosition) is not int:
                msg = 'the arg passwordPosition you provided is {0} which has a type of {1}.  This ' + \
                      'arg must have a type of int.'
                raise ValueError, msg
            instanceMacroKey = self.const.getSrcInstanceParam(passwordPosition)
            schemaMacroKey = self.const.getSrcSchemaParam(passwordPosition)
            proxySchemaMacroKey = self.const.getSrcSchemaProxyParam(passwordPosition)
        if self.fmeMacroVals.has_key(proxySchemaMacroKey):
            # if proxy key has a value then retrieve the password for the proxy.
            # if it does not then then retrieve the value for the schema
            schemaMacroKey = proxySchemaMacroKey
        return schemaMacroKey, instanceMacroKey

    def getSourcePassword(self, passwordPosition=None):
        '''
        Retrieves the source passwords from PMP. This method is 
        passed onto a plugin depending on whether the template
        has detected a databc server or not.
        
        If run on databc server will look to pmp for the passwords.
        If run in development then will look to the dev json file
        for the passwords.
        
        :param  passwordPosition: This is an optional value that is 
                                  used when there is more than one source
                                  password to be retrieved.  The first one 
                                  will be simply called SRC_ORA_PASSWORD,
                                  
                                  When a second one is required it will be 
                                  called SRC_ORA_PASSWORD_2, and will 
                                  correspond with SRC_ORA_INSTANCE_2 and
                                  SRC_ORA_SCHEMA_2.  This is kinda weird 
                                  but when you want the first schemas,
                                  password,  ie SRC_ORA_SCHEMA, just call 
                                  this method without an arg.
                                  
                                  When you want the password that corresponds
                                  with SRC_ORA_INSTANCE_2, and SRC_ORA_SCHEMA_2
                                  then pass the arg passwordPosition = 2. 
                                  3 for SRC_ORA_INSTANCE_3 etc.
                                  
        :type passwordPosition: int
        
        :returns: the source password
        :rtype: str
        '''
        #schemaMacroKey, instanceMacroKey = self.getSchemaForPasswordRetrieval(passwordPosition)
        schemaMacroKey, serviceName = self.getSchemaAndServiceNameForPasswordRetrieval(passwordPosition)
        msg = 'source password retrieval uses the service name and schema to retrieve ' + \
              'the source password from PMP.  There is no {0} published parameter defined ' + \
              'in the FMW, and you are attempting to retrieve a source password.'
        
        if not serviceName in self.fmeMacroVals:
            raise ValueError, msg.format(serviceName)
        if not schemaMacroKey in self.fmeMacroVals:
            raise ValueError, msg.format(schemaMacroKey)
            
        #inst = self.fmeMacroVals[serviceName]
        #schema = self.fmeMacroVals[schemaMacroKey]
        inst = Util.getParamValue(serviceName, self.fmeMacroVals)
        schema = Util.getParamValue(schemaMacroKey, self.fmeMacroVals)

        pswd = self.plugin.getSourcePassword(passwordPosition)
        msg = "retriving password for the service name {0} schema {1} which are the values in " + \
              "the published parameters {2} and {3}"
        msg = msg.format(inst, schema, serviceName, schemaMacroKey)
        self.logger.info(msg)
        return pswd
    
    def getFailedFeaturesFile(self, failedFeatsFileName=None):
        self.logger.debug("Calling plugin to get the failed features")
        failedFeatures = self.plugin.getFailedFeaturesFile(failedFeatsFileName)
        return failedFeatures
        
    def getDestinationPassword(self):
        pswd = self.plugin.getDestinationPassword()
        return pswd
    
    def getSourcePasswordHeuristic(self, position=None):
        pswd = self.plugin.getSourcePasswordHeuristic(position)
        return pswd
    
    def getDatabaseConnectionFilePath(self):
        '''
        returns the database connection file path, this is a
        relative path to the location of this script
        '''
        customScriptDir = self.paramObj.getSdeConnFilePath()
        return customScriptDir
       
    def isSourceBCGW(self, position=None):
        '''
        Reads the source oracle database instance from the 
        published parameters, compares with the destination
        instances defined for the three environments.  If the 
        source database matches any of the environments, 
        returns the dest_key for the database.
        
        If it does not match it will return None
        
        :returns: Describe the return value
        :rtype: What is the return type
        '''
        # make sure it has the parameter for SRC_ORA_INSTANCE
        # indicating that it is a source oracle instance
        retVal = None
        srcServiceNameParamName = self.const.FMWParams_SrcServiceName
        if position:
            srcServiceNameParamName = self.const.getSrcServiceNameParam(position)
        
        # if the param is linked then it will be equal to the name of the parameter it is 
        # linked to.  This method will detect that, and retrieve the actual value if necessary.
        
        
        if srcServiceNameParamName in self.fmeMacroVals:
            # now get the contents of that parameter
            sourceOraServName = Util.getParamValue(srcServiceNameParamName, self.fmeMacroVals)
            self.logger.debug("Oracle source service name parameter / value: {0}/{1}".format(srcServiceNameParamName, sourceOraServName))
            #sourceOraServName = self.fmeMacroVals[srcServiceNameParamName]
            destKeys = self.paramObj.parser.items(self.const.ConfFileSection_destKeywords)
            for destKeyItems in destKeys:
                destKey = destKeyItems[0]
                #print 'destKey', destKey
                inst = self.paramObj.parser.get(destKey, self.const.ConfFileSection_serviceNameKey)
                instAliases = self.paramObj.parser.get(destKey, self.const.ConfFileSection_instanceAliasesKey)
                instances = instAliases.split(',')
                instances.append(inst)
                for curInstName in instances:
                    self.logger.debug("comparing {0} and {1}".format(curInstName, sourceOraServName))
                    if Util.isEqualIgnoreDomain(curInstName, sourceOraServName):
                        retVal = destKey
                        return retVal
        else:
            msg = 'There is no published parameter in the FMW named {0}.  This parameter' + \
                  'is required for the source to be bcgw.'
            self.logger.warning(msg.format(srcServiceNameParamName))
            
        #msg = "the source oracle service name {0} is not a BCGW instance.  " + \
        #      "It is not defined as an instance for any of the following " + \
        #      "environments: (DLV|TST|PRD) " 
        #msg = msg.format(sourceOraServName)
        #self.logger.debug(msg)
        return retVal
        
class CalcParamsDevelopment(object):
    
    def __init__(self, parent):
        self.parent = parent
        self.const = self.parent.const
        self.fmeMacroVals =self.parent.fmeMacroVals
        self.paramObj = self.parent.paramObj
        
        fmwDir = self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        fmwName = self.fmeMacroVals[self.const.FMWMacroKey_FMWName]
        destKey = self.fmeMacroVals[self.const.FMWParams_DestKey]
        
        ModuleLogConfig(fmwDir, fmwName, destKey)
        
        #ModuleLogConfig()
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.logger.debug("constructing a CalcParamsDevelopment object")
        # This is the base / example db creds file. 
        self.credsFileFullPath = self.getDbCredsFile()
        self.logger.debug("Credentials file being used is: {0}".format(self.credsFileFullPath))
        
        if not os.path.exists(self.credsFileFullPath):
            # The creds file doesn't exist, so raise exception
            # TODO: once the svn url used for distribution is known include it in 
            #       this error message s
            msg = 'Script is running in development mode.  In development mode ' + \
                   'passwords are retrieved from a json file in same directory ' + \
                   'as the fmw.  When searching for the file {0} no file was found ' + \
                   'create this .json file.  Example of the syntax is available ' + \
                   'here: {1}'
            msg = msg.format(self.credsFileFullPath, self.const.svn_DevelopmentJSONFile_Url)
            self.logger.error(msg)
            raise ValueError, msg
        self.logger.debug( 'using the creds file {0}'.format(self.credsFileFullPath))
        with open(self.credsFileFullPath, 'r') as jsonFile:
            self.data = json.load(jsonFile)
            
    def getDbCredsFile(self):
        '''
        This method will:
           a) calculate the path to the current fmw's directory
           b) determine if a dbcreds.json file exists in that directory
           c) if it does not copy the default version down and let the user
              know they need to populate it with the database credentials 
              that they want to use.
        '''
        # this gets just the file name, no path
        exampleCredsFile = self.paramObj.getDevelopmentModeCredentialsFileName()
                
        # calculate the expected location of the creds file, ie the creds file 
        # name in the current fmw path.
        fmwPath = self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        self.logger.debug("fmwPath: {0}".format(fmwPath))
        credsFileFMWPath = os.path.join(fmwPath, exampleCredsFile)
        self.logger.info("using the credentials file: {0}".format(credsFileFMWPath))        
        if not os.path.exists(credsFileFMWPath):
            # calculate the full path to the example dbcreds.json file.
            templateRootDir = self.paramObj.getTemplateRootDirectory()
            confDirName = self.paramObj.getConfigDirName()
            exampleCredsFilePath = os.path.join(templateRootDir, confDirName, exampleCredsFile)
            exampleCredsFilePath = os.path.realpath(exampleCredsFilePath)
            
            
            shutil.copy(exampleCredsFilePath, credsFileFMWPath)
            msg = "You are running this script on a non DataBC computer. " + \
                  "As a result instead of retrieving passwords from our " + \
                  "password application, it expects them in a json file " + \
                  "in the same directory as this fmw: {0}.  A example " + \
                  "credential file has been placed in that direcory called " + \
                  "{1}.  You need to edit this file with the username, password, " + \
                  'and instance information that should be used to connect ' + \
                  'and then '
            msg = msg.format(fmwPath, os.path.basename(exampleCredsFilePath))
            self.logger.error(msg)
            # not worrying about raising an error as it will happen down the 
            # road when the script attempts to find parameters in this file.
        return credsFileFMWPath
        
    def getDestinationPassword(self):
        retVal = None
        self.logger.debug("getting password in development mode")
        destSchema = self.parent.fmeMacroVals[self.const.FMWParams_DestSchema]
        destServiceName = self.parent.getDestinationServiceName()
        
        #destInstance = self.parent.getDestinationInstance()
        #destServiceName = self.parent.getServiceName()
        self.logger.debug("destSchema: {0}".format(destSchema))
        self.logger.debug("destServiceName: {0}".format(destServiceName))
        
        msg = 'getting password for the schema ({0}) / servicename ({1})'
        msg = msg.format(destSchema, destServiceName)
        self.logger.info(msg)
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_DestCreds]:
            self.logger.debug("dbParams: {0}".format(dbParams))
            
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]
            dbServName = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbServName]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]
            
            if dbUser == None or  dbServName == None or dbPass == None:
                msg = "dbuser {0}, dbServName {1}, dbPass {2}.  These values: " + \
                      "are extracted from the file {3}.  The file has not " + \
                      "been properly filled out as one or more of the values " + \
                      "has a 'None' Type"
                ValueError, msg.format(dbUser, dbServName, "*"*len(dbPass),  self.credsFileFullPath)
            
            msg = 'dbuser from credentials file: {0}, dbInstance {1}'
            msg = msg.format(dbUser.lower().strip(), dbServName.lower().strip())
            self.logger.debug(msg)
            
            if dbServName.lower().strip() == destServiceName.lower().strip() and \
               dbUser.lower().strip() == destSchema.lower().strip():
                msg = "Found password in creds file for user ({0}) service name ({1})"
                msg = msg.format((dbUser.lower()).strip(), (dbServName.lower()).strip())
                self.logger.info(msg)
                retVal =  dbPass
                break
            
        if not retVal:
            msg = 'DevMod: Was unable to find a password in the credential file {0} for ' + \
                  'the destSchema: {1} and the instance {2}'
            msg = msg.format(self.credsFileFullPath, destSchema, destServiceName)
            self.logger.error(msg)
            raise ValueError, msg
        
        return retVal
    
    def getSourcePassword(self, position=None):
        '''
        This method will search PMP repository for the password that aligns with
        the parameters:
          - SRC_ORA_SCHEMA
          - SRC_ORA_SERVICENAME
          
        if the parameters 
          - SRC_HOST
          - SRC_PORT
          
        are defined then they will also be used in the password retrieval.
        
        The source password resource stores source passwords as 
        schema@service_name:host:port, thus if the host and port exist they 
        help refine the password search.
        '''
        schemaMacroKey, serviceNameMacroKey = self.parent.getSchemaAndServiceNameForPasswordRetrieval(position)
        #host = self.parent.getSrcHost()
        #port = self.parent.getSrcPort()
        self.logger.debug("servicename: {0} schema: {1}".format(serviceNameMacroKey, schemaMacroKey))
        msg = 'Trying to retrieve the source password, in order to do this ' + \
          'you must define the parameters {0}, and {1}.  One or both of ' + \
          'these is not defined'
        # can't proceed if the schema is not defined in the fmw macros
        if schemaMacroKey not in self.fmeMacroVals:
            raise ValueError, msg.format(schemaMacroKey, serviceNameMacroKey)
        # can't proceed if the servicename is not defined in the fmw macros
        if serviceNameMacroKey not in self.fmeMacroVals:
            raise ValueError, msg.format(schemaMacroKey, serviceNameMacroKey)
            
        srcSchema = self.fmeMacroVals[schemaMacroKey]
        srcServiceName = self.fmeMacroVals[serviceNameMacroKey]

        msg = 'Getting source password for user ({0}) and service name ({1})'
        msg = msg.format(srcSchema, srcServiceName)
        self.logger.info(msg)
        
        retVal = None
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_SourceCreds]:
            self.logger.debug("dbParams: {0}".format(dbParams))
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]
            dbServName = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbServName]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]
            if dbServName.lower().strip() == srcServiceName.lower().strip() and \
               dbUser.lower().strip() == srcSchema.lower().strip():
                retVal =  dbPass
                break
        if not retVal:
            retVal = self.getSourcePasswordHeuristic(position)
            self.logger.debug("heuristic password search found the password {0}".format(retVal))
        if not retVal:
            msg = 'Running in DevMod.  This means that the template is attempting ' +  \
                  'to retrieve the password from the json credential file {4} Was unable to ' + \
                  'retrieve the password for ' + \
                  'the srcSchema: {1} and the source service name {2} in the section {3}'
            msg = msg.format(self.credsFileFullPath, srcSchema, srcServiceName, 
                             self.const.DevelopmentDatabaseCredentialsFile_SourceCreds, 
                             self.credsFileFullPath)
            raise ValueError, msg
        return retVal
    
    def getSourcePasswordHeuristic(self, position=None):
        '''
        For now just ignores the domain when searching for the password
        '''
        retVal = None
        schemaMacroKey, serviceNameMacroKey = self.parent.getSchemaAndServiceNameForPasswordRetrieval(position)
        
        msg = 'Trying to retrieve the source password, in order to do this ' + \
              'you must define the parameters {0}, and {1}.  One or both of ' + \
              'these is not defined'
        if not schemaMacroKey in self.parent.fmeMacroVals:
            raise ValueError, msg.format(schemaMacroKey, serviceNameMacroKey)
        
        if not schemaMacroKey in self.parent.fmeMacroVals:
            raise ValueError, msg.format(schemaMacroKey, serviceNameMacroKey)
            
        srcSchema = self.parent.fmeMacroVals[schemaMacroKey]
        srcServiceName = self.parent.fmeMacroVals[serviceNameMacroKey]
        srcServiceName = Util.removeDomain(srcServiceName)
        
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_SourceCreds]:
            self.logger.debug("dbParmas {0}".format(dbParams))
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]
            dbServName = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbServName]
            dbServName = Util.removeDomain(dbServName)
            
            #dbInstLst = dbInst.split('.')
            #dbInst = dbInstLst[0]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]
            self.logger.debug('inst1 {0}, inst2 {1}'.format(dbServName.lower().strip(), srcServiceName.lower().strip()))
            self.logger.debug("schema1 {0} schema2 {1}".format(dbUser.lower().strip(), srcSchema.lower().strip()))
            if dbServName.lower().strip() == srcServiceName.lower().strip() and \
               dbUser.lower().strip() == srcSchema.lower().strip():
                retVal =  dbPass
                msg = 'found the value for service name {0}, schema {1}'.format(srcServiceName, srcSchema)
                self.logger.debug(msg)
                break
        if not retVal:
            msg = 'Running in DevMod.  This means that the template is attempting ' +  \
                  'to retrieve the password from the json credential file {4} Was unable to ' + \
                  'retrieve the password for ' + \
                  'the srcSchema: {1} and the source service name {2} in the section {3}'
            msg = msg.format(self.credsFileFullPath, srcSchema, srcServiceName, 
                             self.const.DevelopmentDatabaseCredentialsFile_SourceCreds, 
                             self.credsFileFullPath)
            raise ValueError, msg
        return retVal
        
    def getFailedFeaturesFile(self, failedFeatsFileName=None):
        '''
        When run in development mode will look for an 
        ./outputs/failed directory, if it doesn't exist
        it will get created.
        
        The csv file will be failedFeatures.csv
        '''
        fmwDir = self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        #fmwFile = self.fmeMacroVals[self.const.FMWMacroKey_FMWName]
        # params to get from config file
        # outputsbasedir
        # failedFeaturesFile
        outputsDir = self.paramObj.getOutputsDirectory()
        failedFeatsDir = self.paramObj.getFailedFeaturesDir()
        failedFeatsFile = self.paramObj.getFailedFeaturesFile()
        
        self.logger.debug("Starting dir for failed features is: {0}".format(fmwDir))
        
        outDir = os.path.join(fmwDir, outputsDir)
        outDir = os.path.realpath(outDir)
        if not os.path.exists(outDir):
            msg = 'The outputs directory {0} does not exist, creating now'
            self.logger.debug(msg.format(outDir))
            os.makedirs(outDir)
        
        ffDir = os.path.join(outDir, failedFeatsDir)
        ffDir = os.path.realpath(ffDir)
        if not os.path.exists(ffDir):
            msg = 'The failed features directory {0} does not exist, creating now'
            self.logger.debug(msg.format(ffDir))
            os.makedirs(ffDir)
        if not failedFeatsFileName:
            ffFile = os.path.join(ffDir, failedFeatsFileName)
        else:
            ffFile = os.path.join(ffDir, failedFeatsFile)
        msg = 'The failed featured file being used is {0}'
        msg = msg.format(ffFile)
        self.logger.debug(msg)
        return ffFile
        
class CalcParamsDataBC(object):
    
    def __init__(self, parent):
        self.parent = parent
        self.const = self.parent.const
        self.paramObj = self.parent.paramObj
        
        #fmwDir = self.parent.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        #fmwName = self.parent.fmeMacroVals[self.const.FMWMacroKey_FMWName]
        #ModuleLogConfig(fmwDir, fmwName)
        
        #ModuleLogConfig()
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("CalcParamsDataBC logger name: {0}".format(modDotClass))
        self.fmeMacroVals = self.parent.fmeMacroVals
        self.currentPMPResource = None
        
    def getDestinationPassword(self, destKey=None, schema=None):
        if not destKey:
            destKey = self.fmeMacroVals[self.const.FMWParams_DestKey]
        else: 
            self.paramObj.validateKey(destKey)
            destKey = self.paramObj.getDestinationDatabaseKey(destKey)
        self.logger.debug("destination key used in password retrieval: {0}".format(destKey) )
        if not schema:
            if self.const.FMWParams_DestSchema in self.fmeMacroVals:
                schema = self.fmeMacroVals[self.const.FMWParams_DestSchema]
        if not schema:
            msg = 'Unable to retrieve a destination password because there is ' + \
                  'no destination schema defined in the fmw.  Create a parameter ' +\
                  'called {0} and populate it with your destination schema ' + \
                  'and then re-run'
            raise ValueError, msg.format(self.const.FMWParams_DestSchema)
                 
        pmpRes = self.paramObj.getDestinationPmpResource(destKey)
        computerName = Util.getComputerName()
        self.logger.debug("computer name: {0}".format(computerName))
        pmpDict = {'token': self.paramObj.getPmpToken(computerName),
                   'baseurl': self.paramObj.getPmpBaseUrl(), 
                   'restdir': self.paramObj.getPmpRestDir()}
        pmp = PMP.PMPRestConnect.PMP(pmpDict)
        #self.logger.debug("pmp dict used to contruct pmp obj: {0}".format(pmpDict))
        msg = 'retrieving the destination password for schema: ({0}) db env key: ({1})'
        msg = msg.format(schema, destKey)
        self.logger.debug(msg)
        
        passwrd = pmp.getAccountPassword(schema, pmpRes)
        if not passwrd:
            msg = 'Having trouble retrieving password from PMP.  Going to try a ' + \
                  'case insensitive search for the password for the account {0}'
            msg = msg.format(schema)
            self.logger.warning(msg)
        if not passwrd:
            msg = 'Cant find the password in PMP resource {0} for the account {1}'
            msg = msg.format(pmpRes, schema)
            self.logger.warning(msg)
            
        return passwrd
        
    def getPmpDict(self, url=None):
        '''
        retrieves the pmp parameters (values required to connect with service
        like host, port, etc) from the config file and formats them into 
        a dictionary required by the pmp module.
        '''
        if not url:
            url = self.paramObj.getPmpBaseUrl()
        computerName = Util.getComputerName()
        pmpDict = {'token': self.paramObj.getPmpToken(computerName),
                   'baseurl': url,
                   'restdir': self.paramObj.getPmpRestDir()}
        return pmpDict
    
    def getSourcePassword(self, position=None):
        # pmp connection
        pmpDict = self.getPmpDict()
        pmp = PMP.PMPRestConnect.PMP(pmpDict)
        
        # first try to connect using the default pmp params to retrieve 
        # pmp resources, if fail then try alternate url
        try:
            resrcs = pmp.getResources()
        except:
            # failed to communicate with pmp, second attempt, try the alt url
            pmpAltUrl = self.paramObj.getPmpAltUrl()
            msg = 'failed to communicate with PMP, trying the alternative ' + \
                   'url {0}'
            msg = msg.format()
            self.logger.warn(msg.format(pmpAltUrl))
            
            pmpDict = self.getPmpDict(url=pmpAltUrl)
            pmp = PMP.PMPRestConnect.PMP(pmpDict)
            try:
                resrcs = pmp.getResources()
            except:
                msg = 'Communication problem with pmp, tried both these urls ' + \
                      '({0}) ({1})neither is responding'
                url = self.paramObj.getPmpBaseUrl()
                altUrl = self.paramObj.getPmpAltUrl()
                self.logger.error(msg.format(url, altUrl))
                raise
        
        # TODO: Should actually move the logic to retrieve the schema / service name from the fmw into the parent class
        # Getting the schema and servicename...  then host and port
        schemaMacroKey, serviceNameMacroKey = self.parent.getSchemaAndServiceNameForPasswordRetrieval(position)
        #srcHost = self.parent.getSrcHost()
        #srcPort = self.parent.getSrcPort()

        missingParamMsg = 'Trying to retrieve the source password from PMP.  In ' + \
                  'order to do so the published parameter {0} needs to exist ' +\
                  'and be populated.  Currently it does not exist in the FMW ' + \
                  'that is being run.'
        # raise error if source schema not defined in the FMW
        if not self.fmeMacroVals.has_key(schemaMacroKey):
            msg = missingParamMsg.format(schemaMacroKey)
            self.logger.error(msg)
            raise ValueError, msg
        
        # get the source schema      
        # accntNameInFMW = self.fmeMacroVals[schemaMacroKey]
        #srcSchemaInFMW = self.fmeMacroVals[schemaMacroKey]
        srcSchemaInFMW = Util.getParamValue(schemaMacroKey, self.fmeMacroVals)
        
        # raise error if the servicename is not defined as a published parameter in the FMW
        if not serviceNameMacroKey in self.fmeMacroVals:
            msg = missingParamMsg.format(serviceNameMacroKey)
            self.logger.error(msg)
            raise ValueError, msg
        
        # delete this, no longer using the "instance Name" parameter
        #instanceInFMW = self.fmeMacroVals[serviceNameMacroKey]
        #srcServiceNameInFMW = self.fmeMacroVals[serviceNameMacroKey]
        srcServiceNameInFMW = Util.getParamValue(serviceNameMacroKey, self.fmeMacroVals)
        pswd = None

        # Need to detect if the source instance is bcgw.  If it is then
        # get the password from there.
        srcDestKey = self.parent.isSourceBCGW(position)
        if srcDestKey:
            msg = 'Source has been detected to be from the ' + \
                  'bcgw.  Retrieving the password for the ' + \
                  'source database using the destination ' + \
                  'keyword {0}'
            msg = msg.format(srcDestKey)
            self.logger.info(msg)
            pswd = self.getDestinationPassword(srcDestKey, srcSchemaInFMW)
        else:
        
            msg = 'retrieving source password from pmp for schema: ({0}), service name: ({1})'
            msg = msg.format(srcSchemaInFMW, serviceNameMacroKey)
            srcResources = self.paramObj.getSourcePmpResources()
            
            for pmpResource in srcResources:
                self.logger.debug("searching for password in the pmp resource {0}".format(pmpResource))
                self.currentPMPResource = pmpResource
                # start by trying to just retrieve the account using 
                # destSchema@servicename as the "User Account" parameter
                try:
                    #accntATInstance = accntNameInFMW.strip() + '@' + instance.strip()
                    #self.logger.debug("account@instance search string: {0}".format(accntATInstance))
                    # todo, need to modify so it can find the account associated with
                    # user@inst:host:port
                    resId = pmp.getResourceId(pmpResource)
                    if not resId:
                        msg = 'Unable to retrieve a resource id in pmp for the resource name {0} using the token {1}'
                        msg = msg.format(pmpResource, self.token)
                        raise ValueError, msg
                    self.logger.debug("getting account ids")
                    accnts = pmp.getAccountsForResourceID(resId)
                    for accntDict in accnts:
                        accntName = PMPSourceAccountParser(accntDict[self.const.PMPKey_AccountName])
                        schema = accntName.getSchema()
                        #self.logger.debug("cur schema / search schema: {0} / {1}".format(schema, accntNameInFMW))
                        if schema.lower() == srcSchemaInFMW.lower():
                            self.logger.debug("schemas match {0}".format(accntDict[self.const.PMPKey_AccountName]))
                            serviceName = accntName.getServiceName()
                            self.logger.debug("cur service_name / search serivce_name: {0}, {1}".format(serviceName, srcServiceNameInFMW))
                            if serviceName.lower() == srcServiceNameInFMW.lower():
                                # match return password for this account
                                accntId = accntDict[self.const.PMPKey_AccountId]
                                pswd = pmp.getAccountPasswordWithAccountId(accntId, resId)
                                break
                    if not pswd:
                        msg = 'unable to get password for the account name: {0} and ' +\
                              'database service name {1}'
                        raise ValueError, msg.format(srcSchemaInFMW, srcServiceNameInFMW)
                    #pswd = pmp.getAccountPassword(accntATInstance, pmpResource)
                except ValueError:
                    # TODO: Add a proper FME log message here
                    msg = 'There is no account for schema {0} / service name {1} in pmp for the resource {2} using the token {3} from the machine {4}'
                    msg = msg.format(srcSchemaInFMW,
                                     srcServiceNameInFMW,
                                     pmpResource, pmp.token, platform.node())
                    self.logger.warning(msg)
                    self.logger.info("trying a heuristic that ignores domain information to find a password for {0}@{1}".format(srcSchemaInFMW, srcServiceNameInFMW))
                    # Going to do a search for accounts that might match the user
                    pswd = self.getSourcePasswordHeuristic(position)
                if pswd:
                    break
                # add some more warnings
        return pswd
    
    def getSourcePasswordHeuristic(self, position=None):
        '''
        Gets a list of accounts for the pmp resource.  Iterates through 
        the list looking for an account that is similar to the destSchema 
        instance combination defined in the FMW.  PMP stores the account 
        names for pmp in the format destSchema@instance.  This method will parse 
        that into destSchema and instance then look for schemas that match then 
        for instance it will look for matchs independent of any domain.  
        Thus envprod1.nrs.bcgov will match envprod1.env.gov.bc.ca.  
        
        If more than one destSchema / instance is discovered, The method will 
        check to see if they are duplicates by testing to see if both 
        the accounts have the same password.  if they do not the method will
        thow an exception.
          
        :param  pmpResource: Name of the pmp resource to search for the 
                             password in.
        :type pmpResource: string
        
        :returns: The source password from pmp that matches the the current
                  fmws destSchema / instance combination
        :rtype: str
        '''
        #schemaMacroKey, instanceMacroKey = self.parent.getSchemaForPasswordRetrieval(position)
        schemaMacroKey, serviceNameMacroKey = self.parent.getSchemaAndServiceNameForPasswordRetrieval(position)
        
        # verify that the required keys exist
        msg = 'Source password retrieval uses the schema and the service name parameters ' + \
                  'to extract a password from PMP.  The parameter {0} used to describe the ' + \
                  'source {1} is undefined.  Please add to your FMW and re-run.'
        if not schemaMacroKey in self.fmeMacroVals:
            raise ValueError, msg.format(schemaMacroKey, 'schema')
        if not serviceNameMacroKey in self.fmeMacroVals:
            raise ValueError, msg.format(serviceNameMacroKey, 'service name')
        
        # retrieve the source schema and service name from the fmw 
        #srcServiceNameInFMW = self.fmeMacroVals[serviceNameMacroKey].lower().strip()
        #srcSchemaInFMW = self.fmeMacroVals[schemaMacroKey].lower().strip()
        srcServiceNameInFMW = Util.getParamValue(serviceNameMacroKey, self.fmeMacroVals)
        srcSchemaInFMW = Util.getParamValue(schemaMacroKey, self.fmeMacroVals)
        
        # sometimes the source instance includes the domain so making sure this 
        # is stripped out, example idwprod1.env.gov.bc.ca becomes just idwprod1
        srcServiceNameInFMWLst = srcServiceNameInFMW.split('.')
        srcServiceNameInFMW = srcServiceNameInFMWLst[0]
        msg = "Using a heuristic to try to find the password for schema/service name: {0}/{1}"
        self.logger.debug(msg.format(srcSchemaInFMW, srcServiceNameInFMW))
        
        # setting up pmp connection
        pmpResource = self.currentPMPResource
        if not pmpResource:
            srcResources = self.paramObj.getSourcePmpResources()
        else:
            srcResources = [self.currentPMPResource]
        pswd = None
        pmpDict = self.getPmpDict()
        pmp = PMP.PMPRestConnect.PMP(pmpDict)
        
        # iterate through pmp resources / accounts looking for a match
        for pmpResource in srcResources:
            # resource id for the pmp resource.
            resId = pmp.getResourceId(pmpResource)
            # list of accounts attached to the resource
            accounts = pmp.getAccountsForResourceID(resId)
            instList = []
            msg = 'iterating through the accounts in resource id {0}, resource name {1}'
            self.logger.debug(msg.format(resId, pmpResource))
            # source instance, and the source instance less the domain portion
            for accntDict in accounts:
                accntName = PMPSourceAccountParser(accntDict[self.const.PMPKey_AccountName]) # 'ACCOUNT NAME'
                self.logger.debug("account name: {0}".format(accntName.getSchema()))
                schema = accntName.getSchema()
                if schema.lower().strip() == srcSchemaInFMW:
                    # found the srcSchema
                    # now check see if the instance matches
                    #print 'schemas {0} : {1}'.format(destSchema, self.fmeMacroVals[self.const.FMWParams_SrcSchema])
                    self.logger.debug("schemas match {0} {1}".format(schema, srcSchemaInFMW))
                    inst = accntName.getInstanceNoDomain()
                    self.logger.debug("instances {0} {1}".format(inst, srcServiceNameInFMW))
                    if inst.lower().strip() == srcServiceNameInFMW:
                        instList.append([accntDict[self.const.PMPKey_AccountName], accntDict[self.const.PMPKey_AccountId]])
        if instList:
            if len(instList) > 1:
                # get the passwords, if they are the same then return
                # the password
                msg = 'found more than one possible source password match for the service name: {0}'
                msg = msg.format(srcServiceNameInFMW)
                self.logger.warning(msg)
                msg = 'service names that match include {0}'.format(','.join(instList))
                self.logger.debug(msg)
                pswdList = []
                # eliminating any possible duplicates then 
                for accnts in instList:
                    pswdList.append(pmp.getAccountPasswordWithAccountId(accnts[1], resId))
                pswdList = list(set(pswdList))
                if len(pswdList) > 1:
                    msg = 'Looking for the password for the destination schema {0}, and serive name {1}' +\
                          'Found the following accounts that roughly match that combination {2}' + \
                          'pmp has different passwords for each of these.  Unable to proceed as ' + \
                          'not sure which password to use.  Fix this by changing the parameter {3} ' + \
                          'to match a "User Account" in pmp exactly.'
                    msg = msg.format(self.fmeMacroVals[schemaMacroKey], 
                                     self.fmeMacroVals[serviceNameMacroKey], 
                                     ','.join(instList), 
                                     serviceNameMacroKey)
                    raise ValueError, msg
            else:
                # get the password and return it
                pswd = pmp.getAccountPasswordWithAccountId(instList[0][1], resId)
        if not pswd:
            msg = 'unable to find the password using the heuristic search for the ' + \
                  'schema: {0}, service name {1}'
            raise ValueError, msg.format(srcSchemaInFMW, srcServiceNameInFMW)
        return pswd
            
    def getFailedFeaturesFile(self, failedFeatsFileName=None):
        '''
        this is going to put the failed features
        into the template outputs directory.
        '''
        rootDir = self.paramObj.getTemplateRootDirectory()
        outDir = self.paramObj.getOutputsDirectory()
        outDir = os.path.realpath(os.path.join(rootDir, outDir))
        if not os.path.exists(outDir):
            msg = 'creating the output directory {0}'
            msg = msg.format(outDir)
            self.logger.debug(outDir)
            os.makedirs(outDir)
            
        ffDir = self.paramObj.getFailedFeaturesDir()
        ffDir = os.path.join(outDir, ffDir)
        if not os.path.exists(ffDir):
            msg = 'Creating the failed featured directory {0}'
            msg = msg.format(ffDir)
            self.logger.debug(msg)
            os.makedirs(ffDir)
            
        fmwName = self.fmeMacroVals[self.const.FMWMacroKey_FMWName]
        #fmwName = Util.getParamValue(self.const.FMWMacroKey_FMWName, self.fmeMacroVals)
        
        fmwName, suffix = os.path.splitext(fmwName)
        del  suffix
        fmwDir = os.path.join(ffDir, fmwName)
        if not os.path.exists(fmwDir):
            msg = "Creating the failed feature fmw dir {0}"
            msg = msg.format(fmwDir)
            self.logger.debug(msg)
            os.makedirs(fmwDir)
        if not failedFeatsFileName:
            failedFeatsFile = self.paramObj.getFailedFeaturesFile()
            ffFullPathFile = os.path.join(fmwDir, failedFeatsFile )
        else:
            ffFullPathFile = os.path.join(fmwDir, failedFeatsFileName )
        msg = "The full file path for failed features csv file is {0}".format(ffFullPathFile)
        self.logger.debug(msg)
        return ffFullPathFile
            
class CalcParams(CalcParamsBase):
    '''
    This class exists to populate scripted parameters
    defined in the fmw.
    
    No real functionality exists in this module.  Functionality
    exists in two other classes:
      - CalcParamsBase
      - or via a plugin.
      
    The module is set up this way to enable us to have 
    base functionality that applies in all cases, and then 
    plugin functionality that works differently depending on 
    whether we are working in a development mode and/or 
    environment, vs a databc environment.
    
    In development mode you may be connecting to your own 
    database, pmp will be unavailable, etc.  
    
    In production environment, the assumption is you 
    can connect to databc servers, and databases, pmp etc
      
    Room for improvement on how this is impelmented.
    Initially tried to set it up with conditional 
    inheritance but found that it didn't work with 
    the way I wanted it to.  The plugin approach was
    the next best thing.
    
    To implement new functionality as a plugin it needs
    to be implmeneted in both of the plugin classes, but 
    should also be implmeneted in the base module.  Good 
    example is the getDestinationPassword() method.  Includes
    versions in both the plugins, as well the base class.
    
    Base class calls plugin.getDestinationPassword().  This 
    works cause plugin is set when the class is instantiated, 
    and then points to either the dev module or the prod one.
    '''
    
    def __init__(self, fmeMacroVals, forceDevelMode=False):
        self.const = TemplateConstants()
        fmwDir = fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        fmwName = fmeMacroVals[self.const.FMWMacroKey_FMWName]
        destKey = fmeMacroVals[self.const.FMWParams_DestKey]
        ModuleLogConfig(fmwDir, fmwName, destKey)
        
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.logger.info("inheriting the CalcParamsBase class")
        CalcParamsBase.__init__(self, fmeMacroVals)
        self.logger.debug("adding plugin functionality")
        self.addPlugin(forceDevelMode)
        
class ModuleLogConfig(object):
    def __init__(self, fmwDir, fmwName, destKey=None):
        if not destKey:
            destKey = 'DEV'
        logFileFullPath = Util.calcLogFilePath(fmwDir, fmwName)
        # get the tmpLog to test to see if the logger has been 
        # initialized yet or not
        tmpLog = logging.getLogger(__name__)
        if not tmpLog.handlers:
            logging.logFileName = logFileFullPath
            const = TemplateConstants()
            confFile = TemplateConfigFileReader(destKey)
            
            # Get the log config file name from the app config file
            logConfFileName = confFile.getApplicationLogFileName()
            
            # get the name of the conf dir
            configDir = const.AppConfigConfigDir
            dirname = os.path.dirname(__file__)
            logConfFileFullPath = os.path.join( dirname,configDir,logConfFileName )
            
            enhancedLoggingFileName = Util.calcEnhancedLoggingFileName(fmwName)
            enhancedLoggingDir = confFile.calcEnhancedLoggingFileOutputDirectory(fmwDir, fmwName)
            enhancedLoggingFullPath = os.path.join(enhancedLoggingDir, enhancedLoggingFileName)
            enhancedLoggingFullPath = os.path.realpath(enhancedLoggingFullPath)
            enhancedLoggingFullPath = enhancedLoggingFullPath.replace(os.path.sep, '/')
            if not os.path.exists(enhancedLoggingFullPath):
                fh = open(enhancedLoggingFullPath, 'w')
                fh.close()
            
            print 'type(enhancedLoggingFullPath)', type(enhancedLoggingFullPath)
            print 'logConfFileFullPath', logConfFileFullPath
            print 'enhancedLoggingFullPath', enhancedLoggingFullPath
            print 'os.path.sep', os.path.sep
            
            logging.config.fileConfig(logConfFileFullPath, defaults={'logfilename': str(enhancedLoggingFullPath)})
            logger = logging.getLogger(__name__)
            logger.debug("logger should be configured")
            logger.debug("log name: {0}".format(__name__))
            logger.debug("enhancedLoggingFullPath: {0}".format(enhancedLoggingFullPath))
        else:
            tmpLog.debug("Loggin has already been configured")
            pass
            
class DWMWriter(object ):
    '''
    Things that were logged by the other logger:
      a) mappingFileID
      b) startTime -  reads the log file and strips out 
                      the start time.  Looks for it in 
                      the log file.  Strips it from the 
                      first line
                      
                      might be able to calc using the elapsed
                      time
                      
      c) endTime - The last line in the file, end time will 
                   be the time that DWM writer was started.
                   
      d) exitStatus - set to 'OK | Error'
      
      e) totalFeaturesWritten - from self.fme.totalFeaturesWritten
      
      f) features rejected - This is the logic found in the lib, 
                             info is being parsed from the log file
                             
            featuresRejectedCount = '-99'
            if os.path.isfile(logFile):
                    for line in fileinput.input(logFile):
                        if re.search('Stored ', line):
                            if re.search(r"(?<=Stored )\d+", line):
                                m = re.search(r"(?<=Stored )\d+", line)
                                featuresRejectedcount = m.group(0)
      
      g) notification Email. - Not sure why we need this.  Thinking 
                            notifications should be set up with the 
                            
      h) logfile - Thinking this should be the restapi link through fme
                   server
                   
      i) Destination instance - Get from the published parameters
      
      j) destSchema - Get from published parameters
      
      k) dest_table - Get from published parameters (what about multiple
                      destinations?)
                      
                      could do a read of the actual fmw's xml and 
                      from there draw lines between sources and destinations
                      but this would require more work.  Other idea, maybe
                      from log.
                      
      l) data source - retrieved from fme.featuresRead
                       if not from there then try the workbench macros?
                       Need to be able to relate back features read, written
                       etc
                    
      m) duration - elapsedRunTime
      
        params = {}
        params['FME_WORKBENCH'] = str(mappingFileID)
        params['COMMENTS'] = str(exitStatus)
        params['TOTAL_FEATURES_READ'] = str(featuresReadCount)
        params["TOTAL_FEATURES_READ"] = str(featuresReadCount)
        params["TOTAL_FEATURES_WRITTEN"] = str(featuresWrittenCount)
        params["DEST_INSTANCE"] = str(destInstance)
        params["DEST_SCHEMA"] = str(destSchema)
        params['DEST_LAYER'] = str(destTable)
        params["SOURCE_FEAT"] = str(dataSource)
        params["SECONDS_ELAPSED"] = str(duration)
            
    # - try to capture source data type - fgdb or sde30 etc.
    # - 
    
    - logger writes features to IDWPROD APP_UTILITY
    
    '''
    #TemplateConfigFileReader
    def __init__(self, fme, const=None):
        self.const = const
        if not self.const:
            self.const = TemplateConstants()
        self.fme = fme
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        destKey = self.fme.macroValues[self.const.FMWParams_DestKey]
        self.config = TemplateConfigFileReader(destKey)
        self.getDatabaseConnection()
        
    def getDatabaseConnection(self):
        computerName = Util.getComputerName()
        pmpDict = {'token': self.config.getPmpToken(computerName),
                   'baseurl': self.config.getPmpBaseUrl(), 
                   'restdir': self.config.getPmpRestDir()}
        # TODO: modify the logging config file so that it captures both the pmp and the database log parameters
        pmp = PMP.PMPRestConnect.PMP(pmpDict)
        accntName = self.config.getDWMDbUser()
        serviceName = self.config.getDestinationServiceName()
        
        #instance = self.config.getDestinationServiceName()
        pmpResource = self.config.getDestinationPmpResource()
        passwrd = pmp.getAccountPassword(accntName, pmpResource)
        if isinstance(passwrd, unicode):
            # when attempting to connect to database using unicode encoded password
            # am getting a error message:
            # "argument 2 must be str, not unicode"
            # this is an attempt to addresss that problem
            passwrd = str(passwrd)
        self.logger.debug(u"accntName: {0}".format(accntName))
        self.logger.debug(u"service name: {0}".format(serviceName))
        self.db = DB.DbLib.DbMethods()
        try:
            self.db.connectParams(accntName, passwrd, serviceName)
        except Exception, e:
            try:
                self.logger.warning(str(e))
                host = self.config.getDestinationHost()
                msg = u'unable to create a connection to the schema: {0}, instance {1} ' + \
                      u'going to try to connect directly to the server: {2}'
                msg = msg.format(accntName, serviceName, host)
                self.logger.warning(msg)
                port = self.config.getDestinationOraclePort()
                self.logger.debug(u"port: {0}".format(port))
                self.logger.debug(u"host: {0}".format(host))
                self.db.connectNoDSN(accntName, passwrd, serviceName, host, port)
                self.logger.debug(u"successfully connected to database using direct connect")
                # TODO: Should really capture the specific error type here
            except Exception, e:
                self.logger.error(u"unable to create the database connection, error message to follow...")
                self.logger.error(u"error message: {0}".format(sys.exc_info()[0]))
                #self.logger.info(msg)
                msg = u'database connection used to write to DWM has failed, ' + \
                      u'dwm record for this replication will not be written'
                self.logger.error(msg)
                raise
        else:
            msg = 'successfully connected to the database {0} with the user {1}'
            msg = msg.format(serviceName, accntName)
            self.logger.info(msg)
                        
    def writeRecord(self):
        '''
        Original logger would only report on a single feature.  For the short
        term am going to change things around so that if there are more 
        '''
        dwmRecord = self.collectData()
        insertState = self.getInsertStatement()
        try:
            self.db.executeOracleSql(insertState, dwmRecord)
            self.db.commit()
            self.logger.info("DWM Record sucessfully written")
        except Exception, e:
            msg = 'unable to write the DWM record to the database'
            self.logger.error(msg)
            self.logger.error(str(e))
            raise
        
    def collectData(self):
        returnDict = {}
        returnDict['mapping_file_id'] = self.getMapFileId()
        returnDict['start_time'] = self.getStartTime()
        returnDict['end_time'] = self.getEndTime()
        returnDict['exit_status'] = self.getExitStatus()
        returnDict['features_read_count'] = self.getTotalFeaturesReadCount()
        returnDict['features_written_count'] = self.getTotalFeaturesWrittenCount()
        returnDict['features_rejected_count'] = self.getTotalFeaturesRejectedCount()
        returnDict['notification_email'] = self.getNotificationEmail()
        returnDict['log_filename'] = self.getLogFileName()
        returnDict['datasource'] = self.getDataSource()
        returnDict['duration'] = self.getDuration()
        # Weakness in DWM, set up to use instance.  To resolve we are just writing the destination
        # servicename to the DWM record
        returnDict['dest_instance'] = self.getDestServiceName()
        returnDict['dest_schema'] = self.getDestSchema()
        returnDict['dest_table'] = self.getDestTable()        
        return returnDict
        
    def getInsertStatement(self):
        tableName = self.config.getDWMTable()
        insertStatement = 'INSERT INTO {0} ( ' + \
                          ' MAPPING_FILE_ID, ' + \
                          ' START_TIME, ' + \
                          ' END_TIME, ' + \
                          ' EXIT_STATUS, ' + \
                          ' FEATURES_READ_COUNT, ' + \
                          ' FEATURES_WRITTEN_COUNT, ' + \
                          ' FEATURES_REJECTED_COUNT, ' + \
                          ' NOTIFICATION_EMAIL, ' + \
                          ' LOG_FILENAME, ' + \
                          ' DATASOURCE, ' + \
                          ' DURATION, ' + \
                          ' DEST_INSTANCE, ' + \
                          ' DEST_SCHEMA, ' + \
                          ' DEST_TABLE ) ' + \
                          ' VALUES ( ' + \
                          ' :mapping_file_id, ' + \
                          ' :start_time, ' + \
                          ' :end_time,  ' + \
                          ' :exit_status, ' + \
                          ' :features_read_count, ' + \
                          ' :features_written_count, ' + \
                          ' :features_rejected_count, ' + \
                          ' :notification_email, ' + \
                          ' :log_filename, ' + \
                          ' :datasource, ' + \
                          ' :duration, ' + \
                          ' :dest_instance, ' + \
                          ' :dest_schema, ' + \
                          ' :dest_table )'
        insertStatement = insertStatement.format(tableName)
        return insertStatement
    
    def getMapFileId(self):
        '''
        
        '''
        mapFileId = self.fme.mappingFileId
        if not mapFileId:
            # in case its not there then try to get it from the 
            # macroValues
            mapFileId = self.fme.macroValues[self.const.FMWMacroKey_FMWName]
            mapFileId, extension = os.path.splitext(mapFileId)
            del extension
            # could also get from WORKSPACE_NAME
        return mapFileId
    
    def getStartTime(self):
        '''
        going to calculate this using the elapsed time, 
        less the current time.
        '''
        startTime = datetime.datetime.now()
        if self.fme.elapsedRunTime:
            currentDateTime = datetime.datetime.now()
            elapsedTime = datetime.timedelta(seconds=float(self.fme.elapsedRunTime))
            startTime = currentDateTime - elapsedTime
        return startTime
    
    def getEndTime(self):
        return datetime.datetime.now()
       
    def getExitStatus(self):
        '''
        self.fme.elapsedRunTime
        '''
        exitStatus = 'Error'
        if self.fme.status:
            exitStatus = 'OK'
        return exitStatus
    
    def getTotalFeaturesReadCount(self):
        return self.fme.totalFeaturesRead
    
    def getTotalFeaturesWrittenCount(self):
        return self.fme.totalFeaturesWritten
     
    def getTotalFeaturesRejectedCount(self):
        return None
    
    def getNotificationEmail(self):
        return None
    
    def getLogFileName(self):
        '''
        How it was done: 
        
        logFilename = os.path.abspath(logFile)
        logFilename = logFilename.replace('\\','%5C')
        logFilename = logFilename.replace(' ','+')
        '''
        # TODO: figure out how fme server fills in the logfilename, might require using the fmeserver published parameters that are part of the new fmeserver to retrieve the log file url.
        logFile = self.fme.logFileName
        #if logFile:
        #    logFile = os.path.abspath(logFile)
        #    logFile = urllib.quote(logFile)
        return logFile
    
    def getDataSource(self):
        '''
        OMG!  its another cludge, just appending 
        datasources together with ,+ as the delimiter!
        '''
        dataSrcStr = 'Python_shutdown_can_not_get_DataSource'
        dataSourceList = self.fme.featuresRead.keys()
        if dataSourceList:
            dataSrcStr = ',+'.join(dataSourceList)
        if len(dataSrcStr) >= 180:
            # oracle table for this only acccepts 180 characters, if the lenght is greater than
            # 180 then try to remove the path from all
            dataSrcTruncatedDirs = []
            dirlist = []
            for datasrc in dataSourceList:
                justDir = os.path.dirname(datasrc)
                if not justDir in dirlist:
                    dataSrcTruncatedDirs.append(datasrc)
                    dirlist.append(justDir)
                else:
                    dataSrcTruncatedDirs.append(os.path.basename(datasrc))
            dataSrcStr = ',+'.join(dataSrcTruncatedDirs)
            if len(dataSrcStr) > 180:
                dataSrcStr = dataSrcStr[0:175] + '...'
        return dataSrcStr
            
    def getDuration(self):
        return self.fme.elapsedRunTime
        
    def getDestServiceName(self):
        # retrieve from the published parameters:
        destServName = None
        if self.fme.macroValues.has_key(self.const.FMWParams_DestServiceName):
            destServName = self.fme.macroValues[self.const.FMWParams_DestServiceName]
        else:
            for macroKey in self.fme.macroValues:
                if macroKey.upper() == self.fme.macroValues[macroKey].upper():
                    destServName = self.fme.macroValues[macroKey]
                    break
        return destServName
     
    def getDestSchema(self):
        return self.fme.macroValues[self.const.FMWParams_DestSchema]
    
    def getDestTable(self):
        # again only captures the first feature class
        destTable = None
        matchExpr = '^{0}.*$'
        matchExpr = matchExpr.format(self.const.FMWParams_DestFeatPrefix)
        for macroKey in self.fme.macroValues.keys():
            if re.match(matchExpr, macroKey, re.IGNORECASE):
                destTable = self.fme.macroValues[macroKey]
                # TODO: Ideally should modify things so that we can report on more than one destination
                break
        return destTable
     
    def printParams(self):
        titleLine = '----  {0} -----'
        print titleLine.format('elapsedRunTime')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.elapsedRunTime)
        print titleLine.format('featuresRead')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.featuresRead)
        print titleLine.format('featuresWritten')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.featuresWritten)
        print titleLine.format('logFileName')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.logFileName)
        print titleLine.format('macroValues')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.macroValues)
        print titleLine.format('mappingFileId')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.mappingFileId)
        print titleLine.format('numFeaturesLogged')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.numFeaturesLogged)
        print titleLine.format('status')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.status)
        print titleLine.format('totalFeaturesRead')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.totalFeaturesRead)
        print titleLine.format('totalFeaturesWritten')
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.fme.totalFeaturesWritten)
        