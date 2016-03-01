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
import sys
import importlib
import os
import pprint

# for fmeobjects to import properly the fme dir has 
# to be closer to the front of the pathlist
# TODO: have a autodetect for 2015 and send an error if
# not running under fme2015
pathList = os.environ['PATH'].split(';')
pathList.insert(0, r'E:\sw_nt\FME2015')
sys.path.insert(0, r'E:\sw_nt\FME2015\fmeobjects\python27')
sys.path.insert(0, r'\\data.bcgov\work\scripts\python\DataBCPyLib')
os.environ['PATH'] = ';'.join(pathList)

import site
import fmeobjects
import platform
import ConfigParser
import json
import logging.config
import PMP.PMPRestConnect
import DB.DbLib
import FMELogger
import datetime
import urllib
import re
import cx_Oracle
import inspect

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
    ConfFileSection_global_configDirName = 'configdirname'
    ConfFileSection_global_devCredsFile = 'development_credentials_file'
    ConfFileSection_global_customScriptDir = 'customizescriptdir'
    ConfFileSection_global_changeLogDir = 'changelogsdir'
    ConfFileSection_global_changeLogFileName = 'changelogsfilename'
    ConfFileSection_global_sdeConnFileDir = 'sdeConnectionDir'

    ConfFileSection_destKeywords = 'dest_param_keywords'
    
    # properties from the config file.
    ConfFileSection_serverKey = 'server'
    ConfFileSection_pmpResKey = 'pmpresource'
    ConfFileSection_oraPortKey = 'oracleport'
    ConfFileSection_sdePortKey = 'sdeport'
    ConfFileSection_instanceKey = 'instance'
    
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
    FMWParams_DestInstance = 'DEST_INSTANCE'
    FMWParams_DestPassword = 'DEST_PASSWORD'
    
    # published parameters - source
    FMWParams_srcDataSet = 'SRC_DATASET_' # prefix for any file based source dataset
    FMWParams_SrcFGDBPrefix = 'SRC_DATASET_FGDB_'
    FMWParams_SrcXLSPrefix = 'SRC_DATASET_XLS_'
    FMWParams_SrcFeaturePrefix = 'SRC_FEATURE_'
    FMWParams_SrcSchema = 'SRC_ORA_SCHEMA'
    FMWParams_SrcInstance = 'SRC_ORA_INSTANCE'
    FMWParams_SrcFeatPrefix = 'SRC_FEATURE_'
    
    FMWParams_FileChangeEnabledParam = 'FILE_CHANGE_DETECTION'
    
    # TODO: define the source database parameters
    
    # The fmw macrovalue used to retrieve the directory 
    # that the fmw is in.
    FMWMacroKey_FMWDirectory = 'FME_MF_DIR'
    FMWMacroKey_FMWName = 'FME_MF_NAME'
    
    # pmp config parameters
    ConfFileSection_pmpConfig = 'pmp_server_params'
    ConfFileSection_pmpConfig_baseurl = 'baseurl'
    ConfFileSection_pmpConfig_restdir = 'restdir'
    ConfFileSection_pmpSrc = 'pmp_source_info'
    ConfFileSection_pmpSrc_resources = 'sourceresource'
    
    # development mode json database params file
    DevelopmentDatabaseCredentialsFile = 'dbCreds.json'
    DevelopmentDatabaseCredentialsFile_DestCreds = 'destinationCreds'
    DevelopmentDatabaseCredentialsFile_SourceCreds = "sourceCredentials"
    DevelopmentDatabaseCredentialsFile_dbUser = 'username'
    DevelopmentDatabaseCredentialsFile_dbInst = 'instance'
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
    
class Start(object):
    
    def __init__(self, fme):
        # getting the app constants
        self.const = TemplateConstants()
        fmwDir = fme.macroValues[self.const.FMWMacroKey_FMWDirectory]
        fmwName = fme.macroValues[self.const.FMWMacroKey_FMWName]
        # set up logging
        ModuleLogConfig(fmwDir, fmwName)        
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
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
        ModuleLogConfig(fmwDir, fmwName)
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
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
        # set the plugin parameter = to the Start() object.
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
     
class DefaultShutdown(object):
    def __init__(self, fme):
        self.fme = fme
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        
    def shutdown(self):
        self.logger.info("Enabling the DWMWriter")
        dwmWriter = DWMWriter(self.fme)
        #dwmWriter.printParams()
        dwmWriter.writeRecord()
        
class TemplateConfigFileReader(object):
    
    parser = None
    key = None
    
    def __init__(self, key, confFile=None):
        #ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
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
            
    def getDestinationServer(self):
        server = self.parser.get(self.key, self.const.ConfFileSection_serverKey)
        return server        
        
    def getSourcePmpResources(self):
        srcPmpResources = self.parser.get(self.const.ConfFileSection_pmpSrc, self.const.ConfFileSection_pmpSrc_resources)
        srcPmpResourcesList = srcPmpResources.split(',')
        # getting rid of leading/trailing spaces on each element in the list
        for cnter in range(0, len(srcPmpResourcesList)):
            srcPmpResourcesList[cnter] = srcPmpResourcesList[cnter].strip()
        return srcPmpResourcesList
        
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
    
    def getDestinationInstance(self):
        inst = self.parser.get(self.key, self.const.ConfFileSection_instanceKey)
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
    
    def getPmpRestDir(self):
        restDir = self.parser.get(self.const.ConfFileSection_pmpConfig, self.const.ConfFileSection_pmpConfig_restdir)
        return restDir
    
    def getConfigDirName(self):
        confDirName = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_configDirName)
        return confDirName
    
    def getDestinationDatabaseKey(self, inkey):
        '''
        recieves a value that indicates the destination database and
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
   
    def getDataBCNodes(self):
        nodeString = self.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_key_govComputers)
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
    
class PMPSourceAccountParser(object):
    
    def __init__(self, accntName):
        #ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.accntName = accntName
        self.accntList = self.accntName.split('@')
        if len(self.accntList) == 1:
            msg = 'unexpected account name format: {0}'.format(self.accntName)
            raise ValueError, msg
        
    def getSchema(self):
        return self.accntList[0].strip()
    
    def getInstance(self):
        return self.accntList[1].strip()
    
    def getInstanceNoDomain(self):
        noDomain = Util.removeDomain(self.accntList[1])
        #accntList = self.accntList[1].strip().split('.')
        #return accntList[0].strip()
        return noDomain
    
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
    def calcLogFilePath(fmwDir, fmwName, returnPathList=False):
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
        del fileExt
        fmwLogFile = fmwFileNoExt + const.AppConfigLogFileExtension
        absFullPath = os.path.join( logDirFullPath, fmwLogFile)
        relativePath = os.path.join('.', const.AppConfigOutputsDir, const.AppConfigLogDir, fmwLogFile)
        # making sure all output paths are properly formatted
        relativePath = os.path.realpath(relativePath)
        absFullPath = os.path.realpath(absFullPath)
        logDirFullPath = os.path.realpath(logDirFullPath)
        outDirFullPath = os.path.realpath(outDirFullPath)
        
        retVal = None
        if returnPathList:
            retVal = [relativePath,absFullPath, logDirFullPath, outDirFullPath]
        else:
            retVal = relativePath
        return retVal
    
    @staticmethod
    def isClassInModule(module, classname):
        retVal = False
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj):
                if name == classname:
                    retVal = True
        return retVal

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
        ModuleLogConfig(fmwDir, fmwName)

        #ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.paramObj = TemplateConfigFileReader(self.fmeMacroVals[self.const.FMWParams_DestKey])
                
        #self.logger = fmeobjects.FMELogFile()  # @UndefinedVariable       
        
    def addPlugin(self, forceDevel=False):
        if forceDevel:
            self.plugin = CalcParamsDevelopment(self)
        elif self.paramObj.isDataBCNode():
            self.logger.debug("Template is operating in Production mode.")
            # TODO: Should implement an abstract class to ensure that all plugins impement the required methods.
            self.plugin = CalcParamsDataBC(self)
        else:
            self.logger.debug("Template is operating in Development mode.")
            self.plugin = CalcParamsDevelopment(self)
        
    def getFMWLogFileRelativePath(self, create=True):
        pathList = Util.calcLogFilePath(self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory], 
                             self.fmeMacroVals[self.const.FMWMacroKey_FMWName], 
                             True)
        # retVal = [relativePath,absFullPath, logDirFullPath, outDirFullPath]
        outDir = pathList.pop()
        fmwDir = pathList.pop()
        if not os.path.exists(outDir) and create:
            os.mkdir(outDir)
        if not os.path.exists(fmwDir) and create:
            os.mkdir(fmwDir)
            
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
        return pathList[0]
        
    def getDestinationServer(self):
        #self.logger.logMessageString('Setting the destination server')
        self.logger.debug("getting the destination server")
        server = self.paramObj.getDestinationServer()
        return server
    
    def getDestinationInstance(self):
        instance = self.paramObj.getDestinationInstance()
        return instance
    
    def getDestinationSDEPort(self):
        port = self.paramObj.getDestinationSDEPort()
        return 'port:{0}'.format(port)
    
    def getDestinationOraclePort(self):
        port = self.paramObj.getDestinationOraclePort()
        return port 
    
    def getSourcePassword(self):
        pswd = self.plugin.getSourcePassword()
        msg = "retriving password for the instance {0}"
        msg = msg.format( self.fmeMacroVals[self.const.FMWParams_SrcInstance] )
        self.logger.info(msg)
        return pswd
        
    def getDestinationPassword(self):
        pswd = self.plugin.getDestinationPassword()
        return pswd
    
    def getSourcePasswordHeuristic(self):
        pswd = self.plugin.getSourcePasswordHeuristic()
        return pswd
    
    def getDatabaseConnectionFilePath(self):
        '''
        returns the database connection file path, this is a
        relative path to the location of this script
        '''
        customScriptDir = self.paramObj.getSdeConnFilePath()
        return customScriptDir
        

class CalcParamsDevelopment(object):
    
    def __init__(self, parent):
        self.parent = parent
        self.const = self.parent.const
        self.paramObj = self.parent.paramObj
        
        fmwDir = self.parent.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        fmwName = self.parent.fmeMacroVals[self.const.FMWMacroKey_FMWName]
        ModuleLogConfig(fmwDir, fmwName)
        
        #ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.logger.debug("constructing a CalcParamsDevelopment object")
        confDirName = self.paramObj.getConfigDirName()
        credsFileName = self.paramObj.getDevelopmentModeCredentialsFileName()        
        # expects the creds file to be ./$(confDirName)/$(credsFileName)
        credsFileFullPath = os.path.join(self.parent.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory], confDirName, credsFileName)
        self.credsFileFullPath = os.path.realpath(credsFileFullPath)
        self.logger.info("using the credentials file: {0}".format(credsFileFullPath))
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
        with open(credsFileFullPath, 'r') as jsonFile:
            self.data = json.load(jsonFile)
        
    def getDestinationPassword(self):
        retVal = None
        
        destSchema = self.parent.fmeMacroVals[self.const.FMWParams_DestSchema]
        destInstance = self.parent.getDestinationInstance()
        
        msg = 'getting password for the schema ({0}) / instance ({1})'
        msg = msg.format(destSchema, destInstance)
        self.logger.info(msg)
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_DestCreds]:
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]
            dbInst = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbInst]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]
            
            msg = 'dbuser from credentials file: {0}, dbInstance {1}'
            msg = msg.format(dbUser.lower().strip(), dbInst.lower().strip())
            self.logger.debug(msg)
            if dbInst.lower().strip() == destInstance.lower().strip() and \
               dbUser.lower().strip() == destSchema.lower().strip():
                msg = "Found password in creds file for user ({0}) instance ({1})"
                msg = msg.format(dbUser.lower().strip(), dbInst.lower().strip())
                self.logger.info(msg)
                retVal =  dbPass
                break
        if not retVal:
            msg = 'DevMod: Was unable to find a password in the credential file {0} for ' + \
                  'the destSchema: {1} and the instance {2}'
            msg = msg.format(self.credsFileFullPath, destSchema, destInstance)
            self.logger.error(msg)
            raise ValueError, msg
        return retVal
    
    def getSourcePassword(self):
        
        srcSchema = self.parent.fmeMacroVals[self.const.FMWParams_SrcSchema]
        srcInstance = self.parent.fmeMacroVals[self.const.FMWParams_SrcInstance]
        msg = 'Getting source password for user ({0}) and instance ({1})'
        msg = msg.format(srcSchema, srcInstance)
        self.logger.info(msg)
        
        retVal = None
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_SourceCreds]:
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]
            dbInst = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbInst]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]
            if dbInst.lower().strip() == srcInstance.lower().strip() and \
               dbUser.lower().strip() == srcSchema.lower().strip():
                retVal =  dbPass
                break
        if not retVal:
            msg = 'Running in DevMod.  This means that the template is attempting ' +  \
                  'to retrieve the password from the json credential file {4} Was unable to ' + \
                  'retrieve the password for ' + \
                  'the srcSchema: {1} and the source instance {2} in the section {3}'
            msg = msg.format(self.credsFileFullPath, srcSchema, srcInstance, 
                             self.const.DevelopmentDatabaseCredentialsFile_SourceCreds, 
                             self.credsFileFullPath)
            raise ValueError, msg
        return retVal
    
    def getSourcePasswordHeuristic(self):
        '''
        For now just ignores the domain when searching for the password
        '''
        retVal = None
        srcSchema = self.parent.fmeMacroVals[self.const.FMWParams_SrcSchema]
        srcInstance = self.parent.fmeMacroVals[self.const.FMWParams_SrcInstance]

        
        srcInstance = Util.removeDomain(srcInstance)
        
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_SourceCreds]:
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]
            dbInst = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbInst]
            dbInstLst = dbInst.split('.')
            dbInst = dbInstLst[0]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]
            
            if dbInst.lower().strip() == srcInstance.lower() and \
               dbUser.lower().strip() == srcSchema.lower().strip():
                retVal =  dbPass
                break
        if not retVal:
            msg = 'Running in DevMod.  This means that the template is attempting ' +  \
                  'to retrieve the password from the json credential file {4} Was unable to ' + \
                  'retrieve the password for ' + \
                  'the srcSchema: {1} and the source instance {2} in the section {3}'
            msg = msg.format(self.credsFileFullPath, srcSchema, srcInstance, 
                             self.const.DevelopmentDatabaseCredentialsFile_SourceCreds, 
                             self.credsFileFullPath)
            raise ValueError, msg
        return retVal
        
class CalcParamsDataBC(object):
    
    def __init__(self, parent):
        self.parent = parent
        self.const = self.parent.const
        self.paramObj = self.parent.paramObj
        
        fmwDir = self.parent.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        fmwName = self.parent.fmeMacroVals[self.const.FMWMacroKey_FMWName]
        ModuleLogConfig(fmwDir, fmwName)
        
        #ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.fmeMacroVals = self.parent.fmeMacroVals
        self.currentPMPResource = None
        
    def getDestinationPassword(self, destKey=None):
        if not destKey:
            destKey = self.fmeMacroVals[self.const.FMWParams_DestKey]
        else: 
            self.paramObj.validateKey(destKey)
            destKey = self.paramObj.getDestinationDatabaseKey(destKey)
        pmpRes = self.paramObj.getDestinationPmpResource(destKey)
        computerName = Util.getComputerName()
        pmpDict = {'token': self.paramObj.getPmpToken(computerName),
                   'baseurl': self.paramObj.getPmpBaseUrl(), 
                   'restdir': self.paramObj.getPmpRestDir()}
        pmp = PMP.PMPRestConnect.PMP(pmpDict)
        accntName = self.fmeMacroVals[self.const.FMWParams_DestSchema]

        msg = 'retrieving the destination password for schame: ({0}) db env key: ({1})'
        msg = msg.format(accntName, destKey)
        self.logger.debug(msg)
        
        passwrd = pmp.getAccountPassword(accntName, pmpRes)
        return passwrd
        
    def getPmpDict(self):
        computerName = Util.getComputerName()
        pmpDict = {'token': self.paramObj.getPmpToken(computerName),
                   'baseurl': self.paramObj.getPmpBaseUrl(), 
                   'restdir': self.paramObj.getPmpRestDir()}
        return pmpDict
    
    def getSourcePassword(self):
        pmpDict = self.getPmpDict()
        pmp = PMP.PMPRestConnect.PMP(pmpDict)
        accntName = self.fmeMacroVals[self.const.FMWParams_SrcSchema]
        accntName = accntName
        instance = self.fmeMacroVals[self.const.FMWParams_SrcInstance]
        msg = 'retreiving source password from pmp for schema: ({0}), instance: ({1})'
        msg = msg.format(accntName, instance)
        srcResources = self.paramObj.getSourcePmpResources()
        pswd = None
        
        for pmpResource in srcResources:
            self.logger.debug("searching for password in the pmp resource {0}".format(pmpResource))
            self.currentPMPResource = pmpResource
            # start by trying to just retrieve the account using 
            # destSchema@instance as the "User Account" parameter
            try:
                accntATInstance = accntName.strip() + '@' + instance.strip()
                self.logger.debug("account@instance search string: {0}".format(accntATInstance))
                pswd = pmp.getAccountPassword(accntATInstance, pmpResource)
                
            except ValueError:
                # TODO: Add a proper FME log message here
                msg = 'There is no account {0} in pmp for the resource {1} using the token {2} from the machine {3}'
                msg = msg.format(accntName, pmpResource, pmp.token, platform.node())
                self.logger.warning(msg)
                # Going to do a search for accounts that might match the user
                self.getSourcePasswordHeuristic()
            if pswd:
                break
            # add some more warnings
        return pswd
    
    def getSourcePasswordHeuristic(self):
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
        srcInstanceInFMW = self.fmeMacroVals[self.const.FMWParams_SrcInstance].lower().strip()
        srcSchemaInFMW = self.fmeMacroVals[self.const.FMWParams_SrcSchema].lower().strip()
        # sometimes the source instance includes the domain so making sure this 
        # is stripped out, example idwprod1.env.gov.bc.ca becomes just idwprod1
        srcInstanceInFMWLst = srcInstanceInFMW.split('.')
        srcInstanceInFMW = srcInstanceInFMWLst[0]

        msg = "Using a heuristic to try to find the password for schema/instance: {0}/{1}"
        msg = msg.format(srcSchemaInFMW, srcInstanceInFMW)
        self.logger.debug(msg)
        pmpResource = self.currentPMPResource
        if not pmpResource:
            srcResources = self.paramObj.getSourcePmpResources()
        else:
            srcResources = [self.currentPMPResource]
        pswd = None
        pmpDict = self.getPmpDict()
        pmp = PMP.PMPRestConnect.PMP(pmpDict)
        
        for pmpResource in srcResources:
            # resource id for the pmp resource.
            resId = pmp.getResourceId(pmpResource)
            # list of accounts attached to the resource
            accounts = pmp.getAccountsForResourceID(resId)
            instList = []
            # source instance, and the source instance less the domain portion
            for accntDict in accounts:
                accntName = PMPSourceAccountParser(accntDict['ACCOUNT NAME'])
                schema = accntName.getSchema()
                if schema.lower().strip() == srcSchemaInFMW:
                    # found the srcSchema
                    # now check see if the instance matches
                    #print 'schemas {0} : {1}'.format(destSchema, self.fmeMacroVals[self.const.FMWParams_SrcSchema])
                    inst = accntName.getInstanceNoDomain()
                    if inst.lower().strip() == srcInstanceInFMW:
                        instList.append([accntDict['ACCOUNT NAME'], accntDict['ACCOUNT ID']])
        if instList:
            if len(instList) > 1:
                # get the passwords, if they are the same then return
                # the password
                msg = 'found more than one possible source password match for the instance{0}'
                msg = msg.format(srcInstanceInFMW)
                self.logger.warning(msg)
                msg = 'instances that match include {0}'.format(','.join(instList))
                self.logger.debug(msg)
                pswdList = []
                # eliminating any possible duplicates then 
                for accnts in instList:
                    pswdList.append(pmp.getAccountPasswordWithAccountId(accnts[1], resId))
                pswdList = list(set(pswdList))
                if len(pswdList) > 1:
                    msg = 'Looking for the password for the destSchema {0}, and instance {1}' +\
                          'Found the following accounts that roughly match that combination {2}' + \
                          'pmp has different passwords for each of these.  Unable to proceed as ' + \
                          'not sure which password to use.  Fix this by changing the parameter {3} ' + \
                          'to match a "User Account" in pmp exactly.'
                    msg = msg.format(self.fmeMacroVals[self.const.FMWParams_SrcSchema], 
                                     self.fmeMacroVals[self.const.FMWParams_SrcInstance], 
                                     ','.join(instList), 
                                     self.const.FMWParams_SrcInstance)
                    raise ValueError, msg
            else:
                # get the password and return it
                pswd = pmp.getAccountPasswordWithAccountId(instList[0][1], resId)
        return pswd
            
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
        ModuleLogConfig(fmwDir, fmwName)
        
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.info("inheriting the CalcParamsBase class")
        CalcParamsBase.__init__(self, fmeMacroVals)
        self.logger.debug("adding plugin functionality")
        self.addPlugin(forceDevelMode)
        
class ModuleLogConfig(object):
    def __init__(self, fmwDir, fmwName):
        pathList = Util.calcLogFilePath(fmwDir, fmwName, True)
        # get the tmpLog to test to see if the logger has been 
        # initialized yet or not
        tmpLog = logging.getLogger(__name__)
        if not tmpLog.handlers:
            #print 'no handlers found'
            logging.logFileName = pathList[1]
            #print 'logFileName', logging.logFileName  # @UndefinedVariable
            #print 'Loading log configs from log config file'
            const = TemplateConstants()
            #print 'created the constants'
            confFile = TemplateConfigFileReader('DEV')
            # Get the log config file name from the app config file
            #print 'confFile is', confFile
            logConfFileName = confFile.parser.get(const.ConfFileSection_global, const.AppConfigAppLogFileName)
            #print 'logConfFileName', logConfFileName
            
            # get the name of the conf dir
            configDir = const.AppConfigConfigDir
            dirname = os.path.dirname(__file__)
            #print 'dirname', dirname
            logConfFileFullPath = os.path.join(dirname,configDir,logConfFileName )
            #print 'logconfig full path:', logConfFileFullPath
            # logFileName
            #logging.logFileName = logFileName
            logging.config.fileConfig(logConfFileFullPath)
            #print 'got here'
            logger = logging.getLogger(__name__)
            #print 'and here'
            logger.debug("logger should be configured")
        else:
            #print 'logger already loaded'
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
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
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
        instance = self.config.getDestinationInstance()
        pmpResource = self.config.getDestinationPmpResource()
        passwrd = pmp.getAccountPassword(accntName, pmpResource)
        self.logger.debug("accntName: {0}".format(accntName))
        self.logger.debug("instance: {0}".format(instance))
        self.db = DB.DbLib.DbMethods()
        try:
            self.db.connectParams(accntName, passwrd, instance)
        except Exception, e:
            try:
                self.logger.warning(str(e))
                server = self.config.getDestinationServer()
                msg = 'unable to create a connection to the schema: {0}, instance {1} ' + \
                      'going to try to connect directly to the server: {2}'
                msg = msg.format(accntName, instance, server)
                self.logger.warning(msg)
                port = self.config.getDestinationOraclePort()
                self.logger.debug("port: {0}".format(port))
                self.logger.debug("server: {0}".format(server))
                self.db.connectNoDSN(accntName, passwrd, instance, server, port)
                self.logger.debug("successfully connected to database using direct connect")
                # TODO: Should really capture the specific error type here
            except Exception, e:
                self.logger.error(str(e))
                self.logger.info(msg)
                msg = 'database connection used to write to DWM has failed, ' + \
                      'dwm record for this replication will not be written'
                self.logger.error(msg)
                raise
        msg = 'successfully connected to the database {0} with the user {1}'
        msg = msg.format(instance, accntName)
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
        returnDict['dest_instance'] = self.getDestInstance()
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
        return dataSrcStr
            
    def getDuration(self):
        return self.fme.elapsedRunTime
        
    def getDestInstance(self):
        # retrieve from the published parameters:
        destInst = None
        if self.fme.macroValues.has_key(self.const.FMWParams_DestInstance):
            destInst = self.fme.macroValues[self.const.FMWParams_DestInstance]
        else:
            for macroKey in self.fme.macroValues:
                if macroKey.upper() == self.fme.macroValues[macroKey].upper():
                    destInst = self.fme.macroValues[macroKey]
                    break
        return destInst    
     
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
        