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

# for fmeobjects to import properly the fme dir has 
# to be closer to the front of the pathlist
pathList = os.environ['PATH'].split(';')
pathList.insert(0, r'E:\sw_nt\FME2014')
sys.path.insert(0, r'E:\sw_nt\FME2014\fmeobjects\python27')
sys.path.insert(0, r'\\data.bcgov\work\scripts\python\DataBCPyLib')
os.environ['PATH'] = ';'.join(pathList)

import site
import fmeobjects
import platform
import pprint
import ConfigParser
import json
import logging
import logging.config
import PMP.PMPRestConnect
import FMELogger
import inspect


class TemplateConstants():
    # no need for a logger in this class as its just
    # a list of properties
    #
    # Maps to sections in the config file
    AppConfigFileName = 'templateDefaults.config'
    AppConfigConfigDir = 'config'
    AppConfigOutputsDir = 'outputs'
    AppConfigLogFileExtension = '.log'
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
    
    ConfFileSection_destKeywords = 'dest_param_keywords'
    
    # properties from the config file.
    ConfFileSection_serverKey = 'server'
    ConfFileSection_pmpResKey = 'pmpresource'
    ConfFileSection_oraPortKey = 'oracleport'
    ConfFileSection_sdePortKey = 'sdeport'
    ConfFileSection_instanceKey = 'instance'
    
    ConfFileSection_pmpTokens = 'pmptokens'
    
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
    FMWParams_SrcSchema = 'SRC_SCHEMA'
    FMWParams_SrcInstance = 'SRC_INSTANCE'
    FMWParams_SrcFeatPrefix = 'SRC_FEATURE_'
    
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
    
class Start():
    
    def __init__(self, fme):
        ModuleLogConfig()
        # This method will always be called regardless of any customizations.
        
        # logging configuration
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.fme = fme
        print 'running the startup'
        self.const = TemplateConstants()
        # Reading the global paramater config file
        self.paramObj = TemplateConfigFileReader(self.fme.macroValues[self.const.FMWParams_DestKey])
        #self.initLogging()
        self.logger.warning("testing writing a warning message")
        # Extract the custom script directory from config file
        customScriptDir = self.paramObj.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_customScriptDir)
        # Assemble the name of a the custom script
        justScript, ext = os.path.splitext(self.fme.macroValues[self.const.FMWMacroKey_FMWName])
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
            print 'loading custom startup', startupScriptDirPath
            site.addsitedir(startupScriptDirPath)
            print 'added path', startupScriptDirPath
            print 'module ', justScript
            print 'sys.path', sys.path
            startupModule = importlib.import_module(justScript)
            print 'import of the module success'
            self.startupObj = startupModule.Start(self.fme)
            print 'object created'
        else:
            print 'using generic startup'
            self.startupObj = DefaultStart(self.fme)

    def initLogging(self):
        logName = os.path.splitext(os.path.basename(__file__))[0] + '.' + self.__class__.__name__
        self.logger = logging.getLogger( logName )         
        #rootLogger = logging.getLogger()
        #hndlr = logging.FileHandler(logFile) 
        hndlr = FMELogger.FMELogHandler()
        formatString = self.const.FMELogStartupFormatString
        formatr = logging.Formatter( formatString )
        formatr.datefmt = self.const.FMELogDateFormatString
        hndlr.setFormatter(formatr)        
        self.logger.addHandler(hndlr)          
        self.logger.setLevel(logging.DEBUG)        
        self.logger.debug('Logger should be setup!')
        
    def startup(self):
        # default startup routine
        #self.fme.macroValues[self.const.FMWParams_DestKey]
        # debugging / develeopment - printing the macrovalues.
        # useful for setting up test cases.
        # --------------------------------------------------------
        # will either call the default startup or a customized one
        self.logger.debug("calling the startup method..")
        self.startupObj.startup()
        
class DefaultStart():
    def __init__(self, fme):
        self.fme = fme
        
        
    def startup(self):
        # default startup routine
        #self.fme.macroValues[self.const.FMWParams_DestKey]
        # debugging / develeopment - printing the macrovalues.
        # useful for setting up test cases.
        #for key in self.fme.macroValues.keys():
        #    print '{0}  {1}'.format(key, self.fme.macroValues[key])
        
        # currently there is no startup code.
        pass
        
class Shutdown():
    
    def __init__(self, fme):
        # shutdown logging works differently
        self.fme = fme
        self.const = TemplateConstants()
        self.__initLogging()
        self.logger.debug("Shutdown has been called...")
        self.logger.debug("log file name: {0}".format(self.fme.logFileName))
        print 'shtudown log file', self.fme.logFileName
    
    def __initLogging(self):
        # full path will be self.fme.
        path2FmwDir = self.fme.macroValues[self.const.FMWMacroKey_FMWDirectory]
        logFileFullPath = os.path.join(path2FmwDir, self.fme.logFileName)
        
        logName = os.path.splitext(os.path.basename(__file__))[0] + '.' + self.__class__.__name__
        self.logger = logging.getLogger( logName )
        #rootLogger = logging.getLogger()
        #hndlr = logging.FileHandler(logFile) 
        hndlr = FMELogger.FMEShutdownLogger(logFileFullPath)        
        #formatString = '%(asctime)s %(name)s.%(funcName)s.%(lineno)d %(levelname)s: %(message)s'
        formatr = logging.Formatter( self.const.FMELogShutdownFormatString )
        formatr.datefmt = self.const.FMELogDateFormatString
        hndlr.setFormatter(formatr)
        self.logger.addHandler(hndlr)          
        self.logger.setLevel(logging.DEBUG)        
        self.logger.debug('Logger should be setup!')
    
    def shutdown(self):
        # what needs to be written can go here.
        self.logger.debug("shutdown has been called")
        pass
   
class TemplateConfigFileReader():
    
    parser = None
    key = None
    
    def __init__(self, key, confFile=None):
        ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.confFile = confFile
        print 'self.confFile', self.confFile
        self.const = TemplateConstants()
        self.readConfigFile()
        self.setDestinationDatabaseEnvKey(key)
        print 'key', key, self.key
        
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
            print 'self.confFile', self.confFile
        if not self.parser:
            self.parser = ConfigParser.ConfigParser()
            self.parser.read(self.confFile)
            print 'sections', self.parser.sections()
            
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
        print 'dbEnvKey', dbEnvKey
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
            msg = 'The config file does not have a pmp token for the ' + \
                  'computer name {0}'
            msg = msg.format(computerName)
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
        print 'curMachine', curMachine
        print 'nodeList', nodeList
        if curMachine in nodeList:
            retVal = True
        print 'retVal', retVal
        return retVal
    
class PMPSourceAccountParser():
    
    def __init__(self, accntName):
        ModuleLogConfig()
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
        ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        print 'start with instatiation of CalcParamsBase'
        self.fmeMacroVals = fmeMacroVals
        self.const = TemplateConstants()
        self.paramObj = TemplateConfigFileReader(self.fmeMacroVals[self.const.FMWParams_DestKey])
        
        # if the computer that script is being run is defined 
        # as a "gov" machine, the app assumes that it can 
        # communicate with pmp.  As a result it will retrieve
        # database passwords from pmp.  If the script is 
        # run on a "nongov" machine it will attempt to retrieve
        # database credentials from a json file. (file path is 
        # returned by the TemplateConfigFileReader class
        # when necessary.   
        
        self.logger = fmeobjects.FMELogFile()  # @UndefinedVariable
        print 'done with instatiation of CalcParamsBase'        
        
    def __initlogger(self):
        logName = os.path.splitext(os.path.basename(__file__))[0] + '.' + self.__class__.__name__
        self.logger = logging.getLogger( logName )         
        
        #rootLogger = logging.getLogger()
        
        #hndlr = logging.FileHandler(logFile) 
        hndlr = FMELogger.FMELogHandler()
        
        # Step 3 - create a formatter along with a format string
        formatString = '%(asctime)s %(name)s.%(funcName)s.%(lineno)d %(levelname)s: %(message)s'
        formatr = logging.Formatter( formatString )
        formatr.datefmt = '%m-%d-%Y %H:%M:%S' # set up the date format for log messages
        hndlr.setFormatter(formatr)        
        self.logger.addHandler(hndlr)          
        self.logger.setLevel(logging.DEBUG)        
        self.logger.debug('Logger should be setup!')
        
    def addPlugin(self, forceDevel=False):
        if forceDevel:
            #CalcParamsDevelopment.__init__(self, fmeMacroVals)
            self.plugin = CalcParamsDevelopment(self)
        elif self.paramObj.isDataBCNode():
            # inheriting from CalcParamsDataBC which will 
            # override various methods specific to password
            # recovery
            print 'inheriting the databc methods'
            #CalcParamsDataBC.__init__(self, fmeMacroVals)
            self.plugin = CalcParamsDataBC(self)
        else:
            #CalcParamsDevelopment.__init__(self, fmeMacroVals)
            self.plugin = CalcParamsDevelopment(self)
        
    def getFMWLogFileRelativePath(self, create=True):
        curDir = self.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        outDirFullPath = os.path.join(curDir, self.const.AppConfigOutputsDir)
        if not os.path.exists(outDirFullPath) and create:
            os.mkdir(outDirFullPath)
        logDirFullPath = os.path.join(outDirFullPath, self.const.AppConfigLogDir)
        if not os.path.exists(logDirFullPath) and create:
            os.mkdir(logDirFullPath)
        fmwFileNoExt, fileExt = os.path.splitext(self.fmeMacroVals[self.const.FMWMacroKey_FMWName])
        fmwLogFile = fmwFileNoExt + self.const.AppConfigLogFileExtension
        relativePath = os.path.join('.', self.const.AppConfigOutputsDir, self.const.AppConfigLogDir, fmwLogFile)
        return relativePath
        
    def getDestinationServer(self):
        self.logger.logMessageString('Setting the destination server')
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
        print 'srcinst1', self.fmeMacroVals['SRC_INSTANCE']
        return pswd
        
    def getDestinationPassword(self):
        pswd = self.plugin.getDestinationPassword()
        return pswd
    
    def getSourcePasswordHeuristic(self):
        pswd = self.plugin.getSourcePasswordHeuristic()
        return pswd

class CalcParamsDevelopment(object):
    
    def __init__(self, parent):
        ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        #self.paramObj = paramObj
        #self.fmeMacroVals = fmeMacroVals
        #self.const = TemplateConstants()
        self.parent = parent
        self.const = self.parent.const
        self.paramObj = self.parent.paramObj
        print 'constructing a CalcParamsDevelopment'
        confDirName = self.paramObj.getConfigDirName()
        credsFileName = self.paramObj.getDevelopmentModeCredentialsFileName()        
        # expects the creds file to be ./$(confDirName)/$(credsFileName)
        credsFileFullPath = os.path.join(self.parent.fmeMacroVals[self.const.FMWMacroKey_FMWDirectory], confDirName, credsFileName)
        self.credsFileFullPath = os.path.realpath(credsFileFullPath)
        print 'credsFileFullPath', credsFileFullPath
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
            raise ValueError, msg
        #self.destSchema = self.parent.fmeMacroVals[self.const.FMWParams_DestSchema]
        #self.destInstance = self.parent.getDestinationInstance()
        #self.srcSchema = self.parent.fmeMacroVals[self.const.FMWParams_SrcSchema]
        #self.srcInstance = self.parent.fmeMacroVals[self.const.FMWParams_SrcInstance]
        with open(credsFileFullPath, 'r') as jsonFile:
            self.data = json.load(jsonFile)
        
    def getDestinationPassword(self):
        retVal = None
        destSchema = self.parent.fmeMacroVals[self.const.FMWParams_DestSchema]
        destInstance = self.parent.getDestinationInstance()

        print 'destSchema', destSchema.lower().strip()
        print 'destInstance', destInstance.lower().strip()
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_DestCreds]:
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]
            dbInst = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbInst]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]
            print 'dbUser', dbUser.lower().strip()
            print 'dbInst', dbInst.lower().strip()
            if dbInst.lower().strip() == destInstance.lower().strip() and \
               dbUser.lower().strip() == destSchema.lower().strip():
                print 'found it'
                retVal =  dbPass
                break
        if not retVal:
            msg = 'DevMod: Was unable to find a password in the credential file {0} for ' + \
                  'the destSchema: {1} and the instance {2}'
            msg = msg.format(self.credsFileFullPath, destSchema, destInstance)
            raise ValueError, msg
        return retVal
    
    def getSourcePassword(self):
        print 'srcinst2', self.parent.fmeMacroVals['SRC_INSTANCE']
        srcSchema = self.parent.fmeMacroVals[self.const.FMWParams_SrcSchema]
        srcInstance = self.parent.fmeMacroVals[self.const.FMWParams_SrcInstance]
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
        ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.parent = parent
        self.const = self.parent.const
        self.paramObj = self.parent.paramObj
        self.fmeMacroVals = self.parent.fmeMacroVals
        self.currentPMPResource = None
        #self.paramObj = paramObj
        #self.fmeMacroVals = fmeMacroVals
        print 'constructing a CalcParamsDataBC'
        
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
        print 'accntName', accntName
        print 'instance', instance
        srcResources = self.paramObj.getSourcePmpResources()
        pswd = None
        
        for pmpResource in srcResources:
            print 'pmpResource', pmpResource
            self.currentPMPResource = pmpResource
            # start by trying to just retrieve the account using 
            # destSchema@instance as the "User Account" parameter
            try:
                accntATInstance = accntName.strip() + '@' + instance.strip()
                print 'trying to retrieve ', accntATInstance
                pswd = pmp.getAccountPassword(accntATInstance, pmpResource)
                
            except ValueError:
                # TODO: Add a proper FME log message here
                msg = 'There is no account {0} in pmp for the resource {1} using the token {2} from the machine {3}'
                msg = msg.format(accntName, pmpResource, pmp.token, platform.node())
                # Going to do a search for accounts that might match the user
                self.getSourcePasswordHeuristic()
                print msg
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
            srcInst = self.fmeMacroVals[self.const.FMWParams_SrcInstance]
            srcInstList = srcInst.split('.')
            srcInstNoDomain = srcInstList[0]
            for accntDict in accounts:
                accntName = PMPSourceAccountParser(accntDict['ACCOUNT NAME'])
                schema = accntName.getSchema()
                if schema.lower() == self.fmeMacroVals[self.const.FMWParams_SrcSchema].lower():
                    # found the srcSchema
                    # now check see if the instance matches
                    #print 'schemas {0} : {1}'.format(destSchema, self.fmeMacroVals[self.const.FMWParams_SrcSchema])
                    inst = accntName.getInstanceNoDomain()
                    #print 'instance', destInstance, srcInstNoDomain
                    if inst.lower() == srcInstNoDomain.lower():
                        instList.append([accntDict['ACCOUNT NAME'], accntDict['ACCOUNT ID']])
                #print 'accntName', destSchema
        if instList:
            if len(instList) > 1:
                # get the passwords, if they are the same then return
                # the password
                print 'found more than one match', instList
                pswdList = []
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
                print 'getting the password'
                pswd = pmp.getAccountPasswordWithAccountId(instList[0][1], resId)
        return pswd
            
class CalcParams(CalcParamsBase):
    '''
    This class is used to populate the scripted parameters
    defined in the fmw.  The following table describes the 
    relationship between these methods and the properties
    that they populate
        getDestinationServer       - DEST_SERVER
        getDestinationInstance     - DEST_INSTANCE
        getDestinationPort         - DEST_PORT
        getDestinationPassword     - DEST_PASSWORD
    
    import DataBCFMWTemplate
    params = DataBCFMWTemplate.calcParams(FME_MacroValues)
    return params.getDestinationPassword()
    '''
    
    def __init__(self, fmeMacroVals, forceDevelMode=False):
        ModuleLogConfig()
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        
        CalcParamsBase.__init__(self, fmeMacroVals)
        self.addPlugin(forceDevelMode)
        print 'self', self
        print 'type(self)', type(self)
        #print getattr(self, '__bases__')
        
class ModuleLogConfig():
    def __init__(self):
        tmpLog = logging.getLogger(__name__)
        
        if not tmpLog.handlers:
            print 'Loading log configs from log config file'
            const = TemplateConstants()
            confFile = TemplateConfigFileReader('DEV')
            # Get the log config file name from the app config file
            logConfFileName = confFile.parser.get(const.ConfFileSection_global, const.AppConfigAppLogFileName)
            # get the name of the conf dir
            configDir = const.AppConfigConfigDir
            dirname = os.path.dirname(__file__)
            logConfFileFullPath = os.path.join(dirname,configDir,logConfFileName )
            print 'logconfig full path:', logConfFileFullPath
            logging.config.fileConfig(logConfFileFullPath)
            logger = logging.getLogger(__name__)
            logger.debug("logger should be configured")
            
