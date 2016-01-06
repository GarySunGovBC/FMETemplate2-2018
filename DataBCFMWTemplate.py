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
sys.path.insert(0, r'E:\sw_nt\FME2014\fmeobjects\python27')
sys.path.insert(0, r'\\data.bcgov\work\scripts\python\DataBCPyLib')
import os
# for fmeobjects to import properly the fme dir has 
# to be closer to the front of the pathlist
pathList = os.environ['PATH'].split(';')
pathList.insert(0, r'E:\sw_nt\FME2014')
os.environ['PATH'] = ';'.join(pathList)
import fmeobjects
import platform
import pprint
import ConfigParser
import json
import warnings
import PMP.PMPRestConnect

class TemplateConstants():
    # configfile name
    AppConfigFileName = 'templateDefaults.config'
    AppConfigConfigDir = 'config'
    AppConfigOutputsDir = 'outputs'
    
    # parameters relating to template sections
    ConfFileSection_global = 'global'
    ConfFileSection_global_key_rootDir = 'rootscriptdir'
    ConfFileSection_global_key_outDir =  'outputsbasedir'
    ConfFileSection_global_key_govComputers = 'gov_computers'
    ConfFileSection_global_configDirName = 'configdirname'
    ConfFileSection_global_devCredsFile = 'development_credentials_file'
    
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
    FMWParams_SrcFGDBPrefix = 'SRC_FGDB_'
    FMWParams_SrcFeaturePrefix = 'SRC_FEATURE_'
    FMWParams_SrcSchema = 'SRC_SCHEMA'
    FMWParams_SrcInstance = 'SRC_INSTANCE'
    FMWParams_SrcFeatPrefix = 'SRC_FEATURE_'
    # TODO: define the source database parameters
    
    # pmp config parameters
    ConfFileSection_pmpConfig = 'pmp_server_params'
    ConfFileSection_pmpConfig_baseurl = 'baseurl'
    ConfFileSection_pmpConfig_restdir = 'restdir'
    ConfFileSection_pmpSrc = 'pmp_source_info'
    ConfFileSection_pmpSrc_resources = 'sourceresource'
    
    # development mode json database params file
    DevelopmentDatabaseCredentialsFile = 'dbCreds.json'
    DevelopmentDatabaseCredentialsFile_CredsSection = 'creds'
    DevelopmentDatabaseCredentialsFile_CredsSection_dbUser = 'username'
    DevelopmentDatabaseCredentialsFile_CredsSection_dbInst = 'instance'
    DevelopmentDatabaseCredentialsFile_CredsSection_dbPswd = 'password'

class Start():
    
    def __init__(self, fme):
        self.fme = fme
        self.logger = fmeobjects.FMELogFile()  # @UndefinedVariable
        self.logger.logMessageString('starting the startup object, writing to the log')
        self.const = TemplateConstants()
        # TODO: add functionality that searches for overrides, ie a file
        #        with the same name as 
        
    def startup(self):
        # default startup routine
        #self.fme.macroValues[self.const.FMWParams_DestKey]
        # debugging / develeopment - printing the macrovalues.
        # useful for setting up test cases.
        for key in self.fme.macroValues.keys():
            print '{0}  {1}'.format(key, self.fme.macroValues[key])
        
class Shutdown():
    
    def __init__(self, fmeMacroVals):
        pass
    
    def shutdown(self):
        # what needs to be written can go here.
        pass
   
class TemplateConfigFileReader():
    
    parser = None
    key = None
    
    def __init__(self, key, confFile=None):
        self.confFile = confFile
        self.const = TemplateConstants()
        self.readConfigFile()
        self.setDestinationDatabaseEnvKey(key)
        
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
        if not dbEnvKey:
            dbEnvKey = self.key
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
    
class CalcParamsDevelopment():
    def __init__(self, fmeMacroVals):
        print 'constructing a CalcParamsDataBC'
        
    def getDestinationPassword(self):
        # need to get the full path to the json file
        # deal with appropriate error messages if the 
        # file cannot be found
        # if it is found read it and extract the 
        # database password
        #self.templateVals.getDatabaseCredentialsFilePath()
        confDirName = self.templateVals.getConfigDirName()
        credsFileName = self.templateVals.getDevelopmentModeCredentialsFileName()
        credsFileFullPath = os.path.join(self.fmeMacroVals['FME_MF_DIR'], confDirName, credsFileName)
        credsFileFullPath = os.path.realpath(credsFileFullPath)
        # schema = 
        schema = self.fmeMacroVals[self.const.FMWParams_DestSchema]
        inst = self.getDestinationInstance()
        with open(credsFileFullPath, 'r') as jsonFile:
            data = json.load(jsonFile)
        retVal = None
        for dbParams in data[self.const.DevelopmentDatabaseCredentialsFile_CredsSection]:
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_CredsSection_dbUser]
            dbInst = dbParams[self.const.DevelopmentDatabaseCredentialsFile_CredsSection_dbInst]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_CredsSection_dbInst]
            if dbInst.lower().strip() == inst.lower().strip() and \
               dbUser.lower().strip() == schema.lower().strip():
                retVal =  dbPass
                break
        if not retVal:
            msg = 'DevMod: Was unable to find a password in the credential file {0} for ' + \
                  'the schema: {1} and the instance {2}'
            msg = msg.format(credsFileFullPath, schema, inst)
            warnings.warn(msg)
        return retVal
    
class Util(object):
    @staticmethod
    def getComputerName():
        computerName = platform.node()
        if computerName.count('.'):
            computerNameParsedList = computerName.split('.')
            computerName = computerNameParsedList[0]
        return computerName
    
class CalcParamsDataBC():
    
    def __init__(self, fmeMacroVals):
        print 'constructing a CalcParamsDataBC'
        
    def getDestinationPassword(self, destKey=None):
        if not destKey:
            destKey = self.fmeMacroVals[self.const.FMWParams_DestKey]
        else: 
            self.paramObj.validateKey(destKey)

        pmpRes = self.paramObj.getDestinationPmpResource(destKey)
        computerName = Util.getComputerName()
        pmpDict = {'token': self.paramObj.getPmpToken(computerName),
                   'baseurl': self.paramObj.getPmpBaseUrl(), 
                   'restdir': self.paramObj.getPmpRestDir()}
        pmp = PMP.PMPRestConnect.PMP(pmpDict)
        accntName = self.fmeMacroVals[self.const.FMWParams_DestSchema]
        passwrd = pmp.getAccountPassword(accntName, pmpRes)
        return passwrd
    
    def getBCGWPassword(self, destKey=None):
        pswd = self.getDestinationPassword(destKey)
        return pswd
    
    def getSourcePassword(self):
        computerName = Util.getComputerName()
        pmpDict = {'token': self.paramObj.getPmpToken(computerName),
                   'baseurl': self.paramObj.getPmpBaseUrl(), 
                   'restdir': self.paramObj.getPmpRestDir()}
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
            try:
                pswd = pmp.getAccountPassword(accntName, pmpResource)
            except ValueError:
                # TODO: Add a proper FME log message here
                msg = 'There is no account {0} in pmp for the resource {1} using the token {2} from the machine {3}'
                msg = msg.format(accntName, pmpResource, pmp.token, platform.node())
                print msg
            if pswd:
                break
            # add some more warnings
        return pswd
         
class CalcParamsBase( CalcParamsDataBC ):
    '''
    This method contains the base functionality which is the 
    development mode base functionality
    
    Development mode assumes the following:
        - PMP is unavailable therefor passwords 
          will be retrieved from the json file 
          defined in t
    
    '''
    def __init__(self, fmeMacroVals):
        self.fmeMacroVals = fmeMacroVals
        self.const = TemplateConstants()
        self.templateVals = TemplateConfigFileReader(self.fmeMacroVals[self.const.FMWParams_DestKey])
        # if the computer that script is being run is defined 
        # as a "gov" machine, the app assumes that it can 
        # communicate with pmp.  As a result it will retrieve
        # database passwords from pmp.  If the script is 
        # run on a "nongov" machine it will attempt to retrieve
        # database credentials from a json file. (file path is 
        # returned by the TemplateConfigFileReader class
        # when necessary.   
        if self.templateVals.isDataBCNode():
            # inheriting from CalcParamsDataBC which will 
            # override various methods specific to password
            # recovery
            print 'inheriting the databc methods'
            CalcParamsDataBC.__init__(self, fmeMacroVals)
        else:
            CalcParamsDevelopment.__init__(self, fmeMacroVals)
        self.logger = fmeobjects.FMELogFile()  # @UndefinedVariable
        dbEnv = self.fmeMacroVals[self.const.FMWParams_DestKey]
        self.paramObj = TemplateConfigFileReader(dbEnv)
        
    def getFMWLogFileFullPath(self, create=True):
        curDir = os.path.dirname(__file__)
        outputsDirName = self.const.AppConfigOutputsDir
        fmwDir, fileExt = os.path.splitext(self.fmeMacroVals['FME_MF_NAME'])
        del fileExt
        logFileDirPath = os.path.join(curDir, 
                                   outputsDirName, 
                                   fmwDir)
        if create:
            if not os.path.exists(logFileDirPath):
                os.makedirs(logFileDirPath)
        logFilePath = os.path.join(logFileDirPath,
                                   fmwDir + '.log')
        return logFilePath
        
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
    
    def __init__(self, fmeMacroVals):
        CalcParamsBase.__init__(self, fmeMacroVals)
        print 'self', self
        print 'type(self)', type(self)
        #print getattr(self, '__bases__')
        

    