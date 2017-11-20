'''
Created on Jan 30, 2017

@author: kjnether

This module contains a bunch of classes that can be attached 
to python caller transformers.  Each class is designed to test
the various aspects of the fme template.  These include:
   - The Startup routine
   - All the possible published parameters
   - The Shutdown routine
   
Template aspects not currently addressed:
   - Custom transformers
   
Although almost all of these tests could be implemented on their own, its important
that they are tested from an FMW as this ensures:
  - The environments is working (FME is finding the correct versions of the 
    python libraries in use.)
  - 

'''
import DataBCFMWTemplate
import logging
import unittest
import os.path
import json
import platform
import pprint
import sys

class ParameterTester(object):
    '''
    This class exists to allow for testing of the various
    Automated parameters
    
    all parameters get their values from the CalcParams
    class.
    
    example:
    
    import DataBCFMWTemplate
    params = DataBCFMWTemplate.CalcParams(FME_MacroValues)
    srcPass = params.getSourcePassword()
    return srcPass
    
    This class is designed to test the retrieval for all 
    source parameters

    '''
    
    def __init__(self, fme):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.fmeMacros = fme.macroValues
        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(self.fmeMacros)
        
        # This params will use the values in the fmw
        self.params = DataBCFMWTemplate.CalcParams(self.fmeMacros)
        self.origMacros = self.fmeMacros.copy()

        self.msg = "{0} : {1}"
        
        # these values will be dumped to the dbcreds file to
        self.dummySrcPassword = 'dummySourcePassword'
        self.dummyDstPassword = 'dummyDestPassword'
        
    def resetFMEMacroValues(self):
        self.fmeMacros = self.origMacros.copy()
        self.params.fmeMacroVals = self.fmeMacros
        self.params.plugin.fmeMacroVals = self.fmeMacros
    
    def testAllParameters(self):
        '''
        parameter fetching works differently for some methods
        if run in development mode.  This method will run tests
        for both environments by toggleing the destination database
        environment key to 'Development' mode.
        
        '''
        self.logger.debug("running prod mode tests now...")
        self.testAllParameters_ProdMode()
        self.logger.debug("running devmod tests now...")
        self.testAllParameters_DevMode()
    
    def testAllParameters_ProdMode(self):
        self.test_getSrcSDEDirectConnectString()
        self.test_getDestSDEConnFile()
        self.test_getSrcSDEConnFile
        self.test_isSourceBCGW()
        self.test_isSourceBCGWLinkedParameters()
        self.test_MD5Calc()
        self.test_getDestinationPassword()
        self.test_getDatabaseConnectionFilePath()
        self.test_getEasyConnectString()
        self.test_getSourcePassword()
        self.test_getSourcePasswordHeuristic()
        self.test_getDestinationOraclePort()
        self.test_getDestinationSDEPort()
        self.test_getDestinationHost()
        self.test_getDestinationServiceName()
        self.test_getFMWLogFileRelativePath()
        
    def test_getDestSDEConnFile(self):
        self.resetFMEMacroValues()
        sdeConnFilePath = self.params.getDestDatabaseConnectionFilePath()
        self.logger.info("sde conn file path: {0}".format(sdeConnFilePath))
        
    def test_getSrcSDEConnFile(self):
        self.resetFMEMacroValues()
        sdeConnFilePath = self.params.getSrcDatabaseConnectionFilePath()
        self.logger.info("source sde conn file is: {0} ".format(sdeConnFilePath))
        
    def test_getSrcSDEDirectConnectString(self):
        self.resetFMEMacroValues()
        
        # now set some macro values that can be used to test 
        # this 
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = 'idwdlvr1.bcgov'
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcHost] = 'bcgw-i.bcgov'
        dcStr = self.params.getSrcSDEDirectConnectString()
        self.logger.info("direct connect string: {0}".format(dcStr))
        
    def testAllParameters_DevMode(self):
        # only do this if not on arneb
        self.logger.debug("running dev mode tests")
        if not self.params.paramObj.isFMEServerNode():
            self.logger.debug("running DevMode tests")
            # now toggle the db_env_key and start the testing
            fmeMacroValues = self.origMacros.copy()
            
            fmeMacroValues[self.params.const.FMWParams_DestKey] = 'dev'
            fmeMacroValues[self.params.const.FMWParams_SrcServiceName] = 'serviceNameValue'
            self.params = DataBCFMWTemplate.CalcParams(fmeMacroValues, True)
            self.__setupDbCredsFile()
    
            self.logger.debug("plugin Name {0}".format( self.params.plugin.__class__.__name__ ))
            # double checking that we are in fact in development mode
            if self.params.plugin.__class__.__name__ <> 'CalcParamsDevelopment':
                msg = 'unable to get template to run in development mode for testing' + \
                      'plugin that is currently being used is: {0}'
                msg = msg.format(self.params.plugin.__class__.__name__ )
                raise ValueError, msg
            self.logger.debug(self.params.plugin.__class__.__name__)
            
            self.test_getDestConnFile_Dev()
            self.test_getDbCredsFile()
            # point to the dbcreds file to be uese for testing
            # by setting this parameter self.const.FMWMacroKey_FMWDirectory
            # to the directory for the creds file we want to use
            

            
            self.test_getPasswordsDevMode()
            self.test_getFMWLogFileRelativePath()
            self.test_getDestinationServiceName()
            
    def test_getDestConnFile_Dev(self):
        # setup create the file that is expected
        
        fmwDir = self.params.fmeMacroVals[self.params.const.FMWMacroKey_FMWDirectory]
        expectedSdeFile = 'bcgw-i.bcgov__idwdlvr1.bcgov.sde'
        expectedFullPath = os.path.join(fmwDir, expectedSdeFile)

        if os.path.exists(expectedFullPath ):
            os.remove(expectedFullPath)
        
        # this will generate an error but it will tell me the name of the file 
        # i need to create in order to prevent it from crashing
        try:
            connFile = self.params.getDestDatabaseConnectionFilePath()
            # this should raise an error therefor this
            # line should not get executed
            raise ValueError, 'The connection file does not exist however the method did not raise an error.'
        except:
            self.logger.debug("correctly raised an error because the connection " + \
                              'file {0} does not exist'.format(expectedFullPath))
                
        # creating the file and trying again
        fh = open(expectedFullPath,'w')
        fh.close()
        
        connFile = self.params.getDestDatabaseConnectionFilePath()
        self.logger.info('returned path: {0}'.format(connFile))
        
        if os.path.exists(expectedFullPath ):
            os.remove(expectedFullPath)
        
    def test_MD5Calc(self):
        '''
        The parcel map replication requires the hashlib module
        to be able to calculate an MD5 for change control.  This
        test simply calls that funcationality on this file
        to make sure that it exists.
        '''
        self.logger.info("test_MD5Calc")
        testfile = __file__
        import hashlib
        md5 = hashlib.md5(open(testfile, 'rb').read()).hexdigest()  # @UndefinedVariable
        self.logger.debug("hashlib md5 works")
        
    def test_getDbCredsFile(self):
        self.logger.info("determing the db credentials file")
        credsFilePath = self.params.plugin.getDbCredsFile()
        self.logger.debug("the dbcreds file is {0}".format(credsFilePath))
        if not os.path.exists(credsFilePath):
            msg = 'The credentials file {0} does not exist'
            msg = msg.format(credsFilePath)
            raise ValueError, msg
        
    def __setupDbCredsFile(self):
        '''
        creates a dbcreds file that can be used for testing the development mode 
        of the framework.
        '''
        const = DataBCFMWTemplate.TemplateConstants()
        
        # first determine the path to the dbCreds file
        self.logger.debug("Creating the dbCreds file")
        credsFileName = self.params.paramObj.getDevelopmentModeCredentialsFileName()
        fmwPath = self.params.fmeMacroVals[self.params.const.FMWMacroKey_FMWDirectory]
        dbCredsFullPath = os.path.join(fmwPath, credsFileName)
        self.logger.debug("path to dbcreds is {0}".format(dbCredsFullPath))
        if os.path.exists(dbCredsFullPath):
            os.remove(dbCredsFullPath)
            
        # define the basic struct, will then add to it later
        struct = {const.DevelopmentDatabaseCredentialsFile_DestCreds: [], 
                  const.DevelopmentDatabaseCredentialsFile_SourceCreds: []}
        self.logger.debug("created the credentials structure, now populating it")
        
        # get the source schema / instance / password
        #srcPass = self.params.getSourcePassword()
        srcPass = self.dummySrcPassword
        #srcInst = self.params
        #schemaMacroKey, instanceMacroKey = self.params.getSchemaForPasswordRetrieval()
        #schemaMacroKey, serviceNameMacroKey = self.params.getSchemaAndServiceNameForPasswordRetrieval()
        
#         macroKeyProxy = const.FMWParams_SrcProxySchema
#         macroKeyProxy = self.getMacroKeyForPosition(macroKeyProxy)
#         macroKeyData = const.FMWParams_SrcSchema
#         macroKeyData = self.getMacroKeyForPosition(macroKeyData)
         
        srcOraSchema = self.params.getSourceOracleSchema()

        srcServiceName = self.params.getSourceOracleServiceName()
        srcServiceName = DataBCFMWTemplate.Util.removeDomain(srcServiceName)
        
        serviceNameMacroKey = const.FMWParams_SrcServiceName
        schemaMacroKey = const.FMWParams_SrcProxySchema
        
        if serviceNameMacroKey not in self.params.fmeMacroVals:
            msg = 'The service name {0} does not exist in the fmw published parameters'
            raise ValueError, msg.format(serviceNameMacroKey)
        sn = self.params.fmeMacroVals[serviceNameMacroKey]
        self.logger.debug("sn: {0}".format(sn))
        schema = self.params.fmeMacroVals[schemaMacroKey]
        self.logger.debug("dbUser param name: {0}".format(const.DevelopmentDatabaseCredentialsFile_dbUser))
        srcParms = {}
        srcParms[const.DevelopmentDatabaseCredentialsFile_dbServName] = sn
        srcParms[const.DevelopmentDatabaseCredentialsFile_dbUser] = schema
        srcParms[const.DevelopmentDatabaseCredentialsFile_dbPswd] = srcPass
        struct[const.DevelopmentDatabaseCredentialsFile_SourceCreds].append(srcParms)
        self.logger.debug("source credentials have been setup onto destination...")
        
        # get the destination schema / instance / password
        schema = self.params.fmeMacroVals[self.params.const.FMWParams_DestSchema]
        #instance = self.params.getDestinationInstance()
        sn = self.params.getDestinationServiceName()
        
        #destPass = self.params.getDestinationPassword()
        destPass= self.dummyDstPassword
        destParams = {const.DevelopmentDatabaseCredentialsFile_dbServName : sn, 
                      const.DevelopmentDatabaseCredentialsFile_dbUser : schema, 
                      const.DevelopmentDatabaseCredentialsFile_dbPswd : destPass
                      }
        self.logger.debug("destParams are: {0}".format(destParams))
        struct[const.DevelopmentDatabaseCredentialsFile_DestCreds].append(destParams)
        with open(dbCredsFullPath, 'w') as fh:
            json.dump(struct, fh)
        self.logger.debug("completed setting up the dev mode creds file")
            
    def test_getDatabaseConnectionFilePath(self):
        self.logger.info("test_getDatabaseConnectionFilePath")
        connFilePath = self.params.getDestDatabaseConnectionFilePath()
        self.logger.debug(self.msg.format('connection file path', connFilePath))
    
    def test_isSourceBCGW(self):
        self.resetFMEMacroValues()
        self.logger.info("test_isSourceBCGW")
        isSrc = self.params.isSourceBCGW()
        self.logger.debug("return value from isSourceBCGW: ".format(isSrc))
        if isSrc:
            msg = 'The FMW incorrectly detected that that source is a BCGW source'
            raise ValueError, msg
        
    def test_isSourceBCGWLinkedParameters(self):
        '''
        Sometimes the source database is the same as the destination database, 
        
        when this happens you can link SRC_HOST to DEST_HOST and SRC_SERVICENAME to DEST_ORA_SERVICENAME
        
        When parameters are linked the values of one value can be equal to a parameter name.
        
        To address this problem the method Util.getParamValue() was created.  This 
        method simulates that situation for testing purposes
        '''
        self.resetFMEMacroValues()
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = '$({0})'.format(self.params.const.FMWParams_DestServiceName)
        self.params.fmeMacroVals[self.params.const.FMWParams_DestServiceName] = 'idwdlvr1.bcgov'
        SrcServiceName = DataBCFMWTemplate.Util.getParamValue(self.params.const.FMWParams_SrcServiceName, self.params.fmeMacroVals)
        if SrcServiceName <> self.params.fmeMacroVals[self.params.const.FMWParams_DestServiceName]:
            msg = 'Not properly retriving the linked parameter values'
            raise ValueError, msg
        isSrc = self.params.isSourceBCGW()
        if not isSrc:
            msg = 'incorrectly detecting source as not a bcgw source'
            raise ValueError, msg
        
    def test_getSourcePassword(self):
        self.logger.info("test_getSourcePassword")
        # retrieving the source password uses 
        # the parameters
        #   - SRC_ORA_SCHEMA or if it exists SRC_ORA_PROXY_SCHEMA
        #   - instance SRC_ORA_INSTANCE
        msg = 'unable to get the source password for schema {0} and ' + \
              'instance {1}'
        # make a copy of the original macros
        self.resetFMEMacroValues()
        
        self.fmeMacros = self.origMacros.copy()
        # remove the SRC_ORA_SERVICENAME parameter for 
        # this test
        snKey = self.params.const.FMWParams_SrcServiceName
        if snKey in self.fmeMacros:
            del self.fmeMacros[snKey]
        
        self.params.fmeMacroVals = self.fmeMacros
        self.params.plugin.fmeMacroVals = self.fmeMacros

        
        #self.params.fmeMacroVals = self.origMacros.copy()
        
        # by default there is no SRC_ORA_SERVICENAME so should raise an error
        try:
            self.logger.info("first password test")
            srcPas = self.params.getSourcePassword()
            self.logger.info("done first password test")
            msg = 'should have raised a ValueError as there is no SRC_ORA_SERVICENAME defined ' + \
                  'in the FMW'
            raise NameError, msg
        except KeyError:
            self.logger.debug("caught the error that was expected, moving on to next test")
            pass
        except:
            self.logger.error("errored out here")
            raise
        # this should retrieve a password
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = 'ISR.BCOGC.CA'
        self.logger.debug("plugin service name parameter value {0}".format(self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName]))
        self.params.plugin.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = 'ISR.BCOGC.CA'
        self.logger.debug("set the param {0} to {1}".format(self.params.const.FMWParams_SrcServiceName, \
                                                            self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName]))
        srcPas = self.params.getSourcePassword()
        self.logger.debug("srcPassword is: {0}".format('*'*len(srcPas)))
        if not srcPas:
            raise ValueError, msg.format(self.params.fmeMacroVals[self.params.const.FMWParams_SrcProxySchema], \
                                         self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName])
         
        # This should generate an error, which gets caught by the except block
        # testing that raises an error when a password does not exist
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcProxySchema] = 'ilrr'
        # changing the instance
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = 'ilrprd.env.gov.bc.ca'
        try:
            # this should raise an error as their is no password for that user
            srcPas = self.params.getSourcePassword()
            #self.logger.debug("second pwd is: {0}".format(srcPas))
            if not srcPas:
                raise NameError, msg.format(self.params.fmeMacroVals[self.params.const.FMWParams_SrcProxySchema], \
                                             self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName])
        except ValueError:
            self.logger.info("successfully raised error as no account exists for the params fed")
        
        # switch the username and service name to one that exists.
        # should get a password fine
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcProxySchema] = 'proxy_waterdw'
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = 'ewrwtst1.env.gov.bc.ca'
        srcPas = self.params.getSourcePassword()
        if not srcPas:
            raise ValueError, msg.format(self.params.fmeMacroVals[self.params.const.FMWParams_SrcProxySchema], \
                                         self.params.fmeMacroVals[self.params.const.FMWParams_SrcInstance])
        
        # now define a second username and password
        FMWParams_SrcProxySchema = self.params.const.FMWParams_SrcProxySchema + '_2'
        FMWParams_SrcServiceName = self.params.const.FMWParams_SrcServiceName + '_2'
        self.logger.debug("service Name macro: {0}".format(FMWParams_SrcServiceName))
        self.logger.debug("schema name macro: {0}".format(FMWParams_SrcProxySchema))
        self.params.fmeMacroVals[FMWParams_SrcProxySchema] = 'csat'
        self.params.fmeMacroVals[FMWParams_SrcServiceName] = 'csattst.env.gov.bc.ca'
        
        srcPas = self.params.getSourcePassword(2)
        # lastly set the original macros back
        #self.params.fmeMacroVals = self.origMacros.copy()
        
    def test_getSourcePasswordHeuristic(self):
        self.logger.debug("test_getSourcePasswordHeuristic")
        #self.params.fmeMacroVals = self.origMacros.copy()
        self.resetFMEMacroValues()
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = 'ISR.gov'

        # testing with the existing entry in the 
        srcPas = self.params.getSourcePasswordHeuristic()
        if not srcPas:
            msg = 'unable to get the source password for schema {0} and ' + \
                  'instance {1}'
            raise ValueError, msg.format(self.params.fmeMacroVals[self.params.const.FMWParams_SrcProxySchema], \
                                         self.params.fmeMacroVals[self.params.const.FMWParams_SrcInstance])
        
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcProxySchema] = 'ilrr'
        # changing the instance
        self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = 'ilrprd.BCGOV'
        srcPas = self.params.getSourcePasswordHeuristic()
        if not srcPas:
            msg = 'unable to get the source password for schema {0} and ' + \
                  'instance {1}'
            raise ValueError, msg.format(self.params.fmeMacroVals[self.params.const.FMWParams_SrcProxySchema], \
                                         self.params.fmeMacroVals[self.params.const.FMWParams_SrcInstance])
        #self.params.fmeMacroVals = self.origMacros.copy()
        
    def test_getPasswordsDevMode(self):
        self.logger.debug("test_getPasswordsDevMode")
        msg = 'Something is wrong with the dbcreds routine. A ' + \
                  'custom version was created with the {2} password '  +\
                  '{0} in it, however the returned password was {1}'
        pswd = self.params.getDestinationPassword()
        if pswd <> self.dummyDstPassword:
            
            msgFormatted = msg.format(self.dummyDstPassword, pswd, 'destination')
            raise ValueError, msgFormatted
        pswd = self.params.getSourcePassword()
        if pswd <> self.dummySrcPassword:
            msgFormatted = msg.format(self.dummyDstPassword, pswd, 'source')
            raise ValueError, msgFormatted
        
    def test_getDestinationPassword(self):
        self.logger.debug("test_getDestinationPassword")
        #self.params.fmeMacroVals = self.origMacros.copy()
        #self.origMacros = self.params.fmeMacroVals.copy()
        self.resetFMEMacroValues()
        pswd = self.params.getDestinationPassword()
        destSchema = self.params.fmeMacroVals[self.params.const.FMWParams_DestSchema]
        destServName = self.params.getDestinationServiceName()
        if not pswd:
            msg = 'unable to get the password for the schema {0} and ' + \
                  'the instance {1}'
            raise ValueError, msg.format(destSchema, destServName)
        self.logger.debug("set destination schema to null. should raise error")
        self.params.fmeMacroVals[self.params.const.FMWParams_DestSchema] = None
        self.params.plugin.fmeMacroVals[self.params.const.FMWParams_DestSchema] = None
        try:
            pswd = self.params.getDestinationPassword()
            success = True
        except:
            success = False
        if success:
            msg = 'somehow the script just returned a password when ' + \
                  'the destination schema was set to NONE.  This should ' + \
                  'not happen'
            raise ValueError, msg

        self.logger.debug("second password is {0}".format(pswd))
        
        destSchema = 'APP_UTILITY'
        destServName = self.params.getDestinationServiceName()
        self.params.fmeMacroVals = self.origMacros.copy()
        self.params.fmeMacroVals[self.params.const.FMWParams_DestSchema] = destSchema
        self.params.plugin.fmeMacroVals[self.params.const.FMWParams_DestSchema] = destSchema

        if not pswd:
            msg = 'unable to get the password for the schema {0} and ' + \
                  'the instance {1}, this is likely either a pmp access issue or ' + \
                  'the correct resources havent been added to the config'
            raise ValueError, msg.format(destSchema, destServName)        
        
    def test_getDestinationOraclePort(self):
        self.logger.debug("test_getDestinationOraclePort")
        oraPort = self.params.getDestinationOraclePort()
        msg = 'destination oracle port {0} '
        self.logger.debug(msg.format(oraPort))
        
    def test_getDestinationSDEPort(self):
        self.logger.debug("test_getDestinationSDEPort")
        sdePort = self.params.getDestinationSDEPort()
        msg = 'sde port for destination {0}'
        self.logger.debug(msg.format(sdePort))
        
    def test_getDestinationServiceName(self):
        self.logger.debug("test_getDestinationServiceName")
        servName  = self.params.getDestinationServiceName()
        msg = 'oracle db instance for destination {0}'
        self.logger.debug(msg.format(servName))
        
    def test_getDestinationHost(self):
        self.logger.debug("test_getDestinationHost")
        host  = self.params.getDestinationHost()
        msg = 'Sde host is {0}'
        self.logger.debug(msg.format(host))
        
    def test_getFMWLogFileRelativePath(self):
        self.logger.debug("test_getFMWLogFileRelativePath")
        logFileRelPath = self.params.getFMWLogFileRelativePath()
        msg = 'log file relative path is {0}'
        self.logger.debug(msg.format(logFileRelPath))
            
    def test_getEasyConnectString(self):
        self.logger.debug("test_getEasyConnectString")
        self.origMacros = self.params.fmeMacroVals.copy()
        
        try:
            # default setup in the testing fmw does not include
            # the parameter src_servicename
            self.assertRaises(ValueError, self.params.getSrcEasyConnectString())
            raise NameError, 'expected get easy connect string to fail as the service name is undefined '
        except:
            # should fail, if it hasn't then 
            pass
        if not self.params.existsSourceOracleServiceName(None):
            msg = 'The parameter {0} does not exist in the fmw'
            raise KeyError, msg.format(self.params.const.FMWParams_SrcServiceName)
        #self.params.fmeMacroVals[self.params.const.FMWParams_SrcServiceName] = self.params.fmeMacroVals[self.params.const.FMWParams_SrcInstance]
        easyConnectString = self.params.getSrcEasyConnectString()
        
        msg = 'easy connect string retrieved is {0}'
        self.logger.debug(msg.format(easyConnectString))
        
class StartupTester():
    '''
    This class attempts to test and verify that the various
    pieces that can be included in a start up are working 
    
    These are the default lines to be included in all 
    fmw startups:
    
    import fme
    import DataBCFMWTemplate
    start = DataBCFMWTemplate.Start(fme)
    start.startup()
    '''
    
    def __init__(self, fmeModule):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.fme = fmeModule
        start = DataBCFMWTemplate.Start(self.fme)
        start.startup()
        
        
        # now report to the log what parameters were calculated 
        # by the startup
        self.logger.debug('------------------------------------')
        self.logger.debug('----      PARAM REPORT        ------')
        self.logger.debug('------------------------------------')

        msg = '{0} : {1}'
        self.logger.debug(msg.format('DataBCFMWTemplate module path', DataBCFMWTemplate.__file__))
        changeLogPath = start.config.getChangeLogsDirFullPath()
        self.logger.debug(msg.format('change log path', changeLogPath))
        
        changeFile = start.config.getChangeLogFile()
        self.logger.debug(msg.format('Dest db key', start.config.getDestinationDatabaseKey(start.config.key)))
        self.logger.debug(msg.format('destination cross reference', start.config.key))

        self.logger.debug(msg.format('dev mode credentials file name', start.config.getDevelopmentModeCredentialsFileName()))
        self.logger.debug(msg.format('change file', changeFile))
        self.logger.debug(msg.format('config directory', start.config.getConfigDirName()))
        self.logger.debug(msg.format('template root dir', start.config.getTemplateRootDirectory()))
        self.logger.debug(msg.format('failed features dir', start.config.getFailedFeaturesDir()))
        self.logger.debug(msg.format('failed features file', start.config.getFailedFeaturesFile()))
        self.logger.debug(msg.format('outputs directory', start.config.getOutputsDirectory()))
        self.logger.debug(msg.format('destination server', start.config.getDestinationHost()))
        self.logger.debug(msg.format('destination sde port', start.config.getDestinationSDEPort()))
        self.logger.debug(msg.format('sde connection file', start.params.getDestDatabaseConnectionFilePath()))
        self.logger.debug(msg.format('is dest prod', start.config.isDestProd()))
        self.logger.debug(msg.format('is run on databc node', start.config.isDataBCNode()))
        self.logger.debug(msg.format('destination oracle port', start.config.getDestinationOraclePort()))
        # no longer a valid method, use getservicename instead.
        #self.logger.debug(msg.format('destination oracle instance', start.config.getDestinationInstance()))
        self.logger.debug(msg.format('pmp source resource', start.config.getSourcePmpResources()))
        self.logger.debug(msg.format('pmp destination resource', start.config.getDestinationPmpResource()))
        self.logger.debug('--------------- END ----------------')

        
     #TODO:test to verify that if on a non databc computer the config file readers
     # getChangeLogsDirFullPath returns the same directory as the fmw.
