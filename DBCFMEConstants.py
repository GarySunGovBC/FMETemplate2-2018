'''
Created on Sep 26, 2018

@author: kjnether
'''


class TemplateConstants(object):
    '''
    Constants used by multiple classes / modules that make up the framework.

    Includes mostly parameter names
    '''
    # no need for a logger in this class as its just
    # a list of properties
    #
    # Maps to sections in the config file
    AppConfigFileName = 'templateDefaults.config'
    AppConfigConfigDir = 'config'
    AppConfigSecretsDir = 'secrets'
    AppConfigOutputsDir = 'outputs'
    AppConfigLogFileExtension = '.log'
    AppConfigSdeConnFileExtension = '.sde'
    AppConfigLogDir = 'log'
    AppConfigAppLogFileName = 'applogconfigfilename'

    # fmeServer configuration deployment file
    FrameworkFMEServDeploymentFile = 'FMEServerDeployment.json'

    # when a script requires the
    # sshTempFile =

    # parameters relating to template sections
    ConfFileSection_global = 'global'
    ConfFileSection_global_key_rootDir = 'rootscriptdir'
    ConfFileSection_global_key_outDir = 'outputsbasedir'
    ConfFileSection_global_key_kirkOutDir = 'kirkoutdir'
    ConfFileSection_global_key_pidDir = 'pid_dir'
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
    ConfFileSection_global_directConnectClientString = \
        'directconnect_clientstring'
    ConfFileSection_global_directConnectSSClientString = \
        'directconnect_ss_clientstring'
    ConfFileSection_global_ARCGISDesktopRootDir = 'defaultArcRootInstall'
    ConfFileSection_global_PythonRootDir = 'defaultArcPythonPath'
    ConfFileSection_global_FMERootDirTmplt = 'defaultFMERootInstallTmplt'
    ConfFileSection_global_FMERootDirTmplt32Bit = 'defaultFMERootInstallTmplt32bit'
    
    ConfFileSection_destKeywords = 'dest_param_keywords'

    ConfFileSection_putty = 'putty'
    ConfFileSection_puttyDir = 'puttydir'
    ConfFileSection_puttyFile = 'puttycommand'

    # properties from the config file.
    # server is deprecated, replaced by host
    # ConfFileSection_serverKey = 'server'
    ConfFileSection_hostKey = 'host'
    ConfFileSection_pmpResKey = 'pmpresource'
    ConfFileSection_oraPortKey = 'oracleport'
    ConfFileSection_sdePortKey = 'sdeport'
    ConfFileSection_serviceNameKey = 'servicename'
    ConfFileSection_instanceAliasesKey = 'instance_aliases'
    ConfFileSection_dwmKey = 'dwmkey'

    ConfFileSection_pmpTokens = 'pmptokens'

    ConfFileDestKey_Prod = 'prod'
    ConfFileDestKey_Test = 'test'
    ConfFileDestKey_Deliv = 'dlv'
    ConfFileDestKey_Devel = 'dev'
    ConfFileDestKey_Other = 'other'

    ConfFile_dwm = 'dwm_config'
    ConfFile_dwm_pmpresource = 'pmp_resource'
    ConfFile_dwm_dbuser = 'db_user'
    ConfFile_dwm_dbinstance = 'db_instance'
    ConfFile_dwm_dbserver = 'db_server'
    ConfFile_dwm_dbport = 'db_port'
    ConfFile_dwm_table = 'dwmtable'
    ConfFile_dwm_valid_dest_keys = 'dwm_valid_keys'

    # dependency management related parameters
    ConfFile_deps = 'dependencies'
    ConfFile_deps_timewindow = 'timewindow'
    ConfFile_deps_maxretries = 'maxretries'
    ConfFile_deps_waittime = 'waittime'

    # Environment variable names that are consumed by the framework
    EnvVar_LogDir = 'DBC_FRAMEWORK_LOGDIR'

    # jenkins params
    jenkinsSection = 'jenkins'
    jenkinsSection_createSDEconnFile_token = 'buildsdeconnfile_token'
    jenkinsSection_createSDEconnFile_url = 'buildsdeconnfile_url'
    # These are the pointers to the config file
    # where the names of the args to be sent
    # to the rest job are located
    jenkinsSection_param_ServiceName = 'ServiceName'
    jenkinsSection_param_SDEConnFilePath = 'SDEConnFilePath'
    jenkinsSection_param_Host = 'Host'
    jenkinsSection_param_Token = 'token'
    jenkinsSection_param_Port = 'port'
    # sqlserver parameters
    sqlserverSection = 'sqlserver'
    sqlserver_param_port = 'defaultport'
    sqlserver_param_pmpidentifier = 'pmpidentifier'

    # email parameters
    emailerSection = 'notifications'
    emailer_smtpServer = 'smtpserver'
    emailer_smtpPort = 'smtpport'
    emailer_fromAddress = 'emailfrom'
    defaultEmailOnFailure = 'dataetl@gov.bc.ca'

    # kirk parameters
    kirkSection = 'kirk'
    kirk_url = 'url'
    kirk_token = 'token'

    # When creating a connection file the framework will initiate a jenkins job
    # it will then wait for this amount of time before testing to see if the
    # jenkins job has created the sde file.  If it has not it will retry
    # ___ number of times
    sdeConnFileMaxRetries = 20
    sdeConnFilePollWaitTimeSeconds = 10

    # published parameters - for kirk
    FMWParams_KirkJobId = 'KIRK_JOBID'
    FMWParams_KirkFldMapCnt = 'KIRK_FLDMAPCNT'
    FMWParams_KirkCounterAttribute = 'KIRK_COUNTERATTRIBUTE'
    FMWParams_KirkCounterTmpAttribute = 'KIRK_TMP_COUNTER_ATTRIBUTE'
    FMWParams_KirkDestDbKeyOverride = 'KIRK_DEST_DB_KEY_OVERRIDE'

    # published parameters - destination
    FMWParams_DestKey = 'DEST_DB_ENV_KEY' 
    FMWParams_DestSchema = 'DEST_SCHEMA'
    FMWParams_DestType = 'DEST_TYPE'
    FMWParams_DestFeatPrefix = 'DEST_FEATURE_'
    FMWParams_DestServer = 'DEST_SERVER'
    FMWParams_DestPort = 'DEST_PORT'
    # FMWParams_DestInstance = 'DEST_INSTANCE'
    FMWParams_DestServiceName = 'DEST_ORA_SERVICENAME'
    FMWParams_DestPassword = 'DEST_PASSWORD'

    FMWParams_FailedFeatures = 'FAILED_FEATURES'

    # published parameters - source
    # prefix for any file based source dataset
    FMWParams_srcDataSet = 'SRC_DATASET_'
    FMWParams_SrcFGDBPrefix = 'SRC_DATASET_FGDB_'
    FMWParams_SrcXLSPrefix = 'SRC_DATASET_XLS_'
    FMWParams_SrcFeaturePrefix = 'SRC_FEATURE_'
    FMWParams_SrcSchema = 'SRC_ORA_SCHEMA'
    FMWParams_SrcOraConnFile = 'SRC_ORA_CONNFILE'
    FMWParams_SrcOraPassword = 'SRC_ORA_PASSWORD'
    FMWParams_SrcProxySchema = 'SRC_ORA_PROXY_SCHEMA'
    FMWParams_SrcSSSchema = 'SRC_SS_SCHEMA'
    FMWParams_SrcProxySSSchema = 'SRC_SS_PROXY_SCHEMA'
    FMWParams_SrcSSDbName = 'SRC_SS_DBNAME'

    FMWParams_SrcSSPswd = 'SRC_SS_PASSWORD'

    FMWParams_SrcInstance = 'SRC_ORA_INSTANCE'
    FMWParams_SrcServiceName = 'SRC_ORA_SERVICENAME'
    FMWParams_SrcHost = 'SRC_HOST'
    FMWParams_SrcPort = 'SRC_PORT'
    FMWParams_SrcSDEDirectConnectClientStr = 'SRC_ORA_CLIENTSTRING'
    # if there is more than one source instance use the method
    # getSrcInstanceParam to retrieve it

    FMWParams_SrcFeatPrefix = 'SRC_FEATURE_'

    FMWParams_FileChangeEnabledParam = 'FILE_CHANGE_DETECTION'

    # ssh tunnelling parameters
    FMWParams_SSHTunnel_LocalPort = 'SSH_LOCALPORT'
    FMWParams_SSHTunnel_DestPort = 'SSH_DESTPORT'
    FMWParams_SSHTunnel_DestHost = 'SSH_DESTHOST'
    FMWParams_SSHTunnel_HostUsername = 'SSH_USERNAME'
    FMWParams_SSHTunnel_KeyFile = 'SSH_KEYFILE'

    # dependency management parameters
    FMWParams_Deps_fmwList = 'DEP_FMW'
    FMWParams_Deps_timeWindow = 'DEP_TIMEWINDOW'
    FMWParams_Deps_waitTime = 'DEP_WAITTIME'
    FMWParams_Deps_maxRetry = 'DEP_MAXRETRIES'

    # email notification parameters
    FMWParams_Notify_All = 'NOTIFY_ALL'
    FMWParams_Notify_Fail = 'NOTIFY_FAILURE'
    FMWParams_Notify_Success = 'NOTIFIY_SUCCESS'

    # The fmw macrovalue used to retrieve the directory
    # that the fmw is in.
    FMWMacroKey_FMWDirectory = 'FME_MF_DIR'
    FMWMacroKey_FMWName = 'FME_MF_NAME'

    # fme macro value that will contain the fme server job id
    FMEMacroKey_JobId = 'FME_JOB_ID'
    FMEMacroKey_LogFileName = 'LOG_FILENAME'

    FMEServerSection = 'fmeserver'
    FMEServerSection_Host = 'host'
    # FMEServerSection_RootDir = '/fmerest/v2/'
    FMEServerSection_RootDir = 'rootdir'
    FMEServerSection_Token = 'token'

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
    # DevelopmentDatabaseCredentialsFile_dbInst = 'instance' # this parameter is deprecated, using service name now @IgnorePep8
    DevelopmentDatabaseCredentialsFile_dbServName = 'servicename'
    DevelopmentDatabaseCredentialsFile_dbPswd = 'password'
    DevelopmentDatabaseCredentialsFile_SSDbName = 'SqlServerDbName'
    DevelopmentDatabaseCredentialsFile_SSDbHost = 'SqlServerDbHost'

    # TODO: Ideally this would be pulled from either a source code repository
    #       like gogs, or from the root directory of the framework home
    svn_DevelopmentJSONFile_Url = \
        r'\\data.bcgov\work\scripts\python\DataBCFmeTemplate2\config\db' + \
        r'Creds.json'

    # log format strings
    FMELogShutdownFormatString = \
        '%(asctime)s|   ?.?|  0.0|PYTHON SHUTDOWN| %(levelname)s: %(message)s'
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

    PIDFileName = 'pid.txt'

    LoggingExtendedLoggerName = 'ExtendedStartup'

    # keys used to retrieve objects from FME Server
    # here is an example structure of a detailed job object
    '''
    {   175323: {   u'description': u'',
                u'engineHost': u'arneb',
                u'engineName': u'ARNEB_Engine6',
                u'id': 175323,
                u'request': {   u'FMEDirectives': {   },
                                u'NMDirectives': {   u'failureTopics': [],
                                                     u'successTopics': []},
                                u'TMDirectives': {   u'description': u'',
                                                     u'priority': 100,
                                                     u'rtc': False,
                                                     u'tag': u''},
                                u'publishedParameters': [   {   u'name': u'SRC_DATASET_FGDB_1',  # @IgnorePep8
                                                                u'raw': u'\\\\data.bcgov\\data_staging\\BCGW\\administrative_boundaries\\alr_data.gdb'},  # @IgnorePep8
                                                            {   u'name': u'FME_SECURITY_ROLES',  # @IgnorePep8
                                                                u'raw': u'fmeadmin fmesuperuser'},  # @IgnorePep8
                                                            {   u'name': u'DEST_DB_ENV_KEY',  # @IgnorePep8
                                                                u'raw': u'PRD'},  # @IgnorePep8
                                                            {   u'name': u'DEST_SCHEMA',  # @IgnorePep8
                                                                u'raw': u'whse_legal_admin_boundaries'},  # @IgnorePep8
                                                            {   u'name': u'FME_SECURITY_USER',  # @IgnorePep8
                                                                u'raw': u'dwrs'},  # @IgnorePep8
                                                            {   u'name': u'FILE_CHANGE_DETECTION',  # @IgnorePep8
                                                                u'raw': u'TRUE'},  # @IgnorePep8
                                                            {   u'name': u'SRC_FEATURE_1',  # @IgnorePep8
                                                                u'raw': u'ALR_Arcs'},  # @IgnorePep8
                                                            {   u'name': u'DEST_FEATURE_1',  # @IgnorePep8
                                                                u'raw': u'OATS_ALR_BOUNDARY_LINES'}],  # @IgnorePep8
                                u'subsection': u'REST_SERVICE',
                                u'workspacePath': u'"BCGW_SCHEDULED/oats_alr_boundary_lines_staging_gdb_bcgw/oats_alr_boundary_lines_staging_gdb_bcgw.fmw"'},  # @IgnorePep8
                u'result': {   u'id': 175323,
                               u'numFeaturesOutput': 53925,
                               u'priority': 100,
                               u'requesterHost': u'142.34.140.26',
                               u'requesterResultPort': -1,
                               u'status': u'SUCCESS',
                               u'statusMessage': u'Translation Successful',
                               u'timeFinished': u'2017-05-29T01:33:55',
                               u'timeRequested': u'2017-05-29T01:30:00',
                               u'timeStarted': u'2017-05-29T01:30:00'},
                u'status': u'SUCCESS',
                u'timeDelivered': u'2017-05-29T01:33:55',
                u'timeFinished': u'2017-05-29T01:33:55',
                u'timeQueued': u'2017-05-29T01:30:00',
                u'timeStarted': u'2017-05-29T01:30:00',
                u'timeSubmitted': u'2017-05-29T01:30:00'},

    '''
    # key used to retrieve time finished from fme server rest api job object
    FMEServerParams_timeFinished = 'timeFinished'
    FMEServerParams_timeSubmitted = 'timeSubmitted'
    FMEServerParams_request = 'request'
    FMEServerParams_pubParams = 'publishedParameters'
    FMEServerParams_wsPath = 'workspacePath'
    FMEServerParams_result = 'result'
    FMEServerParams_status = 'status'

    # example of a date string returned from fme server rest api:
    #                 u'timeStarted': u'2017-05-29T10:00:04',
    FMEServer_DatetimeFormat = '%Y-%m-%dT%H:%M:%S'

    # The version of python the framework is configured to use.
    # used to set up paths to arcpy, allows for multiple python
    # installs.
    PythonVersion = '2.7'

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
        val = self.calcNumVal(self.FMWParams_SrcSDEDirectConnectClientStr,
                              position)
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
                raise ValueError(msg)
        retValtemplate = '{0}_{1}'
        retVal = retValtemplate.format(val, position)
        return retVal

