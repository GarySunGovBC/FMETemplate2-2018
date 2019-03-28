'''

This is the library that is used to support the DataBC
FME framework.  The framework does the following:

a) standardizes the parameter names that can be used in
   an FMW for source and destination data sets.  This
   is what allows us to quickly answer questions about
   the status of data sets.

b) a library (this lib) that is used to calculate various
   derived values such as:
     - passwords (retrieved from pmp)
     - oracle easy connect strings
     - oracle sde direct connect strings
     - etc...

'''
import ConfigParser
import datetime
import importlib
import inspect
import json
import logging.config
import os
import platform
import pprint
import re
import shutil
import site
import sys
import time
import InstallPaths
import FFSReader

import PMP.PMPRestConnect

import DB.DbLib
import DataBCEmailer as Emailer
import DataBCDbMethods
import FMWExecutionOrderDependencies
from DBCFMEConstants import TemplateConstants
import requests

# pylint: disable=invalid-name


class Start(object):
    '''
    The base start class. Any other startup methods should inherit this class.
    '''

    def __init__(self, fme):

        # getting the app constants
        self.const = TemplateConstants()
        self.fme = fme
        self.params = CalcParamsBase(self.fme.macroValues)
        self.params.addPlugin()
        fmwDir = self.params.getFMWDirectory()
        fmwName = self.params.getFMWFile()
        destKey = self.params.getDestDatabaseEnvKey()

        # fmwDir = fme.macroValues[self.const.FMWMacroKey_FMWDirectory]
        # fmwName = fme.macroValues[self.const.FMWMacroKey_FMWName]
        # destKey = fme.macroValues[self.const.FMWParams_DestKey]
        # set up logging
#         ModuleLogConfig(fmwDir, fmwName, destKey)
#         modDotClass = '{0}.{1}'.format(__name__, 'shutdown')
#         self.logger = logging.getLogger(modDotClass)
        self.logger = logging.getLogger(__name__)

        self.logger.info('running the framework startup, on fmw: %s', fmwName)
        self.logger.debug('source fmw dir: %s', fmwDir)
        # Reading the global paramater config file

        self.config = TemplateConfigFileReader(destKey)
        customScriptDir = self.config.getCustomScriptDirectory()
        # Extract the custom script directory from config file
        # customScriptDir = self.paramObj.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_customScriptDir) @IgnorePep8
        # Assemble the name of a the custom script
        justScript = (os.path.splitext(fmwName))[0]

        customScriptFullPath = os.path.join(customScriptDir,
                                            justScript + '.py')
        customScriptLocal = os.path.join(
            self.fme.macroValues[self.const.FMWMacroKey_FMWDirectory],
            justScript + '.py')

        # test to see if the custom script exists, if it does import it, and
        # set the plugin parameter = to the Start() object.
        startupScriptDirPath = None
        if os.path.exists(customScriptLocal):
            startupScriptDirPath = \
                self.fme.macroValues[self.const.FMWMacroKey_FMWDirectory]
        elif os.path.exists(customScriptFullPath):
            startupScriptDirPath = customScriptDir

        # if there is a startup script associated with the current fmw that
        # is being run then import that script and run the startup code
        # in that script if it exists, vs the default startup code.
        if startupScriptDirPath:
            # looking for custom module to load.  If one is found that
            # startup will take precidence
            self.logger.debug("adding to pythonpath {0}".format(
                startupScriptDirPath))
            site.addsitedir(startupScriptDirPath)
            self.logger.debug("python path has been appended successfully")
            self.logger.debug("trying to load the module {0}".format(
                justScript))
            startupModule = importlib.import_module(justScript)
            self.logger.debug("module is now imporated: {0}".format(
                justScript))
            # is there a start class defined in the startup module
            if Util.isClassInModule(startupModule, 'Start'):
                self.logger.debug("{0} module loaded successfully".format(
                    justScript))
                self.startupObj = startupModule.Start(self.fme)
            else:
                self.logger.debug('using the generic template startup')
                self.startupObj = DefaultStart(self.fme)
        else:
            # setting the statup object to be the default one
            self.logger.debug('using the generic template startup')
            self.startupObj = DefaultStart(self.fme)

    def startup(self):
        '''
        base startup method
        '''
        # default startup routine
        # self.fme.macroValues[self.const.FMWParams_DestKey]
        # debugging / develeopment - printing the macrovalues.
        # useful for setting up test cases.
        # --------------------------------------------------------
        # will either call the default startup or a customized one
        self.logger.debug("calling the startup method...")
        self.startupObj.startup()


class DefaultStart(object):
    '''
    The default startup method.  This is the code that is run by default for
    startups.  The functionality can be overridden by creating a python file
    with the same name as the fmw that is being run.  Examples exist!
    '''

    def __init__(self, fme):
        self.fme = fme
#         modDotClass = '{0}.{1}'.format(__name__, 'shutdown')
#         self.logger = logging.getLogger(modDotClass)
        self.logger = logging.getLogger(__name__)

    def startup(self):
        '''
        The actual default startup code.  This is the code that vanilla
        framework scripts will execute.
        '''
        # currently there is no default startup code.
        # if there was it would go here
        # params = GetPublishedParams(self.fme.macroValues)
        params = CalcParamsBase(self.fme.macroValues)
        envKey = params.getDestDatabaseEnvKey()
        config = TemplateConfigFileReader(envKey)
        const = TemplateConstants()
        self.logger.info("running default startup")

        # possible statuses:
        #  - SUCCESS - most common
        #  - FME_FAILURE - most common failure status
        #  - JOB_FAILURE - other failed status
        #  - SUBMITTED
        #  - QUEUED
        #  - DELAYED
        #  - PAUSED
        #  - IN_PROCESS
        #  - DELETED
        #  - ABORTED
        #  - PULLED

        # code associated with dependency of fmw's (these need to be defined
        # in the published parameters, example:
        # https://docs.google.com/document/d/1WXUMN-2RDC_7u3OgtbG-NYFevKqZcZlxwF4PEB_lpu4/edit#heading=h.sszw3nkwygkb @IgnorePep8
        if params.existsDependentFMWSs():
            msg = 'The script has dependencies defined.'
            self.logger.info(msg)
            # only proceed if executing on fme server
            if config.isDataBCFMEServerNode():
                msg = 'The script is being run on an FME Server node. Initiating ' + \
                      'the fmw '
                self.logger.info(msg)

                # retrieve the parameters to create the dependencies
                depFMWs = params.getDependentFMWs()
                depTimeWindow = params.getDependencyTimeWindow()
                depMaxRetries = params.getDependencyMaxRetries()
                depWaitTime = params.getDependencyWaitTime()

                fmeSrvHost = config.getFMEServerHost()
                # commenting out because its not used at this time
                # fmeSrvDir = config.getFMEServerRootDir()
                fmeSrvToken = config.getFMEServerToken()

                execOrder = FMWExecutionOrderDependencies.ExecutionOrder(
                    depFMWs,
                    depTimeWindow,
                    depMaxRetries,
                    depWaitTime,
                    fmeSrvHost,
                    fmeSrvToken)
                retries = 0

                complete = execOrder.isParentsComplete()
                if not complete:
                    while not complete:
                        if retries >= depMaxRetries:
                            msg = 'The dependent FMW jobs: {0} described in the parameter ' + \
                                  '{1} have not been run.  FME Server has been probed {2} ' + \
                                  'times.  This FMW will not proceed.'
                            msg = msg.format(depFMWs,
                                             const.FMWParams_Deps_fmwList,
                                             retries)
                            self.logger.error(msg)
                            raise DependenciesNotMet(msg)
                        else:
                            msg = "The dependencies: {0} described in the " + \
                                  "parameter {1} have not been found to " + \
                                  "have run yet, or they are currently still " + \
                                  "in process. Currently on retry {2} of a " + \
                                  " maximum of {3}"
                            msg = msg.format(depFMWs,
                                             const.FMWParams_Deps_fmwList,
                                             retries,
                                             depMaxRetries)
                            self.logger.info(msg)
                            self.logger.info("waiting for {0} seconds".format(
                                depWaitTime))
                            time.sleep(depWaitTime)
                        complete = execOrder.isParentsComplete()
                        retries += 1


class Shutdown(object):
    '''
    The base Shutdown class, all other shutdowns will inherit
    from this class.  Typically scrips will run the default shutdown unless
    there is a custom shutdown process defined specifically for an FMW.
    '''

    def __init__(self, fme):
        # This method will always be called regardless of any customizations.
        self.fme = fme
        self.const = TemplateConstants()

        self.params = CalcParamsBase(self.fme.macroValues)
        destKey = self.params.getDestDatabaseEnvKey()
        self.config = TemplateConfigFileReader(destKey)
#        self.config = \
#            TemplateConfigFileReader(
#                self.fme.macroValues[self.const.FMWParams_DestKey])

        fmwDir = self.params.getFMWDirectory()
        fmwName = self.params.getFMWFile()
        destKey = self.params.getDestDatabaseEnvKey()
        # validating the key, translates loosly defined destination
        # database environment keys into the authoritative keys
        destKey = self.config.getDestinationDatabaseKey(destKey)

        # logging configuration
        ModuleLogConfig(fmwDir, fmwName, destKey)
        # leaving this as is so we have more control over toggling shutdown
        # code logging levels
        modDotClass = '{0}.{1}'.format(__name__, 'shutdown')
        self.logger = logging.getLogger(modDotClass)
        self.logger.info("Shutdown has been called...")
        self.logger.debug("log file name: {0}".format(self.fme.logFileName))

        # looking for custom script for shutdown
        customScriptDir = self.config.getCustomScriptDirectory()
        self.logger.debug("fmwName: %s", fmwName)
        justScript = os.path.splitext(fmwName)[0]
        customScriptFullPath = os.path.join(customScriptDir,
                                            justScript + '.py')
        customScriptLocal = os.path.join(fmwDir, justScript + '.py')

        # test to see if the custom script exists, if it does import it, and
        # set the plugin parameter = to the Shutdown() object.
        shutdownScriptDirPath = None
        if os.path.exists(customScriptLocal):
            shutdownScriptDirPath = fmwDir
        elif os.path.exists(customScriptFullPath):
            shutdownScriptDirPath = customScriptDir
            site.addsitedir(shutdownScriptDirPath)
            shutdownModule = importlib.import_module(justScript)
            if Util.isClassInModule(shutdownModule, 'Shutdown'):
                # use the custom shutdown
                self.logger.debug("%s module loaded successfully", justScript)
                self.shutdownObj = shutdownModule.Shutdown(self.fme)
            else:
                self.logger.debug('using the generic template shutdown')
                self.shutdownObj = DefaultShutdown(self.fme)
        else:
            self.logger.debug('using the generic template shutdown')
            self.shutdownObj = DefaultShutdown(self.fme)

    def shutdown(self):
        '''
        Calling shutdown code
        '''
        self.shutdownObj.shutdown()


class DefaultShutdown(object):
    '''
    The default shutdown class, if no other shutdown is configured this one
    will get run.
    '''

    def __init__(self, fme):
        self.fme = fme
        self.const = TemplateConstants()
        self.params = CalcParamsBase(self.fme.macroValues)
        destKey = self.params.getDestDatabaseEnvKey()
        self.config = TemplateConfigFileReader(destKey)

        self.params.addPlugin()

        loggerName = '{0}.{1}'.format(__name__, 'shutdown')
        self.logger = logging.getLogger(loggerName)
        self.logger.info("DefaultShutdown shutdown has been called")
        # self.logger.info('destination key word in shutdown: %s',
        #                 self.fme.macroValues[self.const.FMWParams_DestKey])
        self.logger.info('destination key word in shutdown: %s',
                         destKey)

    def shutdown(self):
        exceptionRaised = False
        try:
            # getDestDatabaseEnvKey, and then validate it retrieving the
            # validated version
            destKey = self.params.getDestDatabaseEnvKey()
            destKey = self.config.getDestinationDatabaseKey(destKey)

            if not self.config.isDataBCNode():
                # either not being run on a databc computer, or is being run in
                # development mode, either way should not be writing to to the
                # DWM logger.
                msg = "DWM record is not being writen as script is being " + \
                      "run external to databc firewalls."
                self.logger.info(msg)
            elif destKey in \
                [self.const.ConfFileDestKey_Devel,
                 self.const.ConfFileDestKey_Other]:
                msg = 'DWM record is not being written because the script is being ' + \
                      'run in development mode'
                self.logger.info(msg)
            else:
                dwmValidKeys = self.config.getDWMValidDestinationKeywords()
                # if destKey.lower() in ['dlv', 'tst', 'prd']:
                if destKey.lower() in dwmValidKeys:
                    self.logger.info("DWM record is being created")
                    dwmWriter = DWMWriter(self.fme)
                    # dwmWriter.printParams()
                    dwmWriter.writeRecord()
                else:
                    self.logger.info("destination key is %s so no record is "
                                     "being written to dwm", destKey)

            # don't run analyze if the destination database key word is 'other'
            # of if its being run on a non databc computer
            if not self.config.isDestOther() and not \
                    self.config.isDataBCNode():

                self.logger.info('Starting into analyze block, destination ' +
                                 'key word: %s',
                                 self.fme.macroValues[
                                     self.const.FMWParams_DestKey])
                # analyze destination tables
                dbMeth = DataBCDbMethods.DataBCDbMethods(self.fme,
                                                         self.const,
                                                         self.params,
                                                         self.config)
                dbMeth.analyzeDestinationFeatures()

                # default notifications
                self.logger.debug("starting to process notifications")
        except Exception, e:
            self.logger.exception("Exception raised in Shutdown")
            exceptionRaised = True
        finally:
            # emailer block
            if self.config.isDataBCNode():
                self.logger.debug("starting into the notification block")
                emailer = Emailer.EmailFrameworkBridge(self.fme, self.const,
                                                       self.params,
                                                       self.config)
                email2Add = self.const.defaultEmailOnFailure
                # if an exception was raised in the shutdown then send out
                # an email notifying of this situation even if the fmw
                # completed successfully
                if exceptionRaised:
                    # the exception was in the shutdown not a problem with
                    # the actual fmw so only email databc.  Currently in
                    # beta feature, so emailing only myself
                    emailer.addNotifyEmail('FAIL', email2Add)
                    # emailer.notifyFail = email2Add
                # add default email address for failures.
                elif not emailer.notifyFail:
                    emailer.addNotifyEmail('FAIL', email2Add)
                    self.logger.debug("adding email address to fails")
                else:
                    if email2Add.lower() not in emailer.notifyFail:
                        emailer.addNotifyEmail('FAIL', email2Add)
                self.logger.debug("getting ready to send notification")
                try:
                    emailer.sendNotifications()
                except:
                    self.logger.exception("ERROR RAISED IN NOTIFICATIONS")
                    self.logger.error("error raised when trying to create " +
                                      "an emailer")
                    raise ValueError('crashed when trying to send emailer')
                self.logger.debug("notifications are complete")
                self.logger.info("shutdown is now complete")


class TemplateConfigFileReader(object):
    '''
    reads the fme template config file and provides an interface to the
    data that is stored in that file.
    '''

    def __init__(self, key, confFile=None):
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)

        self.parser = None
        self.key = None

        self.confFile = confFile
        self.logger.info("Reading the config file: %s", self.confFile)
        self.const = TemplateConstants()

        # This should populate self.parser
        self.readConfigFile()
        # Setting the key to the authoritative key, looks at the key that was
        # provided compares against aliases, and if found in the aliases then
        # resets the key to the authoritative key, example would take something
        # like Deliv and turn it into DLV
        self.setDestinationDatabaseEnvKey(key)
        msg = "Destination database environment key has been set to ({0}) " + \
              " the raw input key is ({1})"
        msg = msg.format(self.key, key)
        self.logger.info(msg)

    def calcEnhancedLoggingFileOutputDirectory(self, fmwDir, fmwName):
        '''
        If running on fme server, then will output a path
        relative to the template directory,

        If run on a non fme server node then will output a path
        relative to the actual fmw being run.

        '''
        justFmwName = os.path.splitext(fmwName)[0]
        # if not self.isFMEServerNode():
        logDirKey = self.const.EnvVar_LogDir
        if (logDirKey in os.environ) and os.environ[logDirKey]:
            # if this env var is set then it will be used for the location
            # of the log dir
            outputsDir = self.getOutputsDirectory()
            logDir = os.environ[logDirKey]
            fullPath = os.path.join(fmwDir, outputsDir, logDir, justFmwName)
        elif not self.isDataBCNode():
            outputsDir = self.getOutputsDirectory()
            logDir = self.const.AppConfigLogDir
            fullPath = os.path.join(fmwDir, outputsDir, logDir, justFmwName)
        else:
            templateDir = self.getTemplateRootDirectory()
            outputsDir = self.getOutputsDirectory()
            fullPath = os.path.join(templateDir, outputsDir,
                                    'log', justFmwName)
        fullPath = os.path.normpath(fullPath)
        if not os.path.exists(fullPath):
            os.makedirs(fullPath)
        fullPath = fullPath.replace('\\', '/')
        return fullPath

    def calcPIDCacheDirectory(self, fmwDir, fmwName):
        justFmwName = os.path.splitext(fmwName)[0]

        pidDirName = self.parser.get(self.const.ConfFileSection_global,
                                     self.const.ConfFileSection_global_key_pidDir)  # @IgnorePep8
        outputsDir = self.getOutputsDirectory()

        if not self.isDataBCNode():
            fullPath = os.path.join(fmwDir, outputsDir,
                                    pidDirName, justFmwName)
        else:
            templateDir = self.getTemplateRootDirectory()
            outputsDir = self.getOutputsDirectory()
            fullPath = os.path.join(templateDir, outputsDir,
                                    pidDirName, justFmwName)

        fullPath = os.path.normpath(fullPath)
        if not os.path.exists(fullPath):
            os.makedirs(fullPath)
        fullPath = fullPath.replace('\\', '/')
        return fullPath

    def calcPuttyExecPath(self):
        puttyDir = self.parser.get(self.const.ConfFileSection_putty,
                                   self.const.ConfFileSection_puttyDir)
        puttyFile = self.parser.get(self.const.ConfFileSection_putty,
                                    self.const.ConfFileSection_puttyFile)

        outputDir = self.getTemplateRootDirectory()
        fullPath = os.path.join(outputDir, puttyDir, puttyFile)
        fullPath = os.path.normpath(fullPath)
        if not os.path.exists(fullPath):
            os.makedirs(fullPath)
        fullPath = fullPath.replace('\\', '/')
        return fullPath

    def getApplicationLogFileName(self):
        logConfFileName = self.parser.get(self.const.ConfFileSection_global,
                                          self.const.AppConfigAppLogFileName)
        return logConfFileName

    def getChangeLogsDirFullPath(self):
        # if self.isDataBCNode():
        rootDir = self.getTemplateRootDirectory()
        outputs = self.getOutputsDirectory()
        changeLogDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_changeLogDir)
        changeLogFullPath = os.path.join(rootDir, outputs, changeLogDir)
        changeLogFullPath = os.path.realpath(changeLogFullPath)
        return changeLogFullPath

    def getChangeLogFile(self):
        changeLogFile = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_changeLogFileName)
        return changeLogFile

    def getConfigDirName(self):
        '''
        Reads from the config file the name of the directory that any
        configuration files should be locted in.  Returns just the name of
        the directory, no path information is included.  To get the root
        directory call
        '''
        confDirName = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_configDirName)
        return confDirName

    def getCustomScriptDirectory(self):
        customScriptDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_customScriptDir)
        return customScriptDir

    def getDataBCFmeServerNodes(self):
        nodeString = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_key_govFmeServer)
        nodeList = nodeString.split(',')
        return nodeList

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
        nodeString = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_key_govComputers)
        nodeList = nodeString.split(',')
        return nodeList

    def getDefaultOraclePort(self):
        srcDefaultOraPort = self.parser.get(
            self.const.ConfFileSection_pmpSrc,
            self.const.ConfFileSection_pmpSrc_defaultOraPort)
        return srcDefaultOraPort

    def getDependencyTimeWindow(self):
        '''
        Returns the default depencency time window parameter that is
        stored in the framework config file

        '''
        self.logger.debug("getDependencyTimeWindow")
        depTimeWindow = self.parser.get(
            self.const.ConfFile_deps,
            self.const.ConfFile_deps_timewindow)
        return depTimeWindow

    def getDependencyMaxRetries(self):
        '''
         Returns the depencendy maximum number of retries to perform
         when search fme server for a dependent job.
        '''
        self.logger.debug("getDependencyMaxRetries")
        maxRetries = self.parser.get(
            self.const.ConfFile_deps,
            self.const.ConfFile_deps_maxretries)
        return maxRetries

    def getDependencyWaitTime(self):
        '''
         Returns the default wait time parameter that gets used when
         searching FME Server for a dependent job.
        '''
        self.logger.debug("getDependencyWaitTime")
        waitTime = self.parser.get(
            self.const.ConfFile_deps,
            self.const.ConfFile_deps_waittime)
        return waitTime

    def getDestinationDatabaseKey(self, inkey):
        '''
        :param inkey: This is the destination database env key that you want
                      to validate against defined keys from the config file.
        :type inkey: str

        For each destinantion key, there is a list in the framework config
        file that defined possible typos associated with each key.  This
        method takes the provided key and compares against the list of
        allowable keys.  It will then return the authoritative value.

        Example: you might send the method the key DELIV.  This method will
        then look in the config file, and start iterating over all the
        possible keys and the possible values that are associated with
        them.  The method will find the entry for delivery which is:

        dlv : delivery, deliv, del, bcgwdlv, bcgwdlv1, bcgwdlvr1, bcgwdlvr,
              dlvr, dlv

        In that line you will notice that there is an entry for 'deliv'
        therefor this method will return the authoritative key which is
        'dlv'

        The authoritative key can now be used to pull out values like
        destination host, destination servicename, etc.

        :return: The authoritative key that corresponds with the provided
                 key as defined in the framework config file.
        :rtype: str
        '''
        self.logger.debug("getDestinationDatabaseKey")
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

    def getDestinationHost(self):
        self.logger.debug("db env key: %s", self.key)
        self.logger.debug("host key: %s", self.const.ConfFileSection_hostKey)
        host = self.parser.get(self.key, self.const.ConfFileSection_hostKey)
        return host

    def getDestinationOraclePort(self):
        oraPort = self.parser.get(
            self.key,
            self.const.ConfFileSection_oraPortKey)
        return oraPort

    def getDestinationPmpResource(self, dbEnvKey=None):
        '''
        :param dbEnvKey: The destination database env key.  If none is
                         provided then will use the one that was defined
                         in the constructor

        :return: Returns a list of pmp resources to search.
        :rtype: list

        extracts from the config file what pmp resources should be searched
        for passwords.
        '''
        self.logger.debug("raw input dbEnv Key is {0}".format(dbEnvKey))
        if not dbEnvKey:
            dbEnvKey = self.key
        else:
            dbEnvKey = self.getDestinationDatabaseKey(dbEnvKey)
        pmpRes = self.parser.get(dbEnvKey,
                                 self.const.ConfFileSection_pmpResKey)
        if ',' in pmpRes:
            # there are multiple resources to search if there is a comma, going
            # to parse into a list, and then make sure there is no leading
            # or trailing spaces in the list.
            pmpRes = pmpRes.strip()
            pmpResList = pmpRes.split(',')
            for cnt in range(0, len(pmpResList)):
                pmpResList[cnt] = pmpResList[cnt].strip()
            self.logger.debug("The pmp resource list is: %s", pmpResList)
            pmpRes = pmpResList
        else:
            pmpRes = [pmpRes]
        return pmpRes

    def getDestinationSDEPort(self):
        '''
        :return: the destination sde port defined in the config file
        '''
        sdePort = self.parser.get(self.key,
                                  self.const.ConfFileSection_sdePortKey)
        return sdePort

    def getDestinationServiceName(self):
        '''
        :return: the destination database service name defined in the
                 config file
        '''
        inst = self.parser.get(self.key,
                               self.const.ConfFileSection_serviceNameKey)
        return inst

    def getDWMDestinationKey(self, envKey):
        '''
        When a destination is defined in the global configs for the FME
        framework you can define an optional parameter/property called:
        dwmkey.  See last line in example below

        Example:
         [dbcdlv]
            host: somehost.ca
            sdeport: None
            servicename: dbservname.ca
            instance_aliases: sbsv.ca,svname,bill
            oracleport: 2432
            pmpresource: myResource
            dwmkey: TheDWMKey

        This method will return the env key provided in the args if no
        dwmkey was defined.  If a dwmkey was defined then it will return
        that key.
        '''
        # validating the key
        authoritativeKey = self.getDestinationDatabaseKey(envKey)
        configKeyItems = self.parser.items(authoritativeKey)
        dwmKey = envKey
        for item in configKeyItems:
            if item[0].lower() == self.const.ConfFileSection_dwmKey.lower():
                self.logger.debug("found a dwm key in config %s", item[1])
                dwmKey = item[1]
                # now validate it
                dwmKey = self.getDestinationDatabaseKey(dwmKey)
                self.logger.debug("validated dwm key: %s", dwmKey)
        return dwmKey

    def getDevelopmentModeCredentialsFileName(self):
        '''
        Returns the file name string of the .json credential file
        that the script uses to retrieve database credentials from
        when its in development mode.

        :returns: name of the json file that is used to store database
                  credentials when the script is being developed.
        :rtype: string
        '''
        credsFileName = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_devCredsFile)
        return credsFileName

    def getDWMDbUser(self):
        '''
        :return: the username / schema to use to access the dwm database
        '''
        return self.parser.get(self.const.ConfFile_dwm,
                               self.const.ConfFile_dwm_dbuser)

    def getDWMTable(self):
        '''
        :return: the table to write the dwm table to
        '''
        dwmTab = self.parser.get(self.const.ConfFile_dwm,
                                 self.const.ConfFile_dwm_table)
        return dwmTab

    def getDWMValidDestinationKeywords(self):
        '''
        :return: We only write records to dwm when the destination key is
                dlv,tst, or prd
        '''
        dwmTabStr = self.parser.get(
            self.const.ConfFile_dwm,
            self.const.ConfFile_dwm_valid_dest_keys)
        dwmList = dwmTabStr.split(',')
        for cntr in range(0, len(dwmList)):
            dwmList[cntr] = dwmList[cntr].strip()
        return dwmList

    def getFailedFeaturesDir(self):
        '''
        :return: The name of the directory where failed features files
                 should be stored
        '''
        failedFeatsDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_failedFeaturesDir)
        return failedFeatsDir

    def getFailedFeaturesFile(self):
        '''
        :return: the name of the file that should be used to store failed
                 features.

                 5-7-2018 - modifying so that the name of the ffs file is
                 going to be:

        $(DEST_SCHEMA)_$(DEST_FEATURE_1)_JOB_$(FME_JOB_ID)_failed_features.ffs
        '''
        failedFeatsFile = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_failedFeaturesFile)

        return failedFeatsFile

    def getFMEServerHost(self):
        '''
        :return: the fme server host name
        '''
        host = self.parser.get(
            self.const.FMEServerSection,
            self.const.FMEServerSection_Host)
        return host

    def getFMEServerRootDir(self):
        '''
        :return: the fme server root directory.  Starting place for the rest
                 api.
        '''
        rootdir = self.parser.get(
            self.const.FMEServerSection,
            self.const.FMEServerSection_RootDir)
        return rootdir

    def getFMEServerToken(self):
        '''
        :return: the token to use to communicate with FME server
        '''
        token = self.parser.get(
            self.const.FMEServerSection,
            self.const.FMEServerSection_Token)
        return token

    def getOracleDirectConnectClientString(self):
        '''
        :return: the client string that should be used for oracle direct
                 connects
        '''
        clientStr = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_directConnectClientString)
        return clientStr

    def getSSDirectConnectClientString(self):
        '''
        :return: the string that is used in an Sql Server SDE direct
                 connect to indicate the database type
        '''
        clientStr = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_directConnectSSClientString)
        return clientStr

    def getOutputsDirectory(self):
        '''
        :return: the path to the outputs directory
        '''
        ouptutsDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_key_outDir)
        return ouptutsDir

    def getKirkOutputsDirectory(self):
        '''
        :returns: the directory where kirk related logging information should
                  be written to
        '''
        ouptutsDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_key_kirkOutDir)
        return ouptutsDir

    def getPmpAltUrl(self):
        '''
        :return: the alternate pmp url.  PMP has a failover server, this
        is the url to the failover server.
        '''
        pmpAltUrl = self.parser.get(
            self.const.ConfFileSection_pmpConfig,
            self.const.ConfFileSection_pmpConfig_alturl)
        return pmpAltUrl

    def getPmpBaseUrl(self):
        '''
        :return: The base pmp url
        '''
        pmpBaseUrl = self.parser.get(
            self.const.ConfFileSection_pmpConfig,
            self.const.ConfFileSection_pmpConfig_baseurl)
        return pmpBaseUrl

    def getPmpRestDir(self):
        '''
        :return: pmp rest root directory

        Pmp directory where rest end points begin.
        '''
        restDir = self.parser.get(
            self.const.ConfFileSection_pmpConfig,
            self.const.ConfFileSection_pmpConfig_restdir)
        return restDir

    def getPmpToken(self, computerName):
        '''
        :return: the pmp token used to authorize communication with pmp.
        '''
        try:
            token = self.parser.get(self.const.ConfFileSection_pmpTokens,
                                    computerName)
        except ConfigParser.NoOptionError:
            msg = 'Trying to get a PMP token for the computer {0} but ' + \
                  'there are no pmp tokens defined for that machine in ' + \
                  'the app. config file: {1}.'
            msg = msg.format(computerName, self.confFile)
            self.logger.error(msg)
            raise ValueError(msg)
        return token

    def getSdeConnFileDirectory(self):
        '''
        This is going to just return the contents of the sde directory
        in the config file.


        # getting the name of the sde conn file directory from template
        # config file
        sdeConnectionFileDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_sdeConnFileDir)
        self.logger.debug("customScriptDir: {0}".format(sdeConnectionFileDir))
        curDir = os.path.dirname(__file__)
        self.logger.debug("curDir: {0}".format(curDir))
        # calcuting the name of the
        if self.const.AppConfigSdeConnFileExtension[0] <> '.':
            sdeConnFile = '{0}.{1}'.format(self.key,
                self.const.AppConfigSdeConnFileExtension)
        else:
            sdeConnFile = '{0}{1}'.format(self.key,
                self.const.AppConfigSdeConnFileExtension)

        #sdeDir = os.path.join(curDir, customScriptDir)
        sdeConnFileFullPath = os.path.join(sdeConnectionFileDir, sdeConnFile)
        # creating a list of the sde files that should exist
        dbEnvKeys = self.parser.items(self.const.ConfFileSection_destKeywords)
        sdeConnFiles = []
        for elems in dbEnvKeys:
            sdeConnFiles.append(elems[0] + \
                self.const.AppConfigSdeConnFileExtension)

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
        '''
        sdeConnectionFileDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_sdeConnFileDir)
        return sdeConnectionFileDir

    def getArcGISDesktopRootDirectory(self):
        '''
        :return: the path to where the arcgis desktop package is expected to
                 be installed.
        '''
        arcGISInstallDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_ARCGISDesktopRootDir)
        return arcGISInstallDir

    def getPythonRootDir(self):
        '''
        :return: the root directory to where python is expected to be
                 installed
        '''
        # defaultArcPythonPath
        pythonRootdir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_PythonRootDir)
        self.logger.debug("Got the python root directory from the config" +
                          " file: %s", pythonRootdir)
        return pythonRootdir

    def getFMERootDirTmplt(self, bit32=False):
        '''
        Retrieves the path to the the standardized location of an FME install.
        The year release of the install path is parameterized for string
        format() function.

        Example:
           C:\\Program File\\FME{0}

        where the {0} is where the install version would be inserted
        '''
        if bit32:
            paramName = self.const.ConfFileSection_global_FMERootDirTmplt32Bit
        else:
            paramName = self.const.ConfFileSection_global_FMERootDirTmplt
        fmeRootDirTmplt = self.parser.get(
            self.const.ConfFileSection_global,
            paramName)
        self.logger.debug("FME install path tmplt string from config" +
                          " file: %s", fmeRootDirTmplt)
        return fmeRootDirTmplt

    def getSourcePmpResources(self):
        '''
        :return: a 'cleaned' list of the pmp resources to search for
                 credentials can be more than one
        :rtype: list
        '''
        srcPmpResources = self.parser.get(
            self.const.ConfFileSection_pmpSrc,
            self.const.ConfFileSection_pmpSrc_resources)
        srcPmpResourcesList = srcPmpResources.split(',')
        # getting rid of leading/trailing spaces on each element in the list
        for cnter in range(0, len(srcPmpResourcesList)):
            srcPmpResourcesList[cnter] = srcPmpResourcesList[cnter].strip()
        return srcPmpResourcesList

    def getSqlServerDefaultPort(self):
        '''
        :return: default connection port for SQL server databases
        (from config file)
        '''
        return self.parser.get(self.const.sqlserverSection,
                               self.const.sqlserver_param_port)

    def getEmailSMTPServer(self):
        '''
        :return: the smtp server as defined in the config file
        '''
        return self.parser.get(self.const.emailerSection,
                               self.const.emailer_smtpServer)

    def getEmailSMTPPort(self):
        '''
        :return:  the smtp port that should be used for sending out emails
        '''
        return self.parser.get(self.const.emailerSection,
                               self.const.emailer_smtpPort)

    def getEmailFromAddress(self):
        '''
        :return: The 'from' email address that should be used for any email
                 communications associated with this fmw.
        '''
        return self.parser.get(self.const.emailerSection,
                               self.const.emailer_fromAddress)

    def getKirkUrl(self):
        '''
        returns the base url where the kirk api resides
        '''
        return self.parser.get(self.const.kirkSection, self.const.kirk_url)

    def getKirkToken(self):
        '''
        returns the api token required to access the kirk api
        '''
        return self.parser.get(self.const.kirkSection, self.const.kirk_token)

    def getSqlServerPMPIdentifier(self):
        '''
        :return: retrieves the string used to identify sql server databases in
                 pmp
        '''
        return self.parser.get(self.const.sqlserverSection,
                               self.const.sqlserver_param_pmpidentifier)

    def getTemplateRootDirectory(self):
        '''
        :return: the template root directory, ie the directory where the '
                 framework/template is installed.
        '''
        rootDir = self.parser.get(
            self.const.ConfFileSection_global,
            self.const.ConfFileSection_global_key_rootDir)
        return rootDir

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

    def isDestOther(self):
        '''
        checks to see if the destination database environment key is set to
        'other', ie a non bcgw database destination
        :return: is the destination database keyword set to 'other'
        :rtype: boolean
        '''
        retVal = False
        if self.key == self.const.ConfFileDestKey_Other:
            retVal = True
        return retVal

    def isDataBCFMEServerNode(self):
        '''
        identifies if the server that is running the script is a data
        bc fme server engine.
        '''
        nodeList = self.getDataBCFmeServerNodes()
        retVal = self.isNodeInList(nodeList)
        self.logger.debug("isDataBCFMEServerNode return val: %s", retVal)
        return retVal

    def isDataBCNode(self):
        '''
        :return: boolean indicating whether the code is being executed on a
                databc computer.
        '''
        nodeList = self.getDataBCNodes()
        retVal = self.isNodeInList(nodeList)
        self.logger.debug("isDataBCNode return val: %s", retVal)
        return retVal

    def isNodeInList(self, nodeList):
        '''
        Gets a list of possible node names and returns a boolean indicating
        if ' the current node is in that list.  The search through the list
        extracts any trailing whitespace for the comparison.  Also the
        comparison is done ignoring case.
        '''
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
        msg = "machine is in this nodelist {0} - {1}"
        msg = msg.format(nodeList, retVal)
        self.logger.debug(msg)
        return retVal

    def isFMEServerNode(self):
        '''
        reads the config file and gets the names of databc node names (computer
        names).  Compares that list against the name of the node that the code
        is currently being executed against and returns a boolean indicating
        if the current node is defined as a FME Server node.
        '''
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
        self.logger.debug("isFMEServerNode return val: %s", retVal)
        return retVal

    def readConfigFile(self):
        '''
        reads the config file and creates a parser object that is
        used by other methods of this class to extract information from the
        config file.
        '''
        if not self.confFile:
            thisFileDir = os.path.dirname(__file__)
            self.confFile = os.path.join(thisFileDir,
                                         self.const.AppConfigSecretsDir,
                                         self.const.AppConfigFileName)
            if not os.path.exists(self.confFile):
                msg = 'Can\'t find the application config file: {0} '
                msg = msg.format(self.confFile)
                self.logger.error(msg)
                raise ValueError(msg)
            self.logger.info(
                "config file path that has been calculated is %s",
                self.confFile)
        if not self.parser:
            self.parser = ConfigParser.ConfigParser()
            self.parser.read(self.confFile)
            self.logger.debug("sections in the config file are: %s",
                              self.parser.sections())

    def setDestinationDatabaseEnvKey(self, key):
        '''
        sets the property 'key' to the authoritative string that should be used
        for whatever key was sent to this method.
        '''
        self.logger.info(
            "Destination database environment keyword is set to: %s",
            key)
        self.validateKey(key)
        self.key = self.getDestinationDatabaseKey(key)

    def validateKey(self, key):
        '''
        :param key: a supplied destination database environment key.

        This method will get a possible key sent to it.  The method will
        the look for that key in a list of possible other keys and return a
        boolean indicating wether the key is a valid key or not.

        Valid implies it can be interpreted by the framework.
        '''

        key = self.getDestinationDatabaseKey(key)
        if not key:
            msg = 'You specified a destination key of {0}. The ' + \
                  'config file does not have any destination database ' + \
                  'parameters configured for that key.  Acceptable keys ' + \
                  'are: {1}'
            validKeys = self.getValidKeys()
            msg = msg.format(key, validKeys)
            self.logger.error(msg)
            raise ValueError(msg)


class PMPSourceAccountParser(object):

    '''
    PMP is more designed to store credentials managed by a specific
    organization. We use it for this purpose, but we also use it to store
    credentials for systems that we do not manage.

    For these types of credentials we have developed a standard to store this
    information.  It allows us to parse the account definitions and extract
    account names / hosts /database names etc.

    This class contains helper methods to make it easier to retrieve with these
    types of accounts.

    '''

    def __init__(self, accntName, sqlServerIdentifier):
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.const = TemplateConstants()
        self.sqlServerIdentifier = sqlServerIdentifier

        self.accntName = accntName
        self.accntList = self.accntName.split('@')
        if len(self.accntList) == 1:
            msg = 'unexpected account name format: {0}'.format(self.accntName)
            raise ValueError(msg)

    def getSchema(self):
        '''
        returns the schema / account name parameter from the current
        account list extracted from PMP.
        '''
        return self.accntList[0].strip()

#     def getInstance(self):
#         # deprecated
#         inst = self.getServiceName()
#         return self.accntList[1].strip()

    def getServiceName(self):
        '''
        This is the same as getInstance, however the name was changed
        to reflect the idea that we are moving towards using service
        names instead of instance names to refer to databases
        '''
        self.logger.debug("accntList obj: %s", self.accntList)
        sn = self.accntList[1].strip()
        snList = sn.split(':')
        return snList[0]

    def getServiceNameNoDomain(self):
        '''
        extracts from the PMP record the name of the service name, with the
        domain stripped off.  Example if the servicename was: lafleur.mtrl.ca
        this method would return just lafleur
        '''
        # noDomain = Util.removeDomain(self.accntList[1])
        # return noDomain
        # accntList = self.accntList[1].strip().split('.')
        # return accntList[0].strip()
        # The source format being used in pmp has changed to
        # retVal
        connectionEntryList = self.accntList[1].split(':')
        serviceName = connectionEntryList[0]
        serviceNameList = serviceName.split('.')
        serviceNameNoDomain = serviceNameList[0]
        return serviceNameNoDomain

    def getInstanceNoDomain(self):
        '''
        returns just the instance which is actually the
        servicename for this account object

        changed recently as we have transitioned from using the
        ETL-OPERATIONAL-DBLINKS resource to EXTERNAL-DB
        '''
        # use of instance is deprecated, passing call to
        # other method
        msg = 'Using a deprecated method: getInstanceNoDomain.  Switch to: ' + \
              'getServiceNameNoDomain'
        self.logger.warning(msg)
        return self.getServiceNameNoDomain()

    def isSqlServerRecord(self):
        '''
        :return: indicates if the current record is a sql server record.
        :rtype: boolean
        '''
        retVal = False
        sn = self.getServiceName()
        if sn == self.sqlServerIdentifier:
            retVal = True
        return retVal

    def getSqlServerHost(self, noDomain=False):
        '''
        if the current record is a sql server record, this method will parse it
        and return the host.
        '''
        return self.__getSqlSrvParam(2, noDomain)

    def getSqlServerDatabaseName(self, noDomain=False):
        '''
        extracts and returns the database name from the current record
        '''
        return self.__getSqlSrvParam(1, noDomain)

    def getSqlServerDatabasePort(self, noDomain=False):
        '''
        extracts and returns the database port for the current record
        '''
        return self.__getSqlSrvParam(3, noDomain)

    def __getSqlSrvParam(self, position, noDomain):
        '''
        Internal helper method to help extract information from pmp
        '''
        retVal = None
        if self.isSqlServerRecord():
            sn = self.accntList[1].strip()
            snList = sn.split(':')
            retVal = snList[position]
            if noDomain:
                retVal = Util.removeDomain(retVal)
        return retVal


class Util(object):
    '''
    a bunch of utility / static methods that are used by various aspects of the
    framework.
    '''

    @staticmethod
    def getComputerName():
        '''
        Gets the name of the computer that we are currently executing the
        code on
        '''
        computerName = platform.node()
        if computerName.count('.'):
            computerNameParsedList = computerName.split('.')
            computerName = computerNameParsedList[0]
        return computerName

    @staticmethod
    def removeDomain(inString):
        '''
        :param inString: an input string with a possible domain included..
        :return: the same string with the domain removed. Example if
        inString was lafleur.mtl.ca then this method would return simply
        'lafleur'.
        '''
        inString = inString.strip()
        stringList = inString.split('.')
        return stringList[0].strip()

    @staticmethod
    def calcLogFilePath(fmwDir, fmwName):
        '''
        :param fmwDir: the directory where the fmw that is being run is located
        :param fmwName: the fmw name

        This method will receive the full path to where the current
        fmw that is being processed is located, and the name of the fmw
        that is being processed.  It will then calculate:
          - the full path to the outputs director
          - the full path to the fmw directore (sub dir of outputs)
          - from these paths it will then calculate the relative directory
            from the fmw directory to the output log file and returns
            this value.

        example if the inputs are:
          fmwDir = 'C:\\somedir\\myFmws'
          fmwName = 'CopyData.fmw'

          Then:
           - outputs directory will be: C:\\somedir\\myFmws\\outputs
               (depending on what the value const.AppConfigOutputsDir
                is set to)
           - log dir path will be: C:\\somedir\\myFmws\\outputs\\CopyData
           - and the relative log directory will be:
             ./outputs/CopyData/CopyData.log
               (Which is the relative path for the fmw but not
                for the python code that is doing the calcuating)

         If returnPathList is set to True then the method will
         return a list with all these paths, the order will be:
           [0] relative path (./outputs/CopyData/CopyData.log)
           [1] full path to log file ( C:\\somedir\\myFmws\\outputs\\CopyData\\CopyData.log)  # @IgnorePep8
           [2] log dir ( C:\\somedir\\myFmws\\outputs\\CopyData )
           [3] outputs ( C:\\somedir\\myFmws\\outputs )

        '''
        const = TemplateConstants()
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
        '''
        :param module: the name of a python module
        :param classname: the name of a class in question.
        :return: boolean indicating whether the classname supplied exists in
                 the module supplied
        '''
        retVal = False
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj):
                if name == classname:
                    retVal = True
        return retVal

    @staticmethod
    def isEqualIgnoreDomain(param1, param2):
        '''
        :param param1: the first parameter with a domain
        :param param2: the second parameter with a domain

        Domains are stripped from both param1 and param2, then a case
        insensitive comparison is done between these two parameters.

        :return: boolean indicating whether the two parameters if domain is
                 removed from both of them.
        '''
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
        '''
        Generic method used to calculate the path to the enhanced logging
        file name.  The enhanced logging file is the one that is written to
        by code used to calculate the published parameters.

        The reason is the actual FME log does not exist when these
        parameters are being calculated. As a result if the published
        parameter code crashes we do not get any log messages to help
        diagnose what the problems are.
        '''
        # strip off the .fmw
        fmwFileNameNoSuffix = (os.path.splitext(fmwName))[0]
        enhancedLogKeyword = '_extra'
        logFileNameTmplt = '{0}{1}.{2}'
        logFileName = logFileNameTmplt.format(fmwFileNameNoSuffix,
                                              enhancedLogKeyword, 'log')
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
        # paramValue = None  # init return value

        # start by getting the parameter value
        if paramName not in fmwMacros:
            msg = 'Trying to retrieve the published parameter {0} however it' + \
                  ' is undefined in the FMW.  Current values include: {1}'
            raise KeyError(msg.format(paramName, fmwMacros.keys()))

        paramValue = fmwMacros[paramName]
        if not isinstance(paramValue, basestring):
            if paramName.upper() != \
              TemplateConstants.FMWParams_DestKey.upper():
                msg = 'The macro value for the key {0} is not a string ' + \
                      'type.  Its a {1}'
                raise ValueError(msg.format(paramName, type(paramValue)))
            else:
                # just leave the value as is for dest env key
                paramValue = fmwMacros[paramName]
        else:
            logger.debug(r'input param value/name: -%s/%s-', paramName,
                         paramValue)
            isParamNameRegex = re.compile(r'^\$\((.*?)\)$')
            if isParamNameRegex.match(paramValue):
                justParamName = (isParamNameRegex.search(paramValue)).group(1)
                logger.debug('detected parameter %s', paramValue)
                paramValue = Util.getParamValue(justParamName, fmwMacros)
                logger.info('Value extracted from linked parameter %s',
                            paramValue)
        return paramValue


class GetPublishedParams(object):
    '''
    originally all this logic was in the CalcParamsBase class.
    Moving simple parameter retrieval to this class,

    contains methods that will retrieve published parameters
    from the FMEMACRO dictionary.

    Includes checks that make sure required parameters exist.
    '''

    def __init__(self, fmeMacroVals):
        self.fmeMacroVals = fmeMacroVals
        self.const = TemplateConstants()

        self.logger = logging.getLogger(__name__)

    def existsMacroKey(self, macroKey):
        '''
        Used to determine if a macro key exists
        or not
        '''
        retVal = False
        if macroKey in self.fmeMacroVals:
            retVal = True
        return retVal

    def existsSrcPort(self, position):
        '''
        :param position: See method docstring for method
                         existsSourceOracleServiceName()
        :return: boolean value to indicate whether the fmw has a src port
                 defined it its published parameters
        :rtype: boolean
        '''
        retVal = True
        macroKey = self.const.FMWParams_SrcSSSchema
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        if not self.existsMacroKey(macroKey):
            retVal = False
        return retVal

    def existsSrcSSDatabaseName(self, position):
        '''
        :param position: See method docstring for method
                         existsSourceOracleServiceName()
        :return: boolean value to indicate whether the fmw has a src
                 database name defined it its published parameters
        :rtype: boolean
        '''
        retVal = True
        macroKey = self.const.FMWParams_SrcSSDbName
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        if not self.existsMacroKey(macroKey):
            retVal = False
        return retVal

    def existsSrcSDEDirectConnectClientString(self, position):
        '''
        :param position: See method docstring for method
                         existsSourceOracleServiceName()
        :return: boolean value to indicate whether the fmw has a src sde direct
                 connect client string defined it its published parameters
        :rtype: boolean
        '''
        retVal = True
        macroKey = self.const.FMWParams_SrcSDEDirectConnectClientStr
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        if not self.existsMacroKey(macroKey):
            retVal = False
        return retVal

    def existsDependentFMWSs(self):
        '''
        :param position: See method docstring for method
                         existsSourceOracleServiceName()
        :return: boolean value to indicate whether the fmw has any dependent
                 FMW's defined for it in the published parameters.
        :rtype: boolean
        '''
        retVal = True
        macroKey = self.const.FMWParams_Deps_fmwList
        if not self.existsMacroKey(macroKey):
            retVal = False
        else:
            # now need to get the contents as if there is nothing inside it
            # should return false.
            deps = self.getDependentFMWs()
            self.logger.debug("dependencies: %s", deps)
            if not deps:
                retVal = False
        return retVal

    def existsDestinationSchema(self, position):
        '''
        :param position: See method docstring for method
                         existsSourceOracleServiceName()
        :return: boolean value to indicate whether the fmw has a destination
                 oracle schema defined for it.
        :rtype: boolean
        '''
        retVal = True
        destSchemaKey = self.const.FMWParams_DestSchema
        destSchemaKey = self.getMacroKeyForPosition(destSchemaKey, position)
        if not self.existsMacroKey(destSchemaKey):
            retVal = False
        return retVal

    def existsSourceOracleSchema(self, position):
        '''
        :param position: See method docstring for method
                         existsSourceOracleServiceName()
        :return: boolean value to indicate whether the fmw has a source oracle
                 schema defined for it.
        :rtype: boolean
        '''
        retVal = True
        dataSchema = self.const.FMWParams_SrcSchema
        proxySchema = self.const.FMWParams_SrcProxySchema

        dataSchema = self.getMacroKeyForPosition(dataSchema, position)
        proxySchema = self.getMacroKeyForPosition(proxySchema, position)

        if not self.existsMacroKey(dataSchema) and \
           not self.existsMacroKey(proxySchema):
            retVal = False
        return retVal

    def existsSourceOracleProxySchema(self, position):
        '''
        :param position: See method docstring for method
                         existsSourceOracleServiceName()
        :return: boolean value to indicate whether the fmw has a source
                 oracle proxy schema defined for it.
        :rtype: boolean
        '''
        retVal = True
        proxySchema = self.const.FMWParams_SrcProxySchema
        proxySchema = self.getMacroKeyForPosition(proxySchema, position)
        if not self.existsMacroKey(proxySchema):
            retVal = False
        return retVal

    def existsSourceOracleServiceName(self, position):
        '''
        :param position: When more than one service name needs to be
                         specified you can append _# to the end of the
                         parameter name.  This option specifies what
                         numbered arguement to retrieve.
        :return: is a source oracle service name defined in the published
                 parameters for the fmw that is currently being run.
        '''
        retVal = True
        macroKey = self.const.FMWParams_SrcServiceName
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        if not self.existsMacroKey(macroKey):
            retVal = False
        return retVal

    def getDependentFMWs(self):
        '''
        :return: a list of any dependent fmw's, ie fmw's that should have run
                 before this fmw is run.
        '''
        macroKey = self.const.FMWParams_Deps_fmwList
        depFMWs = self.getFMEMacroValue(macroKey)
        # removing any whitespace in between elements
        # in list
        depFMWs = depFMWs.strip()
        depsList = depFMWs.split(',')
        for depCnt in range(0, len(depsList)):
            depsList[depCnt] = depsList[depCnt].strip()
        return depsList

    def getDestDatabaseEnvKey(self):
        '''
        :return: The destination database environment key
        '''
        macroKey = self.const.FMWParams_DestKey
        destDbEnvKey = self.getFMEMacroValue(macroKey)
        return destDbEnvKey

    def getDestinationSchema(self, position=None):
        '''
        :param position: optional arguement.  If you wanted to retrieve a
                         numbered schema, example: DEST_SCHEMA_9 then
                         specify position 9. if no position is specified
                         retrieves the value in the paramether DEST_SCHEMA.
        :return: The value for the destination schema as defined by the
                 published parameter described in the user docs (at the
                 time of writing this comment its DEST_SCHEMA)
        '''
        destSchemaKey = self.const.FMWParams_DestSchema
        destSchemaKey = self.getMacroKeyForPosition(destSchemaKey, position)
        destSchema = self.getFMEMacroValue(destSchemaKey)
        return destSchema

    def getDestinationFeature(self, position=None):
        '''
        :param position: in the event that there is more than one
                         DEST_FEATURE+# the position is used to indicate
                         which DEST_FEATURE_# it is that you want to
                         retrieve.

        if position is set to 2 then will try to retrieve the value for the
        published parameter DEST_FEATURE_2
        '''
        position2Use = 1
        if position:
            position2Use = position
        destFeatureKey = self.const.FMWParams_DestFeatPrefix
        if destFeatureKey[-1] == '_':
            destFeatureKey = destFeatureKey[0:-1]
        destFeatureKey = self.getMacroKeyForPosition(destFeatureKey,
                                                     position2Use)
        destFeature = self.getFMEMacroValue(destFeatureKey)
        return destFeature

    def getDestinationTables(self, includeSchemaPrefix=False):
        '''
        :param includeSchemaPrefix:  if set to true will append the
                                     destination schema onto the end of the
                                     destination feature. example instead
                                     of returning just "MYTABLE" setting
                                     this param to true would result in the
                                     method returning "SCHEMA.MYTABLE"
        :return: a list of all the destination features in the fmw

        Iterates through all the published parameters looking for any
        parameters that fit the parameter standard for a destination
        feature, currently defined as DEST_FEATURE_#.
        '''
        # again only captures the first feature class
#        destTable = None
        matchExpr = '^{0}.*$'
        destFeatures = []
        matchExpr = matchExpr.format(self.const.FMWParams_DestFeatPrefix)
        for macroKey in self.fmeMacroVals.keys():
            if re.match(matchExpr, macroKey, re.IGNORECASE):
                if includeSchemaPrefix:
                    destFeat = self.fmeMacroVals[macroKey]
                    # get number?  ie, DEST_FEATURE_1 would get 1
                    destFeaturePostion = \
                        macroKey[len(self.const.FMWParams_DestFeatPrefix):]
                    if not destFeaturePostion.isdigit():
                        msg = 'Trying to extract the number from the published ' + \
                              'parameter {0}.  Extracted the number {1} but it ' + \
                              'is not a number'
                        msg = msg.format(macroKey, destFeaturePostion)
                        raise ValueError(msg)
                    # now get the schema for that number
                    destFeaturePostion = int(destFeaturePostion)
                    defaultSchema = self.getDestinationSchema()
                    self.logger.debug("destination position is: {0}".format(
                        destFeaturePostion))
                    if self.existsDestinationSchema(destFeaturePostion):
                        positionalSchema = self.getDestinationSchema(
                            destFeaturePostion)
                        destFeat = '{0}.{1}'.format(positionalSchema, destFeat)
                    else:
                        if not defaultSchema:
                            msg = 'There does not appear to be a published ' + \
                                  'parameter that defines the destination ' + \
                                  'schema in this fmw.'
                            raise ValueError(msg)
                        destFeat = '{0}.{1}'.format(defaultSchema, destFeat)
                else:
                    destFeat = self.fmeMacroVals[macroKey]
                destFeatures.append(destFeat)
        return destFeatures

    def getFailedFeaturesFiles(self):
        '''
        does a search in the FME macro parameters for any parameters that
        match the pattern FAILED_FEATURE.* and returns the values
        associated with them
        '''
        failedFiles = []
        patternString = '^{0}.*$'.format(self.const.FMWParams_FailedFeatures)
        pattern = re.compile(patternString, re.IGNORECASE)
        for paramKey in self.fmeMacroVals:
            if pattern.match(paramKey):
                failedFiles.append(self.fmeMacroVals[paramKey])
        return failedFiles

    def getFMWDirectory(self):
        '''
        :return: the directory that the current fmw that is being run
                 resides in
        '''
        macroKey = self.const.FMWMacroKey_FMWDirectory
        fmwDir = self.getFMEMacroValue(macroKey)
        return fmwDir

    def getFMWFile(self):
        '''
        :return:  the name of the FMW file that is currently being run.
        '''
        macroKey = self.const.FMWMacroKey_FMWName
        fmwFile = self.getFMEMacroValue(macroKey)
        return fmwFile

    def getFMEMacroValue(self, paramNameRaw):
        '''
        When you link parameters together, the parameter ends up being equal
        to the name of the parameter it is linked to.  Example:

        PARAM1 is linked to PARAM2, then when you retrieve PARAM1 it will
        be equal to $(PARAM2).  This method will retrieve the actual
        value.

        Use this method to retrieve all macro values.
        '''
        logger = logging.getLogger(__name__)
        paramName = paramNameRaw.strip()

        # start by getting the parameter value
        if paramName not in self.fmeMacroVals:
            msg = 'Trying to retrieve the published parameter {0} however ' + \
                  'it is  undefined in the FMW.  Current values include: {1}'
            msg = msg.format(paramName, self.fmeMacroVals.keys())
            raise KeyError(msg)

        paramValue = self.fmeMacroVals[paramName]
        logger.debug('input param value/name: -%s/%s-', paramName, paramValue)
        if not isinstance(paramValue, basestring):
            msg = 'The macro value for the key {0} is not a string type. ' + \
                  'Its a {1} type. When a parameter value is not a string ' + \
                  'type assume it cannot be a linked parameter'
            msg = msg.format(paramName, type(paramValue))
            self.logger.warning(msg)
        else:
            # checking to see if the parameter is a linked variable
            isParamNameRegex = re.compile(r'^\$\((.*?)\)$')
            if isParamNameRegex.match(paramValue):
                justParamName = (isParamNameRegex.search(paramValue)).group(1)
                logger.debug('detected parameter %s', paramValue)
                paramValue = Util.getParamValue(justParamName,
                                                self.fmeMacroVals)
                logger.debug('Value extracted from linked parameter %s',
                             paramValue)
                logger.info('parameter (%s) is a linked parameter with an ' +
                            'ultimate value of %s', paramName, paramValue)
        return paramValue

    def getMacroKeyForPosition(self, macroKey, position=None):
        '''
        Some parameters defined by the DataBC FME framework can have
        multiple instances. For example if there are multiple source data
        sets used in a replication these can be defined by the parameters:
        SRC_FEATURE_1, SRC_FEATURE_2, SRC_FEATURE_3.

        Other values like SRC_ORA_SCHEMA do not have numeric values
        associated with them when there is only a single value, but do when
        there are multiple values.

        For example:
        SRC_ORA_SCHEMA - is the parameter when there is only one.

        When there are multple values you would define:
        SRC_ORA_SCHEMA_1 SRC_ORA_SCHEMA_2 SRC_ORA_SCHEMA3... etc.

        This method exists to facilitate the process of retrieving these
        values.  The method uses the variable "position" to retrieve the
        parameter for a  macroKey.

        macroKey - Either a macro key without any increment number, example
                   SRC_ORA_SCHEMA, or for variables that have incrementers
                   like SRC_FEATURE_1 use the first value always

        position - Identifies what numeric position to retrieve.  For example
                   if you specify a macroKey of SRC_FEATURE_1 and position
                   of 5 this method will return the value for the property
                   SRC_FEATURE_5

                   if you want the 3rd value for SRC_ORA_SCHEMA, send
                   SRC_ORA_SCHEMA and the position 3
        '''
        if not position:
            retVal = macroKey
        else:
            # if type(position) is not int:
            if not isinstance(position, int):
                msg = 'the arg passwordPosition you provided is {0} which has ' + \
                      'a type of {1}.  This arg must have a type of int.'
                msg = msg.format(position, type(position))
                self.logger.error(msg)
                raise ValueError(msg)

            # strip off any whitespace
            macroKey = macroKey.strip()
            # now split on the _ characters
            attributePieces = macroKey.split('_')
            numericPositionString = u'{0}'.format(position)
            # only interested in the last piece
            if attributePieces[-1].isdigit() or attributePieces[-1] == '':
                # then we replace this number with the position and rebuild
                attributePieces[-1] = numericPositionString
            elif attributePieces[-1] != '':
                attributePieces.append(numericPositionString)
            macroKey = '_'.join(attributePieces)
            retVal = macroKey
        return retVal

    def getSourceOracleProxySchema(self, position=None):
        '''
        :param position: see explaination for method getSourceOracleSchema() to
                         understand how position works
        :return: The source oracle proxy schema.

        Datasets for source FMW's can use either the same schema as the
        data to connect to the database or they can use a proxy schema.
        When a proxy schema is defined, the proxy refers to the schema used
        to establish the connection to the database, it also implies that
        the data is located in a different schema than the one that was
        used to connect to the database.
        '''
        macroKey = self.const.FMWParams_SrcProxySchema
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        # srcOraProxySchema = ''
        # if self.existsMacroKey(macroKey):
        srcOraProxySchema = self.getFMEMacroValue(macroKey)
        return srcOraProxySchema

    def getSrcSDEDirectConnectClientString(self, position):
        '''
        :param position: Getting tired of explaining how this works.  See
                         description for method getSourceOracleSchema()
        :return: the source SDE connection client string.
        '''
        macroKey = self.const.FMWParams_SrcSDEDirectConnectClientStr
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        srcDCConnStr = self.getFMEMacroValue(macroKey)
        return srcDCConnStr

    def getSourceOracleSchema(self, position=None):
        '''
        :param position: If there is more than one source schema then you can
                         specify which source schema it is you want to
                         retrieve using this parmameter.  Example if not
                         provided this method would return SRC_ORA_SCHEMA.
                         If this parameter is set to 3 then would search
                         for and return the value associtaed with the
                         parameter SRC_ORA_SCHEMA_3
        :return: the source oracle schema
        '''
        macroKey = self.const.FMWParams_SrcSchema
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        srcOraSchema = self.getFMEMacroValue(macroKey)
        return srcOraSchema

    def getSourceOracleServiceName(self, position=None):
        '''
        :param position: Some FMW's define more than one source database.
                         When this happens parameters used to define each
                         database connection recieve numbers.  This
                         parmeter can be used to indicate which numbered
                         parameter it is you are trying to retrieve.
                         Example: SRC_ORA_SERVICENAME is what this method
                         will return if no position is specified, if you
                         specified position 5 then this method would search
                         for and return the value associated with the
                         parameter SRC_ORA_SERVICENAME_5
        :return: the source oracle service name
        '''
        macroKey = self.const.FMWParams_SrcServiceName
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        srcOraServName = self.getFMEMacroValue(macroKey)
        return srcOraServName

    def getSrcHost(self, position=None, noDomain=False):
        '''
        if the fmw has the source host defined as a published parameter
        this method will return it.  If it does not exist then this
        method will return none
        '''
        macroKey = self.const.FMWParams_SrcHost
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        srcHost = self.getFMEMacroValue(macroKey)
        if noDomain:
            srcHost = Util.removeDomain(srcHost)
        return srcHost

    def getSrcPort(self, position=None):
        '''
        if the fmw has the source host defined as a published parameter
        this method will return it.  If it does not exist then this
        method will return none
        '''
        macroKey = self.const.FMWParams_SrcPort
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        srcPort = None
        if not self.existsMacroKey(macroKey):
            msg = "There is no: %s macro key defined"
            self.logger.warning(msg, macroKey)
        else:
            srcPort = self.getFMEMacroValue(macroKey)
        return srcPort

    def getSrcOraServiceName(self, position=None):
        '''
        if the fmw has the source service name defined as a published parameter
        this method will return it.  If it does not exist then this
        method will return none.
        '''
        macroKey = self.const.FMWParams_SrcServiceName
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        srcServName = self.getFMEMacroValue(macroKey)
        return srcServName

    def getSourceSqlServerConnectionSchema(self, position=None):
        '''
        If a proxy schema is defined then it will return that
        schema, otherwise it will return the same schema
        the data resides within
        '''
        macroKeyProxy = self.getMacroKeyForPosition(
            self.const.FMWParams_SrcProxySSSchema, position)
        macroKeyData = self.getMacroKeyForPosition(
            self.const.FMWParams_SrcSSSchema, position)

        if self.existsMacroKey(macroKeyProxy):
            macroKey = macroKeyProxy
        else:
            macroKey = macroKeyData

        msg = "retrieving data from the parameter %s"
        self.logger.debug(msg, macroKey)

        srcSqlServerProxySchema = self.getFMEMacroValue(macroKey)
        return srcSqlServerProxySchema

    def getSrcSqlServerDatabaseName(self, position=None, noDomain=False):
        '''
        :param position: allows you to specify numbered parameters.  See user
                         docs defined in the header for more info
        :param noDomain: if you want to search for a database name independent
                         of the schema.  Or in other words search for any match
                         even if the domain does not match, the specify
                         noDomain=True
        :return: The sql server database name as defined by the published
                 parameters
        '''
        macroKey = self.const.FMWParams_SrcSSDbName
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        self.logger.debug('schemaMacroKey: %s', macroKey)
        if self.existsSrcSSDatabaseName(position):
            srcSqlServerDbName = self.getFMEMacroValue(macroKey)
        if noDomain:
            srcSqlServerDbName = Util.removeDomain(srcSqlServerDbName)
        return srcSqlServerDbName

    def getSrcSqlServerProxySchema(self, position=None):
        '''
        :param position: published parameters can be numbered.
                         Example SRC_SCHEMA
        :return: the sql server proxy schema as defined in published
                 parameters
        '''
        macroKey = self.const.FMWParams_SrcProxySSSchema
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        proxySchema = self.getFMEMacroValue(macroKey)
        return proxySchema

    def getSrcSQLServerSchema(self, position=None):
        '''
        :param position: published parameters can be numbered.
                         Example SRC_SCHEMA
        :return: the sql server schema as defined in the fmw's published
                 parameters
        '''
        macroKey = self.const.FMWParams_SrcSSSchema
        macroKey = self.getMacroKeyForPosition(macroKey, position)
        srcSchema = self.getFMEMacroValue(macroKey)
        return srcSchema


class CalcParamsBase(GetPublishedParams):
    '''
    This method contains the base functionality which
    consists of retrieval of parameters that come from L
    the template config file.

    Password retrieval is defined in inherited classes.
    functionality differs if the script is being developed
    as oppose to if it is in production.

    Development mode assumes the following:
        - PMP is unavailable therefor passwords
          will be retrieved from a hardcoded json file
    '''

    def __init__(self, fmeMacroVals):

        GetPublishedParams.__init__(self, fmeMacroVals)
        self.plugin = None

        fmwDir = self.getFMWDirectory()
        fmwName = self.getFMWFile()
        self.destKey = self.getDestDatabaseEnvKey()

        ModuleLogConfig(fmwDir, fmwName, self.destKey)

        # ModuleLogConfig()
        self.logger = logging.getLogger(__name__)
        self.paramObj = TemplateConfigFileReader(self.destKey)
        self.debugMethodMessage = "method: {0}"

    def addPlugin(self, forceDevel=False):
        '''
        The framework needs to behave differently depending on where it is
        run. For example when run on a databc server it should retrieve
        passwords from our password system.  All functionality that is
        forked like this is implemented as a plugin.

        This method determines what environment the framework should be
        running in and configures either the development plugin or the
        databc plugin.

        Long term should probably define an abstract class for plugins.
        '''
        if forceDevel:
            self.logger.debug("Template is operating in Development mode.")
            self.plugin = CalcParamsDevelopment(self)
        elif self.paramObj.isDataBCNode():
            self.logger.debug("Template is operating in Production mode.")
            # TODO: Should implement an abstract class to ensure that all
            #       plugins impement the required methods.
            self.plugin = CalcParamsDataBC(self)
        else:
            self.logger.debug("Template is operating in Development mode.")
            self.plugin = CalcParamsDevelopment(self)

    def getDestinationHost(self):
        '''
        :return: the destination host, as defined in the config file
                 section that corresponds with the current destination
                 database environment key
        '''
        msg = self.debugMethodMessage.format("getDestinationHost")
        self.logger.debug(msg)
        host = self.paramObj.getDestinationHost()
        return host

    def getDestinationServiceName(self):
        '''
        :return: The destination service name, as extracted from the config
                 file defintion that corresponds with the destination
                 database environment key.
        '''
        msg = self.debugMethodMessage.format("getDestinationServiceName")
        self.logger.debug(msg)
        serviceName = self.paramObj.getDestinationServiceName()
        return serviceName

    def getDestDatabaseConnectionFilePath(self, position=None):
        '''
        returns the database connection file path, this is a
        relative path to the location of this script
        '''
        # this method is getting forked,
        # destination connection file name is calculated based on
        # the DEST_HOST, DEST_SERVICENAME which all come from the config file
        #  anyways
        #
        #  a) if dev mode this file is expected to be in the same dir as the
        #      fmw
        #  b) if prod mode then the path to the connection file is a hard
        #     coded value. when in prod mode the connection file will get
        #     created by a jenkins call in the event that it does not exist.
        if not self.plugin:
            self.addPlugin()
        msg = self.debugMethodMessage.format(
            "getDestDatabaseConnectionFilePath")
        self.logger.debug(msg)
        destConnFilePath = self.plugin.getDestDatabaseConnectionFilePath(
            position)
        return destConnFilePath

    def getDestEasyConnectString(self, position=None):
        '''
        :param position: see other methods for definition of what position does
        :return: the easy connect string that can be used to connect to a non
                 spatial oracle database writer.
        '''
        msg = self.debugMethodMessage.format("getDestEasyConnectString")
        self.logger.debug(msg)
        destHost = self.getDestinationHost()
        destServName = self.getDestinationServiceName()
        destPort = self.getDestinationOraclePort()
        easyConnectString = '{0}:{1}/{2}'.format(destHost, destPort,
                                                 destServName)
        self.logger.info("destination easy connect string: %s",
                         easyConnectString)
        return easyConnectString

    def getDestSDEDirectConnectString(self, position=None):
        '''
        :param position: see other methods for defintion of what position does
        :return: returns the destination SDE direct connect string.
        '''
        msg = self.debugMethodMessage.format("getDestSDEDirectConnectString")
        self.logger.debug(msg)
        if position:
            msg = 'You specified a position parameter for this method however ' + \
                  'it is not currently used by this method'
            self.logger.warning(msg)
        destHost = self.getDestinationHost()
        destServName = self.getDestinationServiceName()
        # don't need port currently
        oraClientString = self.paramObj.getOracleDirectConnectClientString()

        # now check to see if there is already a oracle client string
        # defined.
        dirConnectTemplate = 'sde:{0}:{1}/{2}'
        srcSDEDirectConnectString = dirConnectTemplate.\
            format(oraClientString, destHost, destServName)
        self.logger.info("destination direct connect string: %s",
                         srcSDEDirectConnectString)
        return srcSDEDirectConnectString

    def getDestinationPassword(self, position=None):
        '''
        Retrieves the destination password for the position specified.  If
        no position specified then looking for the password that
        corresponds with DEST_SCHEMA.  If position is set to say 3 then
        will get the password for the schema defined in DEST_SCHEMA_3

        :param position: option arg, Used to indicate which password you
                         want to retrieve.  if not specified retrieves the
                         password for the schema define in the published
                         parameter DEST_SCHEMA.

                         When specified would return the password for:
                         DEST_SCHEMA_<position>
        :return: The password for the destination schema
        '''
        if not self.plugin:
            self.addPlugin()

        msg = self.debugMethodMessage.format("getDestinationPassword")
        self.logger.debug(msg)
        if not self.existsDestinationSchema(position):
            msg = 'The destination schema parameter {0} ' + \
                  'is not defined.  Cannot retrieve the destination ' + \
                  'password until this parameter is defined'
            destSchemaKey = self.getMacroKeyForPosition(
                self.const.FMWParams_DestSchema, position)
            msg = msg.format(destSchemaKey)
            self.logger.error(msg)
            raise KeyError(msg)
        pswd = self.plugin.getDestinationPassword(position=position)
        return pswd

    def getDestinationSDEPort(self):
        '''
        :return: the destination sde port to be used for application connect
                 sde connections.

        This method is now deprecated as Databc no longer supports
        '''
        msg = self.debugMethodMessage.format("getDestinationSDEPort")
        self.logger.debug(msg)
        msg = 'FIX FMW!  APPLICATION CONNECTS ARE NO LONGER SUPPORTED'
        self.logger.warning(msg)
        port = self.paramObj.getDestinationSDEPort()
        return 'port:{0}'.format(port)

    def getDestinationOraclePort(self):
        '''
        :return: the destination oracle port as specified in the framework
                 config file
        '''
        msg = self.debugMethodMessage.format("getDestinationOraclePort")
        self.logger.debug(msg)
        port = self.paramObj.getDestinationOraclePort()
        return port

    def getDependencyMaxRetries(self):
        '''
        This parameter is used to when a FMW has a dependency.

        Dependencies work by filling in the published parameter
        DEPS_FMW with a list of the parent dependencies.  Tbe job
        with that parameter will not start executing until it has
        found all jobs defined in the parameter DEPS_FMW, OR until
        the maximum number or searches for dependencies has been
        exceeded.

        When searching FMW Server for a dependent job fails it retries
        the query later.  This parameter controls how many times
        the query searching for dependent jobs will be executed.

        If this parameter is not populated then the framework will
        use the default amount which is identified in the framework
        config file in the section [dependencies] under timewindow.
        '''
        msg = self.debugMethodMessage.format("getDependencyTimeWindow")
        self.logger.debug(msg)
        maxRetriesMacroKey = self.const.FMWParams_Deps_maxRetry
        if not self.existsMacroKey(maxRetriesMacroKey):
            maxRetries = self.paramObj.getDependencyMaxRetries()
        else:
            maxRetriesMacroKey = self.getMacroKeyForPosition(
                maxRetriesMacroKey)
            maxRetries = self.getFMEMacroValue(maxRetriesMacroKey)
        try:
            maxRetries = int(maxRetries)
        except ValueError:
            msg = 'The value for the parameter {0} is {1}. This value ' + \
                  'cannot be converted to a numeric type'
            msg = msg.format('max retries', maxRetries)
            self.logger.error(msg)
            raise ValueError(msg)

        return maxRetries

    def getDependencyTimeWindow(self):
        '''
        This parameter is used to when a FMW has a dependency.

        Dependencies work by filling in the published parameter
        DEPS_FMW with a list of the parent dependencies.  Tbe job
        with that parameter will not start executing until it has
        found all jobs defined in the paremter DEPS_FMW, OR until
        the maximum number or searches for dependencies has been
        exceeded.

        This method returns the time window parameter.  This parameter
        tells the query that is searching for completed jobs in fme
        server how far to look back in time.  This parameter is always
        reported in seconds.

        If this parameter is not populated then the framework will
        use the default amount which is identified in the framework
        config file in the section [dependencies] under timewindow.
        '''
        msg = self.debugMethodMessage.format("getDependencyTimeWindow")
        self.logger.debug(msg)
        depTimeWindowMacro = self.const.FMWParams_Deps_timeWindow
        if not self.existsMacroKey(depTimeWindowMacro):
            timeWindow = self.paramObj.getDependencyTimeWindow()
        else:
            timeWindowMacroKey = self.getMacroKeyForPosition(
                depTimeWindowMacro)
            timeWindow = self.getFMEMacroValue(timeWindowMacroKey)
        try:
            timeWindow = int(timeWindow)
        except ValueError:
            msg = 'The value for the parameter {0} is {1}. This value ' + \
                  'cannot be converted to a numeric type'
            msg = msg.format('time Window', timeWindow)
            self.logger.error(msg)
            raise ValueError(msg)
        return timeWindow

    def getDependencyWaitTime(self):
        '''
        This parameter is used to when a FMW has a dependency.

        Dependencies work by filling in the published parameter
        DEPS_FMW with a list of the parent dependencies.  Tbe job
        with that parameter will not start executing until it has
        found all jobs defined in the paremter DEPS_FMW, OR until
        the maximum number or searches for dependencies has been
        exceeded.

        This method returns the wait time parameter.  This parameter
        tells the query that is searching for completed jobs in fme
        server how long to wait in between unsucessful queries.
        This parameter is always reported in seconds.

        If this parameter is not populated then the framework will
        use the default amount which is identified in the framework
        config file in the section [dependencies] under timewindow.
        '''
        msg = self.debugMethodMessage.format("getDependencyTimeWindow")
        self.logger.debug(msg)
        depWaitTimeMacroKey = self.const.FMWParams_Deps_waitTime
        if not self.existsMacroKey(depWaitTimeMacroKey):
            waitTime = self.paramObj.getDependencyWaitTime()
        else:
            waitTimeMacroKey = self.getMacroKeyForPosition(depWaitTimeMacroKey)
            waitTime = self.getFMEMacroValue(waitTimeMacroKey)
        try:
            waitTime = int(waitTime)
        except ValueError:
            msg = 'The value for the parameter {0} is {1}. This value ' + \
                  'cannot be converted to a numeric type'
            msg = msg.format('wait time', waitTime)
            self.logger.error(msg)
            raise ValueError(msg)
        return waitTime

    def getFailedFeaturesFile(self, failedFeatsFileName=None, position=None):
        '''
        Sends back the FFS File associated with this job,
        '''
        if not self.plugin:
            self.addPlugin()
        msg = self.debugMethodMessage.format("getFailedFeaturesFile")
        self.logger.debug(msg)
        self.logger.debug("Calling plugin to get the failed features")
        failedFeatures = self.plugin.getFailedFeaturesFile(
            failedFeatsFileName, position)
        return failedFeatures

    def getFMWLogFileRelativePath(self, create=True):
        '''
        :return: Returns the path to the where the log file is to be located.

        The actual log file path is dependent on a number of parameters,
        including:
         - is the script being run in databc infrastructure
         - is the script using a dev env.

        '''
        fmwDir = self.getFMWDirectory()
        fmwFile = self.getFMWFile()
        logFileFullPath = Util.calcLogFilePath(fmwDir, fmwFile)
        # retVal = [relativePath,absFullPath, logDirFullPath, outDirFullPath]
        logFileDir = os.path.dirname(logFileFullPath)
        # outDir = pathList.pop()
        # fmwDir = pathList.pop()
        if not os.path.exists(logFileDir) and create:
            os.makedirs(logFileDir)
        # self.logger.debug("FME LOG FILE TEST STATEMNET")
        self.logger.info('log file path: %s', logFileFullPath)
        return logFileFullPath

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
        # making sure the schema is defined and sending error message if it
        # isn't

        if not self.plugin:
            self.addPlugin()

        msg = self.debugMethodMessage.format("getSourcePassword")
        self.logger.debug(msg)
        if not self.existsSourceOracleSchema(passwordPosition):
            dataSchema = self.const.FMWParams_SrcSchema
            proxySchema = self.const.FMWParams_SrcProxySchema

            dataSchemaPubParam = self.getMacroKeyForPosition(
                dataSchema, passwordPosition)
            proxySchemaPubParam = self.getMacroKeyForPosition(
                proxySchema, passwordPosition)

            msg = 'In order to retrieve the source password you must ' + \
                  'define the published parameters for either source data ' + \
                  'schema: {0}, or a proxy schema parameter: {1}'
            msg = msg.format(dataSchemaPubParam, proxySchemaPubParam)
            self.logger.error(msg)
            raise KeyError(msg)

        # making sure the service name exists and sending error if it does not
        if not self.existsSourceOracleServiceName(passwordPosition):
            serviceNamePubParam = self.const.FMWParams_SrcServiceName
            serviceNamePubParam = self.getMacroKeyForPosition(
                serviceNamePubParam, passwordPosition)
            msg = 'in order to retrieve the source password you must ' + \
                  'define the published parameter for source oracle ' + \
                  'service name ({0})'
            msg = msg.format(serviceNamePubParam)
            self.logger.error(msg)
            raise KeyError(msg)

        # RETRIEVING THE SOURCE SCHEMA AND SERIVE NAME.  ERROR WILL BE RAISE
        # IF THESE PARAMETER DO NOT EXIST
        srcOraSchema = self.getSourceOracleSchema(passwordPosition)
        srcOraServName = self.getSourceOracleServiceName(passwordPosition)
        msg = 'retrieving an oracle password for the schema: {0} ' + \
              'and servicename {1}'
        msg = msg.format(srcOraSchema, srcOraServName)
        self.logger.info(msg)
        pswd = self.plugin.getSourcePassword(passwordPosition)
        return pswd

    def getSourcePasswordHeuristic(self, position=None):
        '''
        Searches through the various accounts that exist in pmp for the
        source and looks for matches for the source database, and username.
        If no exact matches are found domain information will get ignored
        when looking at the source database service names and host.
        '''
        if not self.plugin:
            self.addPlugin()
        msg = self.debugMethodMessage.format("getSourcePasswordHeuristic")
        self.logger.debug(msg)
        pswd = self.plugin.getSourcePasswordHeuristic(position)
        return pswd

    def getSrcDatabaseConnectionFilePath(self, position=None):
        '''
        returns the database connection file path, this is a
        relative path to the location of this script
        '''
        if not self.plugin:
            self.addPlugin()
        msg = self.debugMethodMessage.format(
            "getSrcDatabaseConnectionFilePath")
        self.logger.debug(msg)
        srcConnFilePath = self.plugin.getSrcDatabaseConnectionFilePath(
            position)
        return srcConnFilePath

    def getSrcEasyConnectString(self, position=None):
        '''
        using the parameters SRC_HOST and SRC_SERVICENAME assembles an easy
        connect string
        '''
        msg = self.debugMethodMessage.format("getSrcEasyConnectString")
        self.logger.debug(msg)

        srcHost = self.getSrcHost(position)
        srcServiceName = self.getSrcOraServiceName(position)
        srcPort = self.getSrcPort(position)

        if not srcPort:
            srcPort = self.paramObj.getDefaultOraclePort()

        # fme easy connect string example:
        #   servername.bcgov:1521/database.domain.GOV.BC.CA
        easyConnectString = '{0}:{1}/{2}'.format(srcHost, srcPort,
                                                 srcServiceName)
        return easyConnectString

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
        msg = self.debugMethodMessage.format("getSrcSDEDirectConnectString")
        self.logger.debug(msg)

        srcHost = self.getSrcHost(position)
        srcServiceName = self.getSrcOraServiceName(position)
        if self.existsSrcSDEDirectConnectClientString(position):
            oraClientString = self.getSrcSDEDirectConnectClientString(position)
        else:
            oraClientString = \
                self.paramObj.getOracleDirectConnectClientString()

        # now construct the direct connect string
        # - sde:oracle11g:host/service_name
        dirConnectTemplate = 'sde:{0}:{1}/{2}'
        srcSDEDirectConnectString = dirConnectTemplate.\
            format(oraClientString, srcHost, srcServiceName)
        self.logger.info("source direct connect string: %s",
                         srcSDEDirectConnectString)
        return srcSDEDirectConnectString

    def getSrcSqlServerPassword(self, position=None):
        '''
        :param position: see other methods for defintion of what position does
        :return: The password for the sql server source dataset.
        '''
        if not self.plugin:
            self.addPlugin()
        msg = self.debugMethodMessage.format("getSourceSqlServerPassword")
        self.logger.debug(msg)

        # test to see if a proxy schema exists.  If it does then use the
        # proxy schema to retrieve the password
        schema = self.getSourceSqlServerConnectionSchema(position)
        msg = "schema being used to retrieve sql server  password: %s"
        self.logger.info(msg, schema)

        msg = 'retrieving the sql server password for the schema: {0}, ' + \
              'dbname: {1}, host: {2}'
        ssDbName = self.getSrcSqlServerDatabaseName(position)
        ssHost = self.getSrcHost(position)
        msg = msg.format(schema, ssDbName, ssHost)
        self.logger.info(msg)
        pswd = self.plugin.getSourceSqlServerPassword(position)
        return pswd

    def getSrcSQLServerJDBCConnectString(self, position=None):
        '''
        :param position: usually set to none, if an fmw defines more than one
                         reader then the positon parameters are used to specify
                         which parameters should be tied to which reader.

                         Example if you had two readers you would have two
                         host definitions, SRC_HOST_1 and SRC_HOST_2.  When you
                         specify positon as None it will default to 1.  To
                         refer to the second reader parameters you would set
                         the positon to be 2.

        using the parameters:
            -
            -

        Will construct and return a jdbc string with the following form:
        jdbc:sqlserver://SERVER.COM:1433;databaseName=DATABASE_NAME

        '''
        host = self.getSrcHost(position)
        port = self.getSrcPort(position)
        if not port:
            port = self.paramObj.getSqlServerDefaultPort()
        SQLServerDBName = self.getSrcSqlServerDatabaseName(position)

        tmpltString = 'jdbc:sqlserver://{0}:{1};databaseName={2}'
        jdbcConnStr = tmpltString.format(host, port, SQLServerDBName)
        return jdbcConnStr

    def getSrcSQLServerConnectString(self, position=None):
        '''
        Gets the:
          - host
          - database name
          - port (when defined)

        uses these parameters to construct a connection string like

        host\database,port
        '''
        host = self.getSrcHost(position)
        port = self.getSrcPort(position)
        if not port:
            port = self.paramObj.getSqlServerDefaultPort()
            # get the default port from the config file sqlserver/defaultport
            # TODO: get the port from the config file parameter
        # SQLServerDBName = self.getSrcSqlServerDatabaseName(position)
        if port:
            retStr = u'{0},{1}'.format(host, port)
        else:
            retStr = host
        return retStr

    def getSrcSQLServerSDEDirectConnectString(self, position=None):
        '''
        :return: source orace sql server sde connection string.

        This string is assembled by glueing various other source published
        parameters
        '''
        host = self.getSrcHost(position)
        port = self.getSrcPort(position)
        dbName = None
        if self.existsSrcSSDatabaseName(position):
            dbName = self.getSrcSqlServerDatabaseName(position)
        SSClientString = self.paramObj.getSSDirectConnectClientString()
        if not SSClientString:
            msg = u'unable to retrieve the Sql Server Client string from the framework ' + \
                  u'config file.  Add the parameter: {0} to the global section in the ' + \
                  u'config file.'
            msg = msg.format(
                self.const.ConfFileSection_global_directConnectClientString)
            self.logger.error(msg)
            raise ValueError(msg)
        if not host:
            msg = u'To define a sql server direct connect string you must define ' + \
                  u'the source host in a published parameter called: {0}.  This parameter ' + \
                  u'is not currently defined'
            msg = msg.format(self.const.FMWParams_SrcHost)
            self.logger.error(msg)
            raise ValueError(msg.format(self.const.FMWParams_SrcHost))
        if port and dbName:
            retStr = r'sde:{0}:{1}\{2},{3}'.format(SSClientString, host,
                                                   dbName, port)
        elif dbName:
            retStr = r'sde:{0}:{1}\{2}'.format(SSClientString, host, dbName)
        else:
            retStr = u'sde:{0}:{1}'.format(SSClientString, host)
        return retStr

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
        msg = self.debugMethodMessage.format("isSourceBCGW")
        self.logger.debug(msg)
        print 'isSourceBCGW'
        msg = 'retrieving the source oracle service name parameter for ' + \
              'position {0}'
        msg = msg.format(position)
        self.logger.info(msg)
        sourceOraServName = self.getSourceOracleServiceName(position)
        self.logger.debug("Oracle source service name: %s position %s",
                          sourceOraServName, position)

        retVal = False
        destKeys = self.paramObj.parser.items(
            self.const.ConfFileSection_destKeywords)
        for destKeyItems in destKeys:
            destKey = destKeyItems[0]
            self.logger.debug("dest key: %s", destKey)
            servName = self.paramObj.parser.get(
                destKey, self.const.ConfFileSection_serviceNameKey)
            servNameAliases = self.paramObj.parser.get(
                destKey, self.const.ConfFileSection_instanceAliasesKey)
            instances = servNameAliases.split(',')
            instances.append(servName)
            for curInstName in instances:
                self.logger.debug("comparing %s and %s", curInstName,
                                  sourceOraServName)
                if Util.isEqualIgnoreDomain(curInstName, sourceOraServName):
                    retVal = True
                    return retVal
        return retVal

    def getSourceDestDBKey(self, position=None):
        '''
        if the source data set has been determined to be a BCGW source,
        then the passwords need to be retrieved from a different pmp
        resource.  By default things are set up to retrieve passwords from
        the external database resource.  This resource does not include
        bcgw passwords.

        When source is bcgw need to use the same resources as are used for
        the destination.

        This method will analyze the source database parameters and return a
        dest db env key that corresponds with those parameters.
        '''
        msg = self.debugMethodMessage.format("getSourceDestDBKey")
        self.logger.debug(msg)
        msg = 'retrieving the source oracle service name parameter for ' + \
              'position {0}'
        msg = msg.format(position)
        self.logger.info(msg)
        sourceOraServName = self.getSourceOracleServiceName(position)
        self.logger.debug("Oracle source service name: %s position %s",
                          sourceOraServName, position)

        retVal = None
        destKeys = self.paramObj.parser.items(
            self.const.ConfFileSection_destKeywords)
        for destKeyItems in destKeys:
            destKey = destKeyItems[0]
            # print 'destKey', destKey
            servName = self.paramObj.parser.get(
                destKey, self.const.ConfFileSection_serviceNameKey)
            servNameAliases = self.paramObj.parser.get(
                destKey, self.const.ConfFileSection_instanceAliasesKey)
            instances = servNameAliases.split(',')
            instances.append(servName)
            for curInstName in instances:
                self.logger.debug("comparing %s and %s", curInstName,
                                  sourceOraServName)
                if Util.isEqualIgnoreDomain(curInstName, sourceOraServName):
                    retVal = destKey
                    msg = "found the BCGW environment ((0)) for the source " + \
                          "database {1}"
                    msg = msg.format(destKey, sourceOraServName)
                    self.logger.info(msg)
                    break
            if retVal:
                break
        if not retVal:
            msg = 'Unable to find a database environment key for the ' + \
                  'source service name: {0}'
            msg = msg.format(sourceOraServName)
            self.logger.error(msg)
        return retVal


class CalcParamsDevelopment(object):

    '''
    Some framework functionality is forked depending on what environment
    the fmw is running in.  This method defines the behaviour for some
    methods when the framework is run in production mode.
    '''

    def __init__(self, parent):
        self.parent = parent
        self.const = self.parent.const
        self.fmeMacroVals = self.parent.fmeMacroVals
        self.paramObj = self.parent.paramObj

        fmwDir = self.parent.getFMWDirectory()
        fmwName = self.parent.getFMWFile()
        destKey = self.parent.getDestDatabaseEnvKey()

        ModuleLogConfig(fmwDir, fmwName, destKey)

        # ModuleLogConfig()
        self.logger = logging.getLogger(__name__)

        # This is the base / example db creds file.
        self.credsFileFullPath = self.getDbCredsFile()
        self.logger.debug("Credentials file being used is: %s",
                          self.credsFileFullPath)

        if not os.path.exists(self.credsFileFullPath):
            # The creds file doesn't exist, so raise exception
            # TODO: include the framework gogs url in this error message.
            msg = 'Script is running in development mode.  In development mode ' + \
                   'passwords are retrieved from a json file in same directory ' + \
                   'as the fmw.  When searching for the file {0} no file was found ' + \
                   'create this .json file.  Example of the syntax is available ' + \
                   'here: {1}'
            msg = msg.format(self.credsFileFullPath,
                             self.const.svn_DevelopmentJSONFile_Url)
            self.logger.error(msg)
            raise ValueError(msg)
        self.logger.debug('using the creds file %s', self.credsFileFullPath)
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
        exampleCredsFile = \
            self.paramObj.getDevelopmentModeCredentialsFileName()

        # calculate the expected location of the creds file, ie the creds file
        # name in the current fmw path.
        fmwPath = self.parent.getFMWDirectory()
        self.logger.debug("fmwPath: %s", fmwPath)
        credsFileFMWPath = os.path.join(fmwPath, exampleCredsFile)
        credsFileFMWPath = os.path.realpath(credsFileFMWPath)
        self.logger.info("using the credentials file: %s", credsFileFMWPath)
        if not os.path.exists(credsFileFMWPath):
            # calculate the full path to the example dbcreds.json file.
            templateRootDir = self.paramObj.getTemplateRootDirectory()
            confDirName = self.paramObj.getConfigDirName()
            exampleCredsFilePath = os.path.join(templateRootDir,
                                                confDirName, exampleCredsFile)
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

    def getDestinationPassword(self, position=None):
        '''
        :return: the destination password.  This is the password that aligns
                 with the destination database environment.
        '''
        retVal = None
        self.logger.debug("getting password in development mode")
        destSchema = self.parent.getDestinationSchema(position)
        destServiceName = self.parent.getDestinationServiceName()

        self.logger.debug("destSchema: %s", destSchema)
        self.logger.debug("destServiceName: %s", destServiceName)

        msg = 'getting password for the schema ({0}) / servicename ({1})'
        msg = msg.format(destSchema, destServiceName)
        self.logger.info(msg)
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_DestCreds]:  # @IgnorePep8
            self.logger.debug("dbParams: %s", dbParams)
            cnst = self.const
            dbUser = \
                dbParams[cnst.DevelopmentDatabaseCredentialsFile_dbUser]
            dbServName = \
                dbParams[cnst.DevelopmentDatabaseCredentialsFile_dbServName]
            dbPass = dbParams[cnst.DevelopmentDatabaseCredentialsFile_dbPswd]

            if dbUser is None or dbServName is None or dbPass is None:
                msg = "dbuser {0}, dbServName {1}, dbPass {2}.  These values: " + \
                      "are extracted from the file {3}.  The file has not " + \
                      "been properly filled out as one or more of the values " + \
                      "has a 'None' Type"
                ValueError(msg.format(dbUser, dbServName, "*"*len(dbPass),
                                      self.credsFileFullPath))

            msg = 'dbuser from credentials file: {0}, dbInstance {1}'
            msg = msg.format(dbUser.lower().strip(),
                             dbServName.lower().strip())
            self.logger.debug(msg)

            if dbServName.lower().strip() == destServiceName.lower().strip() and \
               dbUser.lower().strip() == destSchema.lower().strip():
                msg = "Found password in creds file for user ({0}) " + \
                      "service name ({1})"
                msg = msg.format((dbUser.lower()).strip(),
                                 (dbServName.lower()).strip())
                self.logger.info(msg)
                retVal = dbPass
                break

        if not retVal:
            msg = 'DevMod: Was unable to find a password in the credential file {0} for ' + \
                  'the destSchema: {1} and the instance {2}'
            msg = msg.format(self.credsFileFullPath, destSchema,
                             destServiceName)
            self.logger.error(msg)
            raise ValueError(msg)

        return retVal

    def getSourceSqlServerPassword(self, position):
        '''

        :param position: Sometimes there are more than one source datasets.
                         When this is the case the published parameters get
                         numbered with _#.  When position is specified it
                         will retrieve the number that corresponds with the
                         given position.
        :return: the source sql server password.  Sql server passwords use
                 a different methodology to extract them from the password
                 system, thus a specific method for SQL server passwords
                 vs. oracle
        '''
        self.logger.debug("getSourceSqlServerPassword")
        retVal = None

        schema = self.parent.getSourceSqlServerConnectionSchema()

        # schema = self.parent.getSrcSQLServerSchema(position)
        ssDbName = self.parent.getSrcSqlServerDatabaseName(position)
        ssHost = self.parent.getSrcHost(position)

        msg = "Retrieving the SQL Server password for schema: {0}, database" + \
              " name: {1}, host: {2}"
        msg = msg.format(schema, ssDbName, ssHost)
        self.logger.info(msg)

        if not schema:
            msg = 'Cannot retrieve the password without first defining the ' + \
                  'source sqlserver schema in the parameter {0}'
            ssSchemaMacroKey = self.parent.getMacroValueUsingPosition(
                self.const.FMWParams_SrcSSSchema, position)
            msg = msg.format(ssSchemaMacroKey)
            self.logger.error(msg)
            raise ValueError(msg)
        if not ssDbName:
            msg = 'Cannot retrieve the password without first defining the ' + \
                  'source sqlserver database name in the parameter {0}'
            ssDbNameMacroKey = self.parent.getMacroValueUsingPosition(
                self.const.FMWParams_SrcSSDbName, position)
            msg = msg.format(ssDbNameMacroKey)
            self.logger.error(msg)
            raise ValueError(msg)
        if not ssHost:
            msg = 'Cannot retrieve the password without first defining the ' + \
                  'source database host name in the parameter {0}'
            ssHostMacroKey = self.parent.getMacroValueUsingPosition(
                self.const.FMWParams_SrcHost, position)
            msg = msg.format(ssHostMacroKey)
            self.logger.error(msg)
            raise ValueError(msg)

        # -----------------
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_SourceCreds]:  # @IgnorePep8
            if self.const.DevelopmentDatabaseCredentialsFile_SSDbName in dbParams and \
               self.const.DevelopmentDatabaseCredentialsFile_SSDbHost in dbParams:  # @IgnorePep8
                self.logger.debug("dbParams: %s", dbParams)
                dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]  # @IgnorePep8
                dbHost = dbParams[self.const.DevelopmentDatabaseCredentialsFile_SSDbHost]  # @IgnorePep8
                dbName = dbParams[self.const.DevelopmentDatabaseCredentialsFile_SSDbName]  # @IgnorePep8
                dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]  # @IgnorePep8
                if dbName.lower().strip() == ssDbName.lower().strip() and \
                    dbHost.lower().strip() == ssHost.lower().strip() and \
                        dbUser.lower().strip() == schema.lower().strip():
                    retVal = dbPass
                    break
        if not retVal:
            retVal = self.getSqlServPasswordHeuristic(position)
            self.logger.debug("heuristic password search found the " +
                              "password %s", retVal)
        if not retVal:
            msg = 'Running in DevMod.  This means that the template is attempting ' + \
                  'to retrieve the password from the json credential file {0} Was ' + \
                  'unable to retrieve the password for the srcSchema: {1} and the ' + \
                  'source sql server database name: {2} in the section {3}'
            msg = msg.format(self.credsFileFullPath, schema, ssDbName,
                             self.const.DevelopmentDatabaseCredentialsFile_SourceCreds)  # @IgnorePep8
            raise ValueError(msg)
        return retVal

    def getSourcePassword(self, position=None):
        '''
        This method will search PMP repository for the password that aligns
        with the parameters:
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
        # No checking for parameter existence goes here as that should
        # have already been dealt with in the class that called
        # this method.

        # This code is only to help with error messages relating to
        # the src service name and src schema not being defined.
        if self.parent.existsMacroKey(self.const.FMWParams_SrcProxySchema):
            msg = "getting the oracle user from the parameter {0}".format(
                self.const.FMWParams_SrcProxySchema)
            self.logger.debug(msg)
            srcOraSchema = self.parent.getSourceOracleProxySchema(position)
        else:
            msg = "getting the oracle user from the parameter %s", \
                  self.const.FMWParams_SrcSchema
            self.logger.debug(msg)
            srcOraSchema = self.parent.getSourceOracleSchema(position)

        srcOraServName = self.parent.getSourceOracleServiceName(position)

        msg = 'Getting source password for user ({0}) and service name ({1})'
        msg = msg.format(srcOraSchema, srcOraServName)
        self.logger.info(msg)

        retVal = None
        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_SourceCreds]:  # @IgnorePep8
            self.logger.debug("dbParams: %s", dbParams)
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]  # @IgnorePep8
            dbServName = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbServName]  # @IgnorePep8
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]  # @IgnorePep8
            if dbServName.lower().strip() == srcOraServName.lower().strip() and \
               dbUser.lower().strip() == srcOraSchema.lower().strip():
                retVal = dbPass
                break
        if not retVal:
            retVal = self.getSourcePasswordHeuristic(position)
            self.logger.debug("heuristic password search found the " +
                              "password %s", retVal)
        if not retVal:
            msg = 'Running in DevMod.  This means that the template is ' + \
                  'attempting to retrieve the password from the json ' + \
                  'credential file {4} Was unable to retrieve the password ' + \
                  'for the srcOraSchema: {1} and the source service name ' + \
                  '{2} in the section {3}'
            msg = msg.format(self.credsFileFullPath, srcOraSchema,
                             srcOraServName,
                             self.const.DevelopmentDatabaseCredentialsFile_SourceCreds,  # @IgnorePep8
                             self.credsFileFullPath)
            raise ValueError(msg)
        return retVal

    def getSqlServPasswordHeuristic(self, position=None):
        '''
        heuristic search for a password in the dbcreds file that
        aligns with the password in the fmw

        nothing fancy here at the moment.  Only searches for server and
        host without the domain suffix.
        '''
        retVal = None

        # These are the params that come from macros in the FMW
        schema = self.parent.getSourceSqlServerConnectionSchema(position)
        ssDbName = self.parent.getSrcSqlServerDatabaseName(position)
        ssDbName = Util.removeDomain(ssDbName)
        ssHost = self.parent.getSrcHost(position)
        ssHost = Util.removeDomain(ssHost)

        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_SourceCreds]:  # @IgnorePep8
            self.logger.debug("dbParmas %s", dbParams)
            if self.const.DevelopmentDatabaseCredentialsFile_SSDbName in dbParams and \
               self.const.DevelopmentDatabaseCredentialsFile_SSDbHost in dbParams:  # @IgnorePep8

                # the params extracted from the dbcreds file for the current
                # iteration of the loop.
                self.logger.debug("dbParams: %s", dbParams)
                dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]  # @IgnorePep8
                dbHost = dbParams[self.const.DevelopmentDatabaseCredentialsFile_SSDbHost]  # @IgnorePep8
                dbHost = Util.removeDomain(dbHost)
                dbName = dbParams[self.const.DevelopmentDatabaseCredentialsFile_SSDbName]  # @IgnorePep8
                dbName = Util.removeDomain(dbName)
                dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]  # @IgnorePep8

                self.logger.debug('sql server db name 1: %s, sql server db ' +
                                  'name 2: %s',
                                  ssDbName.lower().strip(),
                                  dbName.lower().strip())
                self.logger.debug("schema1 %s schema2 %s",
                                  dbUser.lower().strip(),
                                  schema.lower().strip())

                if dbName.lower().strip() == ssDbName.lower().strip() and \
                   dbHost.lower().strip() == ssHost.lower().strip() and \
                   dbUser.lower().strip() == schema.lower().strip():
                    retVal = dbPass
                    msg = 'Found a password entry for host: {0},  db name: {2}' + \
                          ', schema {1}'
                    msg = msg.format(ssHost, schema, ssHost)
                    self.logger.debug(msg)
                    break
        if not retVal:
            msg = 'Running in DevMod.  This means that the template is ' + \
                  'attempting to retrieve the password from the json ' + \
                  'credential file {0} Was unable to retrieve the password ' + \
                  'for a SQL server database entry in this file with the ' + \
                  'values the username: {1}, the source host {2}, and source ' + \
                  'dbname {3} in the section {3}'
            msg = msg.format(self.credsFileFullPath, schema, ssHost, ssDbName,
                             self.const.DevelopmentDatabaseCredentialsFile_SourceCreds)  # @IgnorePep8
            raise ValueError(msg)
        return retVal

    def getSourcePasswordHeuristic(self, position=None):
        '''
        For now just ignores the domain when searching for the password
        '''
        retVal = None

        if not self.parent.existsSourceOracleSchema(position):
            macroKeyProxy = self.const.FMWParams_SrcProxySchema
            macroKeyProxy = self.parent.getMacroKeyForPosition(
                macroKeyProxy, position)
            macroKeyData = self.const.FMWParams_SrcSchema
            macroKeyData = self.parent.getMacroKeyForPosition(
                macroKeyData, position)
            msg = 'The source oracle schema must be defined in one of ' + \
                  'these two parameters to be able to retrieve a source ' + \
                  'password, {0} or {1}'
            msg = msg.format(macroKeyProxy, macroKeyData)

        if self.parent.existsSourceOracleProxySchema(position):
            srcSchema = self.parent.getSourceOracleProxySchema(position)
        else:
            srcSchema = self.parent.getSourceOracleSchema(position)

        srcServiceName = self.parent.getSourceOracleServiceName(position)
        srcServiceName = Util.removeDomain(srcServiceName)

        for dbParams in self.data[self.const.DevelopmentDatabaseCredentialsFile_SourceCreds]:  # @IgnorePep8
            self.logger.debug("dbParmas %s", dbParams)
            dbUser = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbUser]  # @IgnorePep8
            dbServName = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbServName]  # @IgnorePep8
            dbServName = Util.removeDomain(dbServName)

            # dbInstLst = dbInst.split('.')
            # dbInst = dbInstLst[0]
            dbPass = dbParams[self.const.DevelopmentDatabaseCredentialsFile_dbPswd]  # @IgnorePep8
            self.logger.debug('inst1 %s, inst2 %s',
                              dbServName.lower().strip(),
                              srcServiceName.lower().strip())
            self.logger.debug("schema1 %s schema2 %s",
                              dbUser.lower().strip(),
                              srcSchema.lower().strip())
            if dbServName.lower().strip() == srcServiceName.lower().strip() and \
               dbUser.lower().strip() == srcSchema.lower().strip():
                retVal = dbPass
                msg = 'found the value for service name %s, schema %s', \
                      srcServiceName, srcSchema
                self.logger.debug(msg)
                break
        if not retVal:
            msg = 'Running in DevMod.  This means that the template is ' + \
                  'attempting to retrieve the password from the json ' + \
                  'credential file {4} Was unable to retrieve the password ' + \
                  'for the srcSchema: {1} and the source service name {2} ' + \
                  'in the section {3}'
            msg = msg.format(self.credsFileFullPath, srcSchema, srcServiceName,
                             self.const.DevelopmentDatabaseCredentialsFile_SourceCreds,  # @IgnorePep8
                             self.credsFileFullPath)
            raise ValueError(msg)
        return retVal

    def getFailedFeaturesFile(self, failedFeatsFileName=None, postion=None):
        '''
        When run in development mode will look for an
        ./outputs/failed directory, if it doesn't exist
        it will get created.

        The csv file will be failedFeatures.csv
        '''
        self.logger.debug("failed features in dev mode")
        fmwDir = self.parent.getFMWDirectory()
        # fmwFile = self.fmeMacroVals[self.const.FMWMacroKey_FMWName]
        # params to get from config file
        # outputsbasedir
        # failedFeaturesFile
        outputsDir = self.paramObj.getOutputsDirectory()
        failedFeatsDir = self.paramObj.getFailedFeaturesDir()
        destSchema = self.parent.getDestinationSchema(postion)
        destFeature = self.parent.getDestinationFeature(postion)
        # ("^Y^m^d^H^M^S")_failed_features.ffs
        now = datetime.datetime.now()
        nowStr = now.strftime('%Y%m%d%H%M%S')
        failedFeatsFile = '{0}_{1}_{2}_failedFeatures.ffs'.format(
            destSchema, destFeature, nowStr)

        # changing where the failed features file comes from, instead of from
        # the framework config file it is calculated by appending the following
        # parameters.
        # $(DEST_SCHEMA)_$(DEST_FEATURE_1)_JOB_$(FME_JOB_ID)_failed_features.ffs
        # failedFeatsFile = self.paramObj.getFailedFeaturesFile()
        self.logger.debug("failedFeatsFile: %s", failedFeatsFile)
        self.logger.debug("Starting dir for failed features is: %s", fmwDir)

        outDir = os.path.join(fmwDir, outputsDir)
        outDir = os.path.realpath(outDir)
        if not os.path.exists(outDir):
            msg = 'The outputs directory %s does not exist, creating now'
            self.logger.debug(msg, outDir)
            os.makedirs(outDir)

        ffDir = os.path.join(outDir, failedFeatsDir)
        ffDir = os.path.realpath(ffDir)
        self.logger.debug("failed features directory (ffDir): %s", ffDir)
        if not os.path.exists(ffDir):
            msg = 'The failed features directory %s does not exist, creating' + \
                  ' now'
            self.logger.debug(msg, ffDir)
            os.makedirs(ffDir)
        if failedFeatsFileName:
            self.logger.debug('ffDir: %s', ffDir)
            self.logger.debug('failedFeatsFileName: %s', failedFeatsFileName)
            ffFile = os.path.join(ffDir, failedFeatsFileName)
        else:
            ffFile = os.path.join(ffDir, failedFeatsFile)
        msg = 'The failed featured file being used is {0}'
        msg = msg.format(ffFile)
        self.logger.info(msg)
        return ffFile

    def getDestDatabaseConnectionFilePath(self, position=None):  # @IgnorePep8
        '''
        :param position: The position or in other words what connection file
                         numbered parameters do you want to use in this
                         operation
        :return: the full path to the destination database connection file.
        '''
        destDir = self.parent.getFMWDirectory()
        host = self.parent.getDestinationHost()
        serviceName = self.parent.getDestinationServiceName()
        fileNameTmpl = '{0}__{1}.sde'
        connectionFile = fileNameTmpl.format(host, serviceName)
        connectionFileFullPath = os.path.join(destDir, connectionFile)
        connectionFileFullPath = os.path.realpath(connectionFileFullPath)
        self.logger.debug("connectionFileFullPath %s", connectionFileFullPath)
        if not os.path.exists(connectionFileFullPath):
            msg = 'Looking for a destination connection file with the name {0}.  ' + \
                  'This file does not exist.  Please create it using arccatalog ' + \
                  'and then re-run this job'
            msg = msg.format(connectionFileFullPath)
            self.logger.error(msg)
            raise IOError(msg.format(connectionFileFullPath))
        else:
            self.logger.debug("SDE connection file %s exists",
                              connectionFileFullPath)
        return connectionFileFullPath

    def getSrcDatabaseConnectionFilePath(self, position=None):
        '''
        Using the service name  and host constructs a standardized file path
        name for the connection file that the framework expects to use to
        connect to the database.

        :param position: when position is specified it effects how the method
                         retrieves the src host and src servicename from the
                         published parameters.  If position is not specified
                         for example will retrieve for host just SRC_HOST
                         if a position of say 3 is specified the SRC_HOST
                         parameter that is retrieved will be SRC_HOST_3.

        :return: The full path to the connection file that should be used to
                 connect to the SDE database.
        '''
        destDir = self.parent.getFMWDirectory()
        host = self.parent.getSrcHost(position)
        serviceName = self.parent.getSrcOraServiceName(position)
        fileNameTmpl = '{0}__{1}.sde'
        connectionFile = fileNameTmpl.format(host, serviceName)
        connectionFileFullPath = os.path.join(destDir, connectionFile)
        if not os.path.exists(connectionFileFullPath):
            msg = 'Looking for a destination connection file with the name {0}.' + \
                  'This file does not exist.  Please create it using arccatalog ' + \
                  'and then re-run this job'
            msg = msg.format(connectionFileFullPath)
            self.logger.error(msg)
            raise IOError(msg)
        else:
            self.logger.debug("SDE connection file %s exists",
                              connectionFileFullPath)
        return connectionFileFullPath


class CalcParamsDataBC(object):

    '''
    This class contains methods to accomplish various tasks related to
    production operations, or in other words how the framework should
    behave when it is running on an official databc server.  The config
    file determines the names of official servers.  Some functionality
    forks at this point.  For example in production mode password retrieval
    comes from PMP and in development mode password retrieval comes from a
    dbcreds.json file.
    '''

    def __init__(self, parent):
        self.parent = parent
        self.const = self.parent.const
        self.paramObj = self.parent.paramObj

        self.destKey = self.parent.getDestDatabaseEnvKey()

        self.logger = logging.getLogger(__name__)
        self.fmeMacroVals = self.parent.fmeMacroVals
        self.currentPMPResource = None

        # adding code to report on the pythonpath, helps us know definitively
        # what the import order is for fmeserver
        if 'PYTHONPATH' in os.environ:
            user_paths = os.environ['PYTHONPATH'].split(os.pathsep)
            self.logger.debug("os paths: %s", user_paths)
        self.logger.debug("sys paths: %s", sys.path)

    def getSrcDatabaseConnectionFilePath(self, position=None):
        '''
        With FME 2015 and up when using a SDE Geodatabase writer you need to
        specify a ESRI SDE connection file to establish your connection.

        This method returns the path to the SDE connection file.  If the
        connection file does not already exist it will get created.  The
        parameters required to create the connection file come from the
        parameters:
          - SRC_HOST
          - SRC_SERVICENAME
          - SRC_PORT.
        '''
        destDir = self.paramObj.getSdeConnFileDirectory()

        host = self.parent.getSrcHost(position)
        port = self.parent.getSrcPort(position)
        servName = self.parent.getSrcOraServiceName(position)

        msg = 'In order to calculate the name of the source database connection ' + \
              'the {1} parameter: {0} must be defined as a published ' + \
              'parameter in the fmw.  Currently it is not.  Define the parameter ' + \
              'and re-run'
        msg = msg.format(self.const.FMWParams_SrcHost, 'source host')
        # now verification that we have values
        if not host:
            self.logger.error(msg)
            raise IOError(msg)
        if not servName:
            self.logger.error(msg)
            raise IOError(msg)

        connFileName = '{0}__{1}.sde'.format(host, servName)
        connectionFileFullPath = os.path.join(destDir, connFileName)
        self.logger.debug("connectionFileFullPath: %s", connectionFileFullPath)
        if not os.path.exists(connectionFileFullPath):
            # get the url, token
            self.logger.debug("conn file does not exist, attempting to " +
                              "create")

            self.__createSDEConnectionFile(connectionFileFullPath,
                                           host, servName, port)

        else:
            self.logger.debug("SDE connection file %s already exists",
                              connectionFileFullPath)
        return connectionFileFullPath

#     def testCreate(self, connectionFileFullPath, host,
#                                   serviceName, port=None):
#         self.__createSDEConnectionFile(connectionFileFullPath, host,
#                                   serviceName, port)

    def __createSDEConnectionFile(self, connectionFileFullPath, host,
                                  serviceName, port=None):
        '''
        This method used to make a rest call to a job that would assemble the
        sde connection file.  This is no longer possible so instead will call
        a module that will setup the python paths for the arcpy import
        then call another module to create the sde file
        '''
        # this will get the arcpy paths, and add them to the sys.path
        # parameter which should then allow for use of arcpy using the
        # fme python default interpreter
        try:
            # this will get the arcpy paths, and add them to the sys.path
            # parameter which should then allow for use of arcpy using the
            # fme python default interpreter
            arcpyPaths = InstallPaths.ArcPyPaths()
            arcpyPaths.getPathsAndAddToPYTHONPATH(self.const.PythonVersion)
        except WindowsError, e:
            #
            self.logger.exception(e)
            msg = "was unable to pull the arc install from the registry.  trying " + \
                  'to guess what the install location is before failing.'
            self.logger.warning(msg)
            desktopDir = self.paramObj.getArcGISDesktopRootDirectory()
            pythonRootDir = self.paramObj.getPythonRootDir()
            self.logger.debug("pythonRootDir: %s", pythonRootDir)
            self.logger.debug("desktopDir: %s", desktopDir)

            arcpyPaths = InstallPaths.ArcPyPaths()
            desktopPaths = arcpyPaths.getArcGisDesktopPaths(desktopDir)
            pyPaths = arcpyPaths.ammendPythonPaths(pythonRootDir)
            self.logger.debug("desktopPaths: %s", desktopPaths)
            self.logger.debug("pyPaths: %s", pyPaths)

            desktopPaths.extend(pyPaths)  # merge the two lists
            sys.path.extend(desktopPaths)
            # self.paramObj.get
            # if os.path.exists(E:\sw_nt\arcgis\Desktop10.2)

        except:
            self.logger.error("uncaught exceptions when fixing arcy paths")
            raise

        # next step... call CreateSDEConnectonFile module to create the path
        # I know its strange to import this module here as opposed to at the
        # top reason is this module uses arcpy... importing arcpy has a
        # significatn processing costs as it can take up to 30 seconds to
        #  complete.  Also the number of times this needs to be done is rare.
        import CreateSDEConnectionFile

        connFile = CreateSDEConnectionFile.CreateConnectionFile(
            connectionFileFullPath, host, serviceName, port)
        connFile.createConnFile()

    def getDestDatabaseConnectionFilePath(self, position=None):
        '''
        This method will retrieve the destination database connection file
        path.

        This is the production version so it will:
          a) calculate the path
          b) calculate the connection file name
          c) look for the existence of that file, if it does not exist then
             call a method to create it via a jenkins rest call
          d) returns the path to the connection file

        '''
        if position:
            self.logger.info("position parameter is not being used! " +
                             "Currently set to %s",
                             position)
        destDir = self.paramObj.getSdeConnFileDirectory()
        host = self.parent.getDestinationHost()
        serviceName = self.parent.getDestinationServiceName()
        fileNameTmpl = '{0}__{1}.sde'
        connectionFile = fileNameTmpl.format(host, serviceName)
        connectionFileFullPath = os.path.join(destDir, connectionFile)
        if not os.path.exists(connectionFileFullPath):
            # get the url, token
            self.__createSDEConnectionFile(connectionFileFullPath,
                                           host, serviceName)
        else:
            self.logger.debug("SDE connection file %s already exists",
                              connectionFileFullPath)
        return connectionFileFullPath

    def getDestinationPassword(self, destKey=None, schema=None, position=None):
        '''
        :param destKey: The destination database environment key.  Used as a
                        lookup key to identify various destination parameters
        :param schema: The schema whos password you want to retrieve.  If no
                       schema is provided then the schema is extracted from
                       the published parameters
        :param position: If schema is retrieved from the published parameters,
                         AND the position is specified then will search for
                         a published parameter for the schema that corresponds
                         with the position. Example if position is 3 then
                         searches for DEST_SCHEMA_3.  If no posiiton is
                         provided then searches for just DEST_SCHEMA
        :return: the destination password extracted from PMP.
        '''
        self.logger.debug("params: getDestinationPassword")
        if not destKey:
            destKey = self.destKey
        else:
            self.paramObj.validateKey(destKey)
            destKey = self.paramObj.getDestinationDatabaseKey(destKey)
        self.logger.debug("destination key used in password retrieval: %s",
                          destKey)

        if not schema:
            schema = self.parent.getDestinationSchema(position)

        pmpHelper = PMPHelper(self.paramObj, destKey)

        msg = 'retrieving the destination password for schema: ({0}) db ' + \
              'env key: ({1})'
        msg = msg.format(schema, destKey)
        self.logger.debug(msg)

        passwrd = pmpHelper.getDestinationPMPAccountPassword(schema)
        self.logger.info("successfully retrieved password!")
        return passwrd

    def getSourceSqlServerPassword(self, position, ignoreDomain=False,
                                   retry=False):
        '''
        :param position: the position parameter to be used when retrieving
                         the password
        :param ignoreDomain: if this parameter set to True then when looking
                             for a match for the password, ignores domains.
        :param retry: whether to make multiple attempts at getting a password
                      if one attempt fails.

        :return: the sql server password from our PMP credential management
                 system.
        '''
        self.logger.debug("getSourceSqlServerPassword")
        pswd = None

        pmpHelper = PMPHelper(self.paramObj, self.destKey)


        # get the schema / host / dbname from the
        # fme parameters
        srcSchemaInFMW = self.parent.getSourceSqlServerConnectionSchema(
            position)
        self.logger.debug("using the schema: %s", srcSchemaInFMW)

        ssDbNameInFMW = self.parent.getSrcSqlServerDatabaseName(position,
                                                                ignoreDomain)
        ssHostInFMW = self.parent.getSrcHost(position, ignoreDomain)
        self.logger.debug("host from fmw %s", ssHostInFMW)
        self.logger.debug("dbName from fmw %s", ssDbNameInFMW)

        # now get the pmp resource to search
        srcResources = self.paramObj.getSourcePmpResources()
        self.logger.debug("src resources are: %s", srcResources)
        accntCnt = 1

        # TODO: most of the logic below should be moved into the pmp helper
        #  class with a new method witha title like

        # pmp sql server identifier
        sqlServerIdentifier = self.paramObj.getSqlServerPMPIdentifier()
        for pmpResource in srcResources:
            self.logger.debug("searching for password in the pmp resource %s",
                              pmpResource)
            self.currentPMPResource = pmpResource
            # start by trying to just retrieve the account using
            # destSchema@servicename as the "User Account" parameter

            self.logger.debug("getting account ids")
            accnts = pmpHelper.getAccountDictionary(pmpResource)
            for accntDict in accnts:

                pmpSrcRecordParser = PMPSourceAccountParser(
                    accntDict[self.const.PMPKey_AccountName],
                    sqlServerIdentifier)
                # only print every 20th account name
                if not accntCnt % 20:
                    self.logger.debug("accntName: %s",
                                      accntDict[self.const.PMPKey_AccountName])
                if pmpSrcRecordParser.isSqlServerRecord():
                    schemaInPMP = pmpSrcRecordParser.getSchema()
                    self.logger.debug("schemaInPMP: %s", schemaInPMP)
                    # self.logger.debug("cur schema / search schema: {0} / {1}". \ @IgnorePep8
                    # format(schema, accntNameInFMW))
                    if schemaInPMP.lower() == srcSchemaInFMW.lower():
                        self.logger.debug(
                            "schemas match %s",
                            accntDict[self.const.PMPKey_AccountName])

                        # now check for host match
                        pmpHost = pmpSrcRecordParser.getSqlServerHost(
                            ignoreDomain)
                        pmpDbName = \
                            pmpSrcRecordParser.getSqlServerDatabaseName(
                                ignoreDomain)

                        if ignoreDomain:
                            pmpDbName = pmpDbName.lower().strip()
                            ssDbNameInFMW = pmpDbName.lower().strip()

                        if pmpHost.lower() == ssHostInFMW.lower() and \
                          pmpDbName == ssDbNameInFMW:
                            # we have a match
                            self.logger.debug("host and database names match")
                            accntId = accntDict[self.const.PMPKey_AccountId]

                            pswd = pmpHelper.getPMPPassword(accntId,
                                                            pmpResource)

                            break
                accntCnt += 1
        if not pswd and not retry:
            self.logger.debug("No password found when trying exact " +
                              "host/dbname match, now trying to match " +
                              "without suffix")
            pswd = self.getSourceSqlServerPassword(position,
                                                   ignoreDomain=True,
                                                   retry=True)

        if not pswd:
            msg = 'unable to get password for account: {0}, ' + \
                  'database name: {1}, host: {2}'
            raise ValueError(msg.format(srcSchemaInFMW,
                                        ssDbNameInFMW, ssHostInFMW))
        return pswd

    def getSourcePassword(self, position=None):
        '''
        :param position: used to grab the servicename and schema who's
                         password we should be retrieving.  If no position
                         specified then retrieves SRC_ORA_SCHEMA and
                         SRC_ORA_SERVICENAME.  If a position is specified
                         then looks for that positions schema and
                         servicename, example SRC_ORA_SCHEMA_3 and
                         SRC_ORA_SERVICENAME_3
        :return: The password associated with the schema / servicename
        '''
        # pmp connection
        # pmp = self.__getPMPObj()
        self.logger.debug("params: getSourcePassword")
        pmpHelper = PMPHelper(self.paramObj, self.destKey)
        # Below is going to test to see if the proxy schema parameter
        # is set.  In order to do so if the position paramter is set then need
        # to calculate what the key is according to the position in order to
        # test to see whether its there or not.
        if position:
            proxySchemaMacroKey = self.parent.getMacroKeyForPosition(
                self.const.FMWParams_SrcProxySchema, position)
        else:
            proxySchemaMacroKey = self.const.FMWParams_SrcProxySchema
        msg = 'Searching for a proxy schema published parameter in the ' + \
              'macro %s'
        self.logger.info(msg, proxySchemaMacroKey)

        # get the source schema
        if self.parent.existsMacroKey(proxySchemaMacroKey):
            srcOraSchema = self.parent.getSourceOracleProxySchema(position)
        else:
            srcOraSchema = self.parent.getSourceOracleSchema(position)
        srcOraServName = self.parent.getSourceOracleServiceName(position)

        pswd = None

        # Need to detect if the source instance is bcgw.  If it is then
        # get the password from there.
        isSrcBCGW = self.parent.isSourceBCGW(position)

        if isSrcBCGW:
            # destKey = self.parent.getDestDatabaseEnvKey()
            # need to figure out what the destination is now based on
            #
            destKey = self.parent.getSourceDestDBKey(position)
            msg = 'Source has been detected to be from the ' + \
                  'bcgw.  Retrieving the password for the ' + \
                  'source database using the destination ' + \
                  'keyword {0}'
            msg = msg.format(destKey)
            self.logger.info(msg)

            pswd = self.getDestinationPassword(destKey=destKey,
                                               schema=srcOraSchema)
        else:
            msg = 'retrieving source password from pmp for schema: (%s), ' + \
                  'service name: (%s)'
            self.logger.info(msg, srcOraSchema, srcOraServName)
            srcResources = self.paramObj.getSourcePmpResources()

            sqlServerIdentifier = self.paramObj.getSqlServerPMPIdentifier()
            for pmpResource in srcResources:
                self.logger.debug("searching for password in the pmp " +
                                  "resource %s", pmpResource)
                self.currentPMPResource = pmpResource
                # start by trying to just retrieve the account using
                # destSchema@servicename as the "User Account" parameter
                try:
                    self.logger.debug("getting account ids")
                    accnts = pmpHelper.getAccountDictionary(pmpResource)
                    for accntDict in accnts:
                        accntName = PMPSourceAccountParser(
                            accntDict[self.const.PMPKey_AccountName],
                            sqlServerIdentifier)
                        schema = accntName.getSchema()
                        if schema.lower() == srcOraSchema.lower():
                            self.logger.debug("schemas match %s",
                                              accntDict[self.const.PMPKey_AccountName])
                            serviceName = accntName.getServiceName()
                            self.logger.debug("cur service_name / search " + \
                                              "serivce_name: %s, %s",
                                              serviceName, srcOraServName)
                            if serviceName.lower() == srcOraServName.lower():
                                # match return password for this account
                                accntId = accntDict[
                                    self.const.PMPKey_AccountId]
                                pswd = pmpHelper.getPMPPassword(accntId,
                                                                pmpResource)
                                break
                    if not pswd:
                        msg = 'unable to get password for the account ' + \
                              'name: {0} and database service name {1}'
                        raise ValueError(msg.format(srcOraSchema,
                                                    srcOraServName))
                except ValueError:
                    msg = 'There is no account for schema %s / service name ' + \
                          '%s in pmp for the resource %s using the token %s ' + \
                          ' from the machine %s'
                    self.logger.warning(msg, srcOraSchema, srcOraServName,
                                        pmpResource, pmpHelper.pmpDict['token'],
                                        platform.node())
                    msg = "trying a heuristic that ignores domain " + \
                                     "information to find a password for {0}@{1}"
                    msg = msg.format(srcOraSchema, srcOraServName)
                    self.logger.info(msg)
                    # Going to do a search for accounts that might match the user
                    pswd = self.getSourcePasswordHeuristic(position)
                if pswd:
                    break
                # add some more warnings
        return pswd

    def getSourcePasswordHeuristic(self, position=None):
        '''
        Gets a list of accounts for the pmp resource.  Iterates through the
        list looking for an account that is similar to the destSchema
        instance combination defined in the FMW.  PMP stores the account
        names for pmp in the format destSchema@instance.  This method will
        parse that into destSchema and instance then look for schemas that
        match then for instance it will look for matchs independent of any
        domain. Thus envprod1.nrs.bcgov will match envprod1.env.gov.bc.ca.

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
        self.logger.debug("params: getSourcePasswordHeuristic")

        # Getting the oracle schema and service name, so can get
        # corresponding password
        if self.parent.existsMacroKey(self.const.FMWParams_SrcProxySchema):
            srcOraSchema = self.parent.getSourceOracleProxySchema(position)
        else:
            srcOraSchema = self.parent.getSourceOracleSchema(position)
        srcOraServNameWithDomain = self.parent.getSourceOracleServiceName(
            position)

        # remove domain from the service name for this search
        srcOraServName = Util.removeDomain(srcOraServNameWithDomain)

        msg = "Using a heuristic to try to find the password for schema/s" + \
              "ervice name: %s/%s"
        self.logger.debug(msg, srcOraSchema, srcOraServName)

        # setting up pmp connection
        pmpResource = self.currentPMPResource
        if not pmpResource:
            srcResources = self.paramObj.getSourcePmpResources()
        else:
            srcResources = [self.currentPMPResource]

        pswd = None
        pmpHelper = PMPHelper(self.paramObj, self.destKey)

        # test pmp connectivity
        self.logger.debug("testing connectivity with PMP")

        # need this parameter to create the PMPSourceAccountParser object
        # but doesn't get used as this method retrieves oracle passwords
        sqlServerIdentifier = self.paramObj.getSqlServerPMPIdentifier()

        # iterate through pmp resources / accounts looking for a match
        for pmpResource in srcResources:
            # resource id for the pmp resource.
            # resId = pmp.getResourceId(pmpResource)
            # list of accounts attached to the resource
            # accounts = pmp.getAccountsForResourceID(resId)
            accounts = pmpHelper.getAccountDictionary(pmpResource)
            snList = []
            msg = 'iterating through the accounts in resource name {0}'
            msg = msg.format(pmpResource)
            self.logger.debug(msg)
            # source instance, and the source instance less the domain portion
            for accntDict in accounts:
                accntName = PMPSourceAccountParser(
                    accntDict[self.const.PMPKey_AccountName],
                    sqlServerIdentifier)  # 'ACCOUNT NAME'
                self.logger.debug("account name: (%s) searching for (%s)",
                                  accntName.getSchema(), srcOraSchema)
                schema = accntName.getSchema()
                if schema.lower().strip() == srcOraSchema.lower().strip():
                    # found the srcSchema
                    # now check see if the instance matches
                    # print 'schemas {0} : {1}'.format(destSchema, self.fmeMacroVals[self.const.FMWParams_SrcSchema]) @IgnorePep8
                    self.logger.debug("schemas match %s %s",
                                      schema, srcOraSchema)
                    servName = accntName.getServiceNameNoDomain()
                    self.logger.debug("service name %s %s",
                                      servName, srcOraServName)
                    if servName.lower().strip() == srcOraServName.lower().strip():  # @IgnorePep8
                        snList.append([accntDict[self.const.PMPKey_AccountName],  # @IgnorePep8
                                       accntDict[self.const.PMPKey_AccountId]])
        if snList:
            if len(snList) > 1:
                # get the passwords, if they are the same then return
                # the password
                msg = 'found more than one possible source password match' + \
                      ' for the service name: {0}'
                msg = msg.format(srcOraServName)
                self.logger.warning(msg)
                msg = 'service names that match include {0}'.format(
                    ','.join(snList))
                self.logger.debug(msg)
                pswdList = []
                # eliminating any possible duplicates then
                for accnts in snList:
                    pswd = pmpHelper.getPMPPassword(accnts[1], pmpResource)
                    pswdList.append(pswd)
                    # pswdList.append(pmp.getAccountPasswordWithAccountId(accnts[1], resId)) @IgnorePep8
                pswdList = list(set(pswdList))
                if len(pswdList) > 1:
                    serviceNameMacroKey = \
                        self.parent.getMacroKeyForPosition(
                            self.const.FMWParams_SrcServiceName, position)
                    msg = 'Looking for the password for the schema {0}, and service name {1}' + \
                          'Found the following accounts that roughly match that combination {2}' + \
                          'pmp has different passwords for each of these.  Unable to proceed as ' + \
                          'not sure which password to use.  Fix this by changing the parameter ' + \
                          'to match a "User Account" in pmp exactly.'
                    msg = msg.format(srcOraSchema,
                                     srcOraServName,
                                     ','.join(snList),
                                     serviceNameMacroKey)
                    raise ValueError(msg)
            else:
                # get the password and return it
                pswd = pmpHelper.getPMPPassword(snList[0][1], pmpResource)
        if not pswd:
            msg = 'unable to find the password using the heuristic search' + \
                  'for the schema: {0}, service name {1}'
            msg = msg.format(srcOraSchema, srcOraServName)
            self.logger.error(msg)
            raise ValueError(msg)
        return pswd

    def getFailedFeaturesFile(self, failedFeatsFileName=None, position=None):
        '''
        this is going to put the failed features
        into the template outputs directory.
        '''
        self.logger.debug("params: getFailedFeaturesFile")
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

        fmwName, suffix = os.path.splitext(fmwName)
        del suffix
        fmwDir = os.path.join(ffDir, fmwName)
        if not os.path.exists(fmwDir):
            msg = "Creating the failed feature fmw dir {0}"
            msg = msg.format(fmwDir)
            self.logger.debug(msg)
            os.makedirs(fmwDir)
        if not failedFeatsFileName:
            destSchema = self.parent.getDestinationSchema(position)
            destFeature = self.parent.getDestinationFeature(position)
            jobNumStr = ''
            if self.const.FMEMacroKey_JobId in self.fmeMacroVals:
                jobNumStr = self.fmeMacroVals[self.const.FMEMacroKey_JobId]
            # ("^Y^m^d^H^M^S")_failed_features.ffs
            # job will be dest_schema
            failedFeatsFile = '{0}_{1}_{2}_failedFeatures.ffs'.format(
                destSchema, destFeature, jobNumStr)
            ffFullPathFile = os.path.join(fmwDir, failedFeatsFile)
        else:
            ffFullPathFile = os.path.join(fmwDir, failedFeatsFileName)
        msg = "The full file path for failed features ffs file is {0}".format(
            ffFullPathFile)
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

    def __init__(self, fmeMacroVals, forceDevelMode=False,
                 customLogConfig=None):
        self.const = TemplateConstants()
        fmwDir = fmeMacroVals[self.const.FMWMacroKey_FMWDirectory]
        fmwName = fmeMacroVals[self.const.FMWMacroKey_FMWName]
        destKey = fmeMacroVals[self.const.FMWParams_DestKey]

        if destKey.lower() == self.const.ConfFileDestKey_Devel.lower():
            forceDevelMode = True

        ModuleLogConfig(fmwDir, fmwName, destKey, customLogConfig)

        self.logger = logging.getLogger(__name__)

        # self.logger.info("inheriting the CalcParamsBase class")
        CalcParamsBase.__init__(self, fmeMacroVals)
        # self.logger.debug("adding plugin functionality")
        self.addPlugin(forceDevelMode)


class ModuleLogConfig(object):
    '''
    This class is used to help create a second "enhanced logger" that
    writes all python log messages somewhere other than the main fme log.
    The reason for this is the main fme log is not available to scripted
    parameters and as such when errors occur in scripted parameters its
    very difficult to figure out what exactly is happening.

    The log that is configured by this module is intended to give us that
    information.
    '''

    def __init__(self, fmwDir, fmwName, destKey=None, customLogConfig=None):
        '''
        :param fmwDir: the directory of the fmw that is being run
        :param fmwName: the name of the fmw file
        :param destKey: contents of the DEST_DB_ENV_KEY
        :param customLogConfig: a custom log config file, this parameter
                                was added to allow for unit_tests to be run
                                independent of fme.  the default log config
                                is set up to use fmeobjects to write to the
                                fme log file.  This config file allows for
                                the elimination of that depedency.

        '''
        logConfFileFullPath = None
        if not destKey:
            destKey = 'DEV'
        logFileFullPath = Util.calcLogFilePath(fmwDir, fmwName)

        # get the tmpLog to test to see if the logger has been
        # initialized yet or not
        tmpLog = logging.getLogger(__name__)
        tmpLog.info("Module Log config logger: %s", __name__)
        tmpLog.debug("logFileFullPath: %s", logFileFullPath)
        tmpLog.debug("handlers already configed: {0}".format(tmpLog.handlers))
        if not tmpLog.handlers:
            tmpLog.info('Logger does not have a handler configured')
            logging.logFileName = logFileFullPath
            const = TemplateConstants()
            confFile = TemplateConfigFileReader(destKey)

            # if the log config file has been sent specifically then don't
            # calculate it
            if customLogConfig:
                logConfFileFullPath = customLogConfig
            else:

                # Get the log config file name from the app config file
                logConfFileName = confFile.getApplicationLogFileName()

                # get the name of the conf dir.  The output is always
                # relative to this module.  the output log is
                configDir = const.AppConfigConfigDir
                dirname = os.path.dirname(__file__)
                logConfFileFullPath = os.path.join(dirname, configDir,
                                                   logConfFileName)
            enhancedLoggingFileName = Util.calcEnhancedLoggingFileName(fmwName)
            enhancedLoggingDir = confFile.calcEnhancedLoggingFileOutputDirectory(fmwDir, fmwName)  # @IgnorePep8
            enhancedLoggingFullPath = os.path.join(enhancedLoggingDir,
                                                   enhancedLoggingFileName)
            enhancedLoggingFullPath = os.path.realpath(enhancedLoggingFullPath)
            enhancedLoggingFullPath = enhancedLoggingFullPath.replace(
                os.path.sep, '/')
            if not os.path.exists(enhancedLoggingFullPath):
                fh = open(enhancedLoggingFullPath, 'w')
                fh.close()

            logging.config.fileConfig(logConfFileFullPath, defaults={
                'logfilename': str(enhancedLoggingFullPath)})
            logger = logging.getLogger(__name__)
            # logger.debug("logger should be configured")
            # logger.debug("log name: {0}".format(__name__))
            logger.info("enhancedLoggingFullPath: %s", enhancedLoggingFullPath)
            logger.info("log config file being used: %s", logConfFileFullPath)
        else:
            tmpLog.debug("log already configured")


class PMPHelper(object):
    '''
    This class has been created to ensure that every time
    we need a pmp object it is created in a consistent manner
    and also enables all time we need pmp communication

    that it use the failover address if initial communication attempts
    fail.

    :ivar paramObj: contains a instance of the TemplateConfigFileReader class.
    :ivar destKey: the destination database key.  Comes from the published
                   parameter of the fmw, (DEST_DB_ENV_KEY)
    '''

    def __init__(self, paramObj, destKey, testConnection=True):
        # having trouble getting the PMP fail over to work properly, so have
        # created a new log entry here that can be set to debug to help
        # resolve this issue next time it comes up.
        loggerName = '{0}.{1}'.format(__name__, self.__class__.__name__)

        self.logger = logging.getLogger(loggerName)
        self.logger.debug("PMPHELPER Test logger, log name: %s", loggerName)
        self.paramObj = paramObj
        self.destKey = destKey

        self.pmpDict = self.getPMPConnectionDictionary()
        self.logger.debug("attempt to communicate with pmp url: %s",
                          self.pmpDict['baseurl'])
        self.pmp = PMP.PMPRestConnect.PMP(self.pmpDict)
        self.resources = None
        if testConnection:
            self.attemptCommunication()

        self.resIdCache = {}

        self.failWaitTime = 60 * 5
        self.maxWaitIntervals = 5
        self.currentRetry = 0

        self.sshKeyFile = 'SSH.ppk'

    def getPMPConnectionDictionary(self, baseUrl=None):
        '''
        Creates and returns a PMP connection dictionary.  The dictionary
        contains the application token, pmp base url, and the directory
        path to the rest api on the pmp server.

        This dictionary is what is required by the python pmp wrapper to create
        a PMP object.
        :param baseUrl: if the base url to the rest api is different from that
                        which is defined in the framework config file, you
                        can specify it here.
        :return: a dictionary is a required arguement for the
                 PMP.PMPRestConnect.PMP class constructor.
        :rtype: dict
        '''
        computerName = Util.getComputerName()
        self.logger.debug("Computer name: %s", computerName)
        if not baseUrl:
            baseUrl = self.paramObj.getPmpBaseUrl()
        pmpDict = {'token': self.paramObj.getPmpToken(computerName),
                   'baseurl': baseUrl,
                   'restdir': self.paramObj.getPmpRestDir()}
        return pmpDict

    def getPMPAltConnectionDictionary(self):
        '''
        PMP servers are set up with a fail over server.  This method will
        return the dictionary that can be used to connect to the fail over
        server.  Keys in the return dict:
           - token
           - baseurl
           - restdir
        '''
        computerName = Util.getComputerName()
        altUrl = self.paramObj.getPmpAltUrl()
        pmpAltDict = {'token': self.paramObj.getPmpToken(computerName),
                      'baseurl': altUrl,
                      'restdir': self.paramObj.getPmpRestDir()}
        return pmpAltDict

    def getDestinationPMPAccountPassword(self, schema):
        '''
        Using the destination key to identify the pmp resource, extracts the
        password from pmp for the provided schema
        :param schema: the schema for which we want to retrieve a password for.
        :return: The password !
        :rtype: str
        '''
        passwrd = None
        pmpResources = self.paramObj.getDestinationPmpResource(self.destKey)

        # pmpRes is plural, ie could be a string if there is one or a list
        # if more than one.
        for pmpResource in pmpResources:
            self.logger.debug('searching for password in %s', pmpResource)
            try:
                passwrd = self.pmp.getAccountPassword(schema, pmpResource)
                if passwrd:
                    break
            except ValueError, e:
                msg = 'could not find the password for: {0} in the pmp resource {1}.' + \
                      ' original error message: {2}'
                msg = msg.format(schema, pmpResource, e)
                self.logger.warning(msg)

        if not passwrd:
            msg = 'Cant find the password in any of the PMP resources: {0} for ' + \
                  'the account {1}'
            msg = msg.format(pmpResources, schema)
            self.logger.warning(msg)
        return passwrd

    def attemptCommunication(self):
        '''
        Sometimes PMP does go offline.  It seems that this usually happens
        around 3am.  This method puts in place an attempt to monitor
        communication with pmp, and when pmp becomes unavailable, instead
        of simply crashing the current replication, the script instead goes
        into a retry loop, where it pauses and then attempts to retry
        communication with PMP.

        Properties that influence this method and default values in brackets
         - failWaitTime = amount of time to wait in between attempts to
                          communicate with pmp. (5 minutes)
         - maxWaitIntervals, maximum number of times to retry communication
                             with pmp (5 times)
         - currentRetry, Used to keep track of what retry attempt we are on
        '''
        try:
            # try communication with the default url
            self.getResources()
        # ConnectionError
        except (PMP.PMPRestConnect.PMPCommunicationProblem,
                requests.ConnectionError), e:
            self.logger.warning("attempting the alternate pmp server")
            # error trapped, now try alt url
            altdict = self.getPMPAltConnectionDictionary()

            msg = "error raised in attempt to communicate with pmp." + \
                  "switching to use the alternative url: <not listing " + \
                  "here>, pmp error message: %s"
            self.logger.warning(msg, e)
            self.pmpDict = altdict
            self.pmp = PMP.PMPRestConnect.PMP(self.pmpDict)

            self.resources = None
            try:
                self.getResources()
            except PMP.PMPRestConnect.PMPCommunicationProblem, e:
                msg = 'Communication problem with pmp, tried both these  ' + \
                      'urls ({0}) ({1})neither is responding, error ' + \
                      'message is: {2}'
                url = self.paramObj.getPmpBaseUrl()
                altUrl = self.paramObj.getPmpAltUrl()
                msg = msg.format(url, altUrl, e)
                self.logger.error(msg)

                if self.currentRetry >= self.maxWaitIntervals:

                    self.logger.error("Unable to communicate with PMP or" +
                                      " the alternate PMP url")
                    raise
                else:
                    msg = "unable to communicate with PMP. Waiting {0} " + \
                          'seconds currently on retry interval {1} of {2}'
                    msg = msg.format()
                    self.logger.warning(self.failWaitTime,
                                        self.currentRetry,
                                        self.maxWaitIntervals)
                    time.sleep(self.failWaitTime)

                    # return to base parameters
                    self.pmpDict = self.getPMPConnectionDictionary()
                    self.pmp = PMP.PMPRestConnect.PMP(self.pmpDict)

                    self.currentRetry += 1
                    self.attemptCommunication()

    def getResources(self):
        '''
        Returns a list resources available to the account that is configured
        behind the token that we gained access to PMP with.
        '''
        if not self.resources:
            self.resources = self.pmp.getResources()

    def getResourceId(self, resourceName):
        '''
        :param resourceName: The name of the resource who's resource id it
                             is we want to retrieve.
        :return: The resource id for the given resource name
        :rtype: int
        '''
        retVal = None
        if resourceName in self.resIdCache:
            retVal = self.resIdCache[resourceName]
        else:
            retVal = self.pmp.getResourceId(resourceName)
            self.resIdCache[resourceName] = retVal
        msg = "resource id / resource name: {0}/{1}".format(retVal,
                                                            resourceName)
        self.logger.info(msg)
        return retVal

    def getAccountDictionary(self, resourceName):
        '''
        Returns an account dictionary that describes the resoure name provided.
        (actually returns a list of dictionaries with the following keys:)
         - ACCOUNT ID
         - ACCOUNT NAME
         - AUTOLOGONLIST
         - AUTOLOGONSTATUS
         - ISFAVPASS
         - ISREASONREQUIRED
         - IS_TICKETID_REQD
         - IS_TICKETID_REQD_ACW
         - IS_TICKETID_REQD_MANDATORY
         - PASSWDID
         - PASSWORD STATUS

        see pmp rest api for more information.

        :param resourceName: name of the resource who's acccounts we want to
                             retrieve.
        :return: see description above
        :rtype: list(dict)
        '''
        resId = self.getResourceId(resourceName)
        token = self.pmpDict['token']
        if not resId:
            msg = 'Unable to retrieve a resource id in pmp for the ' + \
                  'resource name {0} using the token {1}'
            msg = msg.format(resourceName, token)
            raise ValueError(msg)
        accnts = self.pmp.getAccountsForResourceID(resId)
        return accnts

    def getAccountPassword(self, accountName, resourceNames):
        '''
        :param accountName: the account name (schema name) who's password
                            it is that you want to retrieve.
        :param resourceNames: the names of the resource in PMP that the
                             script should search for the account/password in.
        :type resourceNames: list
        :return: This method returns the password that matches the
                 "accountName" in the PMP "resourceName"
        '''
        for resourceName in resourceNames:
            passwrd = self.pmp.getAccountPassword(accountName, resourceName)
            if passwrd:
                break
        return passwrd

    def getPMPPassword(self, accountId, resourceName):
        '''
        :param accountId: The schema or account name who's password we want to
                          retrieve
        :param resourceName: The resource name in pmp that contains the
                             resource
        :return: password for the given account / resourceName

        Retrieves the password for the accountId from the pmp resource in the
        parameter resourceName
        '''
        resId = self.getResourceId(resourceName)
        pswd = self.pmp.getAccountPasswordWithAccountId(accountId, resId)
        return pswd

    def getSSHKey(self, sshKeyFilePath=None):
        '''
        This method will use the properties described in the app default config
        file to retrieve the ssh key from pmp and locate it in the location
        specified!  If no location is specified it gets located in the config
        directory
        '''
        if not sshKeyFilePath:
            # get the destination path to the ssh key file
            confDir = self.paramObj.getConfigDirName()
            rootDir = self.paramObj.getTemplateRootDirectory()
            sshKeyFilePath = os.path.join(rootDir, confDir, self.sshKeyFile)

        if os.path.exists(sshKeyFilePath):
            os.remove(sshKeyFilePath)

        keyFileContents = self.pmp.getPasswordFiles('BIOT', 'SSH_KEYS')
        # next need to write the contents of the variable keyFileContents to
        # the destination file.
        fh = open(sshKeyFilePath, 'w')
        fh.write(keyFileContents)
        fh.close()
        return sshKeyFilePath


class DWMWriter(object):
    r'''  #pylint: disable=anomalous-backslash-in-string
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

    # TemplateConfigFileReader
    def __init__(self, fme, const=None):
        self.const = const
        if not self.const:
            self.const = TemplateConstants()
        self.fme = fme
        self.logger = logging.getLogger(__name__)

        self.params = CalcParamsBase(self.fme.macroValues)
        self.pubParams = GetPublishedParams(self.fme.macroValues)

        self.destKey = self.params.getDestDatabaseEnvKey()
        self.config = TemplateConfigFileReader(self.destKey)

        self.dwmKey = self.config.getDWMDestinationKey(self.destKey)
        # in case the dwm key is
        self.config = TemplateConfigFileReader(self.dwmKey)

        self.getDatabaseConnection()

    def getDatabaseConnection(self):
        '''
        Using the information in the config file defined for the framework,
        retrieves the database connection information required to populate
        the DWM report, including communication with PMP to retrieve the
        password.

        Uses this information to create a database connection object that will
        be used by subsequent method to populate a record in the DWM reporting
        table.
        '''
        pmpHelper = PMPHelper(self.config, self.dwmKey)
        accntName = self.config.getDWMDbUser()
        serviceName = self.config.getDestinationServiceName()

        pmpResources = self.config.getDestinationPmpResource()
        passwrd = pmpHelper.getAccountPassword(accntName, pmpResources)
        if isinstance(passwrd, unicode):
            # when attempting to connect to database using unicode encoded
            # password am getting a error message:
            # "argument 2 must be str, not unicode"
            # this is an attempt to addresses that problem
            passwrd = str(passwrd)
        self.logger.debug(u"accntName: %s", accntName)
        self.logger.debug(u"service name: %s", serviceName)
        self.db = DB.DbLib.DbMethods()
        try:
            self.db.connectParams(accntName, passwrd, serviceName)
        except DB.DbLib.ConnectionError, e:
            try:
                self.logger.warning(str(e))
                host = self.config.getDestinationHost()
                msg = u'unable to create a connection to the schema: {0},' + \
                      u' instance {1} going to try to connect directly ' + \
                      u'to the server: {2}'
                msg = msg.format(accntName, serviceName, host)
                self.logger.warning(msg)
                port = self.config.getDestinationOraclePort()
                self.logger.debug(u"port: %s", port)
                self.logger.debug(u"host: %s", host)
                self.db.connectNoDSN(accntName, passwrd,
                                     serviceName, host, port)
                self.logger.debug(u"successfully connected to database " +
                                  u"using direct connect")
                # TODO: Should really capture the specific error type here
            except Exception, e:
                self.logger.error(u"unable to create the database " +
                                  u"connection, error message to follow...")
                self.logger.error(u"error message: %s", sys.exc_info()[0])
                # self.logger.info(msg)
                msg = u'database connection used to write to DWM has failed, ' + \
                      u'dwm record for this replication will not be written'
                self.logger.error(msg)
                raise
        else:
            msg = u'successfully connected to the database {0} with ' + \
                  u' the user {1}'
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
        '''
        This method calls various other methods to coallate all the information
        about the run event for reporting in a new dwm record.
        :return: Returns a dictionary that describes the most recent run event.
                 keys in the dictionary include:
                  - mapping_file_id
                  - start_time
                  - end_time
                  - exit_status
                  - features_read_count
                  - features_written_count
                  - features_rejected_count
                  - notification_email
                  - log_filename
                  - datasource
                  - duration
                  - dest_instance
                  - dest_schema
                  - dest_table
        :rtype: dict
        '''
        returnDict = {}
        returnDict['mapping_file_id'] = self.getMapFileId()
        returnDict['start_time'] = self.getStartTime()
        returnDict['end_time'] = self.getEndTime()
        returnDict['exit_status'] = self.getExitStatus()
        returnDict['features_read_count'] = self.getTotalFeaturesReadCount()
        returnDict['features_written_count'] = \
            self.getTotalFeaturesWrittenCount()
        returnDict['features_rejected_count'] = \
            self.getTotalFeaturesRejectedCount(subprocess=True)
        returnDict['notification_email'] = self.getNotificationEmail()
        returnDict['log_filename'] = self.getLogFileName()
        returnDict['datasource'] = self.getDataSource()
        returnDict['duration'] = self.getDuration()
        # Weakness in DWM, set up to use instance.  To resolve we are just
        # writing the destination servicename to the DWM record
        returnDict['dest_instance'] = self.getDestServiceName()
        returnDict['dest_schema'] = self.getDestSchema()
        returnDict['dest_table'] = self.getDestTable()
        return returnDict

    def getInsertStatement(self):
        '''
        :return: Retrieves the insert statement that is used to populate the
                 dwm table.  the insert statement is parameterized to allow
                 it to be run with a value list
        '''
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
        mapFileId is the property in the FMW "Workspace Properties" in
        the Name: property. This property is not always correct.  Changing
        this to get the property from the file name.

        In a nutshell returns the name of the fmw that is being executed
        '''
        mapFileId = self.params.getFMWFile()
        # mapFileId = self.fme.macroValues[self.const.FMWMacroKey_FMWName]
        mapFileId, extension = os.path.splitext(mapFileId)
        del extension

        # logic has been flipped.  Start by getting the name of the fmw
        # from the file name, if that's blank then get it from the metadata.
        if not mapFileId:
            mapFileId = self.fme.mappingFileId
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
            elapsedTime = datetime.timedelta(
                seconds=float(self.fme.elapsedRunTime))
            startTime = currentDateTime - elapsedTime
        return startTime

    def getEndTime(self):  # pylint: disable=no-self-use
        '''
        Returns the time, now.  Used to indicate when the script finished
        running. Grabbed from the shutdown process
        '''
        endTime =  datetime.datetime.now()
        self.logger.debug("end time: %s", endTime)
        return endTime

    def getExitStatus(self):
        '''
        :return: returns either Error or OK to indicate the status
                 of the script
        '''
        exitStatus = 'Error'
        if self.fme.status:
            exitStatus = 'OK'
        return exitStatus

    def getTotalFeaturesReadCount(self):
        '''
        :return: the total number of feature read.  Extracted from the fme
                 object.
        '''
        return self.fme.totalFeaturesRead

    def getTotalFeaturesWrittenCount(self):
        '''
        :return: The total number of features written.  This value is extracted
                 from the fme object.
        '''
        return self.fme.totalFeaturesWritten

    def getTotalFeaturesRejectedCount(self, subprocess=True):  # pylint: disable=no-self-use @IgnorePep8
        '''
        :return: the total number of features that have been rejected by the
                 replication.  Currently not enabled, only returns None.

        Steps:
        a) does ffs file exist?
        b) create ffs file reader
        c) determine how many features exist in the ffs file
        d) return them

        for both
          - start by getting the value of fme.logFileName
          - just add a .ffs extension (don't remove the log extension just
            add to it with .ffs.
          - if that file exists there is your ffs file.
          - open and read it.

        '''
        # self.params.get

        # need to calculate the path to python based on whether the fme
        # version is 64 bit or not, then make the 64 bit install the
        # default location
        #
        retVal = None
        logFile = self.getLogFileName()
        self.logger.debug("log file path: %s", logFile)
        logFileNoSuffix = os.path.splitext(logFile)[0]
        # the ffs file name auto created with the SDE30 writer.
        ffsFileNameSDE30 = '{0}_log.ffs'.format(logFileNoSuffix)
        # the calculated ffs file name, this is how ffs is generated for
        # geodb writers
        ffsFileNameGeoDb = self.params.getFailedFeaturesFile()
        self.logger.debug("ffs file is: %s", ffsFileNameSDE30)

        # ffs files from SDE 30 writer are named with the same name as the job
        # number.  If the fmw uses the SDE Geodb writer then the the ffs files
        # will come from a published parameter.
        ffsFile = None
        if os.path.exists(ffsFileNameSDE30):
            ffsFile = ffsFileNameSDE30
            self.logger.debug("found an SDE30 generated FFS file: %s", ffsFile)
        elif os.path.exists(ffsFileNameGeoDb):
            ffsFile = ffsFileNameGeoDb
            self.logger.debug("found an GeoDb generated FFS file: %s", ffsFile)

        self.logger.debug('FFS File that is to be read is: %s', ffsFile)

        if (ffsFile is not None) and os.path.exists(ffsFile):
            self.logger.debug("creating and FFReader object")
            fmeInstallPath = self.fme.macroValues['FME_HOME']
            ffs = FFSReader.Reader(ffsFile, fmeInstallPath=fmeInstallPath)
            if subprocess:
                # in order to configure the paths we need to send it the
                # product number ie, 2012, 2013, 2015, 2017, etc.
                productNum = self.getFMEProductNumber()
                retVal = ffs.getFeatureCountSeparateProcess(productNum)
            else:
                retVal = ffs.getFeatureCountSameProcess()
            self.logger.debug("failed features read from ffs file: %s", retVal)
        else:
            # no ffs file has been found for either geodb or
            # for sde30 writers.
            retVal = 0
        return retVal

    def getFMEProductNumber(self):
        '''
        extracts the current product string from the macro value
        FME_BUILD_DATE which looks something like:
        '20170731'

        This method will extract the first 4 characters from this string
        '''
        curProductStr = self.fme.macroValues['FME_BUILD_DATE']
        curProductStr = curProductStr.strip()
        self.logger.debug("FME_BUILD_DATE: %s", curProductStr)
        productNumber = curProductStr[0:4]
        self.logger.debug('fme product number: %s', productNumber)
        return productNumber

    def getNotificationEmail(self):  # pylint: disable=no-self-use
        '''
        :return: The notification emails used in the script.  This is no
        longer supported, and currently only returns None.
        '''
        return None

    def getLogFileName(self):
        '''
        :return: the name of the log file that is being used for this script,
                 framework scripts now use two log files.  One for the
                 published parameters logging and the other for the actual
                 fme logging / startup and shutdown logging.  The latter is
                 the one returned by this method.

        How it was done:

        logFilename = os.path.abspath(logFile)
        logFilename = logFilename.replace('\\','%5C')
        logFilename = logFilename.replace(' ','+')
        '''
        logFile = self.fme.logFileName
        return logFile

    def getDataSource(self):
        '''
        OMG!  its another cludge, just appending
        datasources together with ,+ as the delimiter!

        Long term think about writing separate records for each input, or redo
        the datamodel.
        '''
        dataSrcStr = 'Python_shutdown_can_not_get_DataSource'
        dataSourceList = self.fme.featuresRead.keys()
        if dataSourceList:
            dataSrcStr = ',+'.join(dataSourceList)
        if len(dataSrcStr) >= 180:
            # oracle table for this only accepts 180 characters, if the
            # length is greater than 180 then try to remove the path from all
            dataSrcTruncatedDirs = []
            dirlist = []
            for datasrc in dataSourceList:
                justDir = os.path.dirname(datasrc)
                if justDir not in dirlist:
                    dataSrcTruncatedDirs.append(datasrc)
                    dirlist.append(justDir)
                else:
                    dataSrcTruncatedDirs.append(os.path.basename(datasrc))
            dataSrcStr = ',+'.join(dataSrcTruncatedDirs)
            if len(dataSrcStr) > 180:
                dataSrcStr = dataSrcStr[0:175] + '...'
        return dataSrcStr

    def getDuration(self):
        '''
        Retrieves the script run duration for the dwm reporter
        :return: amount of time in seconds that the fmw script in question
                 ran for.  This value is to be used in the DWM record for this
                 run event.
        '''
        return self.fme.elapsedRunTime

    def getDestServiceName(self):
        '''
        Method that retrieves the destination service name that is to be
        used in the DWM reporter.
        :return: name of the service name to be used for the destination
        :rtype: str
        '''
        # retrieve from the published parameters:
        destServName = None
        if self.fme.macroValues.has_key(self.const.FMWParams_DestServiceName):  # @IgnorePep8
            destServName = self.fme.macroValues[
                self.const.FMWParams_DestServiceName]
        else:
            for macroKey in self.fme.macroValues:
                if macroKey.upper() == self.fme.macroValues[macroKey].upper():
                    destServName = self.fme.macroValues[macroKey]
                    break
        # the parameter destservicename is a scripted parameter so it looks
        # like the actual calculated value is unavailable to the shutdown
        # process so instead grabbing direct from the config file
        if not destServName:
            # destServName = self.params.getDestinationServiceName()
            destServName = self.config.getDestinationServiceName()
            if not destServName:
                msg = 'Unable to retrieve destination service name that ' + \
                      'gets used by the DWM reporter.  Without the service' + \
                      ' name the record will not appear in the DWM database.'
                self.logger.error(msg)
        return destServName

    def getDestSchema(self):
        '''
        :return: the destination schema as it it will appear in the dwm
                 reporting record.
        :rtype: str
        '''
        destSchema = self.params.getDestinationSchema()
        return destSchema

    def getDestTable(self):
        '''
        :return: Returns the destination table name that will  be put in
                 the dwm record.
        '''
        # again only captures the first feature class
        destTable = None
        matchExpr = '^{0}.*$'
        matchExpr = matchExpr.format(self.const.FMWParams_DestFeatPrefix)
        for macroKey in self.fme.macroValues.keys():
            if re.match(matchExpr, macroKey, re.IGNORECASE):
                destTable = self.fme.macroValues[macroKey]
                # TODO: Ideally should modify things so that we can report on more than one destination @IgnorePep8
                break
        return destTable

    def printParams(self):
        '''
        A utility method that prints out all the information nicely formatted
        that is going to be written to the DWM record
        '''
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


class DependenciesNotMet(Exception):
    '''
    This method is used by scripts that define dependencies.
    When a script that defined dependencies cannot locate them this error
    get triggered
    '''
    pass
