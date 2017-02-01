"""
About
=========
:synopsis:     Automates the update of FMW files on FME Server 
:moduleauthor: Kevin Netherton
:date:         10-18-2016
:description:  We are embarking on the process of updating our
               fme server based replications from version 1 of
               our template system to version 2.  This process 
               will see each fmw script get converted manually 
               and then we need to:
                 0. upload the new version of the job to dummy 
                    repository and verify that it works in a test
                    replication to DLV
                 1. get the cron schedule for the current schedule
                    associated with the existing job 
                 2. delete the jobs schedule
                 3. delete the existing job (in most cases this 
                    will be the version that exists in the fme 
                    server repository BCGW_REP_SCHEDULED
                 4. upload the new version to, in most cases
                    BCGW_SCHEDULED
                 5. recreate the schedule.
   

Inputs / Outputs:
-------------------
In summary each set of uploads will create the following directory 
structures.  The intent of this is so that the script can recover
if it crashes and that no data is lost in that process.

The root directory will be called $PROJDIR

$PROJDIR/.      - This is the list of updated fmws that should be 
                  uploaded to FME Server.
                  
$PROJDIR/tests - If script has been uploaded to a dummy server, and 
                  test run successfully there will be a file in here 
                  called <fmw file name>.tested
                  
                  Its just a placeholder, will be an empty file.
                  
$PROJDIR/crons  - When the cron strings are harvested from fme server
                  they will be written to this directory.  The crons
                  will go into a separate file with the name of the 
                  fme script with a .cron suffix.
                  
                  if the cron file exists in this directory for a fmw 
                  script then the script will not try to get it from 
                  fme server.
                  
$PROJDIR/backup - When the script runs before it deletes the existing 
                  version of a script from FME server it will copy it 
                  down to this directory.
                  
$PROJDIR/status - This is a folder that is used to keep track of what 
                  has and has not been done with respect to the conversion
                       
Dependencies:
-------------------
 - This script is part of the fme template. Dependencies are managed at 
   the project level.
 - Does make heavy use of the DataBCLib module FMEUtil.PyFMEServerV2

API DOC:
===============     
"""

import DataBCFMWTemplate
import os.path
import FMEUtil.PyFMEServerV2  # @UnresolvedImport
import logging
import pprint
import pickle
import datetime
import sys

class Params(object):
    
    def __init__(self):
        '''
        Get the secrets from the config file
        '''
        self.const = DataBCFMWTemplate.TemplateConstants
        self.configFile = self.calcConfigFileAbsPath()
        print 'self.configFile', self.configFile
        self.configRdr = DataBCFMWTemplate.TemplateConfigFileReader('DLV',self.configFile) 
        self.configRdr.readConfigFile()
        self.fmeServerHost = self.configRdr.getFMEServerHost()
        self.fmeServerRootDir = self.configRdr.getFMEServerRootDir()
        self.fmeServerToken = self.configRdr.getFMEServerToken()
        
        self.currentFMWRepository = 'BCGW_REP_SCHEDULED'
        self.destinationFMWRepository = 'BCGW_SCHEDULED'
        self.srcTemplate2FMWDirectory = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\templateImplementation\BCGW_REP_SCHEDULED'
        
    def calcConfigFileAbsPath(self):
        curdir = os.path.dirname(__file__)
        rootdir = os.path.join(curdir, '..')
        configFile = os.path.join(rootdir, 
                                  self.const.AppConfigConfigDir,
                                  self.const.AppConfigFileName)
        configFile = os.path.realpath(configFile)
        return configFile
    
class T1T2ConversionConstants(object):
    FMWTestRepo = 'TemplateV2_TEST'

    crondir = 'crons'
    cronCacheSuffix = '.cron'
    
    schedsPickleFile = 'scheds.pcl'
    
    FMEArchiveDir = 'backup'
    
    statusTestDir = 'tests'
    statusTestSuffix = 'tested'
    
    statusFMWUpdtDir = 'status'
    statusFMWUpdtRepoSuffix = 'updtrepo'
    statusFMWUpdtSchedSuffix = 'updtschd'

class FMEServerInteraction(T1T2ConversionConstants):
    def __init__(self,  fmwDir=None, fmwServCurRepo=None, fmwDestRepo=None):
        T1T2ConversionConstants.__init__(self, )
        # set up the logging
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger()
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(lineno)d -  %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug("first message")
        
        self.fmwServCurRepo = fmwServCurRepo
        self.fmwDestRepo = fmwDestRepo
        
        # Will get populated with a Schedules objects (below) if 
        # we actually need to retrieve the schedule
        self.FMESchedule = None
        
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        #self.logger = logging.getLogger(modDotClass)
        self.params = Params()

        if fmwDir:
            self.params.srcTemplate2FMWDirectory = fmwDir
        if fmwServCurRepo:
            self.params.currentFMWRepository = fmwServCurRepo
        if fmwDestRepo:
            self.params.destinationFMWRepository = fmwDestRepo
        
        self.fmeServer = FMEUtil.PyFMEServerV2.FMEServer(
                            self.params.fmeServerHost,
                            self.params.fmeServerToken)
        
    def iterator(self):
        '''
        contains the code that is going to be implemented on 
        each fmw.
        '''
        fmwFiles = self.getFMWs()
        
        self.logger.debug("")
        scheds = Schedules(self.fmeServer)
        for fmwFile in fmwFiles:
            # for each fmw file,
            #  a) verify that their is a cron
            #  b) upload to a test repo and try to run to delivery, if not success then stop
            #  c) upload to new repository
            #  d) copy existing fmw in old repo to a backup folder
            #  e) delete the fmw from fme server
            #  f) delete the schedule
            #  g) recreate the schedule
            msg = "current fmw is {0}".format(fmwFile)
            self.logger.debug(msg)
            #TODO: uncomment when rest is working...  (Coding is complete)
            self.testRun(fmwFile)
            # get the cron string, at same time its cached if it hasn't already 
            # been done
            cronStr, category, schdName = self.getCronAndCategoryAndScheduleName(fmwFile, self.fmwServCurRepo)
            # now archive the existing one
            self.archiveExistingFMW(fmwFile, self.fmwServCurRepo)

            self.updateFMW(fmwFile, self.fmwServCurRepo, self.fmwDestRepo)
            self.updateSchedule(fmwFile, self.fmwServCurRepo, self.fmwDestRepo, cronStr, category, schdName)
        
    def updateSchedule(self, fmwFileFullPath, fmwServCurRepo, fmwDestRepo, cronStr, category, schedName):
        '''
        This method will find the existing schdule in FME server, remove that 
        schedule, and replace with the same schedule that points to the new 
        updated FMW file
        
        :param  fmwFileFullPath: The full path to the fmw file that is to replace the
                                 existing one on FME Server
        :type fmwFileFullPath: str (path)
        :param  fmwServCurRepo: The name of the FME Server repository that the fmw job
                                was originally located in.  The repository that contains 
                                DataBC template version 1.
        :type fmwServCurRepo: str
        :param  fmwDestRepo: The name of the FME Server destination repository.  This is 
                             the repository that uses the version 2 of the databc fmw
                             template.
        :type fmwDestRepo: str
        :param  cronStr: The cron string that is used to describe the timing for the 
                         schedule.
        :type cronStr: str(quartz cron  str)
        :param  category: The category of the schedule that is to be re-created
        :type category: str
        '''
        # steps:
        # 
        #  1. get the published parameters for the current fmw
        #  2. use the published parameters for the current fmw to create a schedule
        #  2.5. cache the schedule description object
        #  3. Find the existing schedule and delete it
        #  4. Create the new schedule.
        fmwFile = os.path.basename(fmwFileFullPath)
        
        # getting published parameters
        repo = self.fmeServer.getRepository()
        wrkSpcs = repo.getWorkspaces(fmwDestRepo)
        pubParams = wrkSpcs.getPublishedParams(fmwFile)
        
        # status file, used to indicate whether this work has been completed yet
        statusDir = os.path.join(os.path.dirname(fmwFileFullPath), self.statusFMWUpdtDir)
        nosuffix, suffix = os.path.splitext(fmwFile)
        statusFile = "{0}.{1}".format(nosuffix, self.statusFMWUpdtSchedSuffix)
        statusFileFullPath = os.path.join(statusDir, statusFile)
        self.logger.debug("status file {0} used to indicate if fmw schedule has been updated".format(statusFileFullPath))
        
        if not os.path.exists(statusFileFullPath):
            if not os.path.exists(statusDir):
                os.mkdir(statusDir)
            self.logger.debug("about the update the schedule for the fmw {0}".format(fmwFile))
            # formatting the published parameters into the structure
            # expected by fme server.
            self.logger.debug("getting the published parameters for the fmw job")
            pubParamsForSchedule = []
            for param in pubParams:
                tmpParam = {}
                tmpParam['name'] = param['name']
                tmpParam['value'] = param['defaultValue']
                if param['name'].upper() == 'DEST_DB_ENV_KEY':
                    self.logger.debug("DEST_DB_ENV_KEY parameter is described: {0}".format(param))
                    if param['model'].lower() == 'string':
                         tmpParam['name'] = param['name']
                         tmpParam['value'] = 'PRD'
                    else:
                        tmpParam['name'] = param['name']
                        tmpParam['value'] = ['PRD']
                pubParamsForSchedule.append(tmpParam)
            self.logger.debug("params for schedule: {0}".format(pubParamsForSchedule))
                
            # setting up a schedule description
            schedStruct = {}
            schedStruct['category'] = category
            schedStruct['recurrence'] = 'cron'
            now = datetime.datetime.now()
            beginStr = "{0}T00:00:00".format(now.strftime('%Y-%m-%d'))
            # example string: '2016-10-19T00:00:00'
            schedStruct['begin'] = beginStr
            schedStruct['name'] = schedName
            schedStruct['repository'] = fmwDestRepo
            schedStruct['request'] = {'publishedParameters':pubParamsForSchedule}
            schedStruct['enabled'] = True
            schedStruct['cron'] = cronStr
            schedStruct['workspace'] = fmwFile
            schedStruct['name'] = schedName
            
            # set up a schedule object
            scheds = self.fmeServer.getSchedules()
            sched = scheds.getSchedule()
            
            # check to see if the schedule exists
            if scheds.exists(schedName, category):
                # now delete the existing schedule
                self.logger.debug("removing the schedule {0} with the category {1}".format(schedName, category))
                sched.delete(category, schedName)
                self.logger.debug("schedule {0} has been removed".format(schedName))
            
            # finally create the schedule
            self.logger.debug("recreating the schedule to refer to the new fmw ")
            sched.addSchedule(schedStruct)
            self.logger.debug("schedule creation completed")
            
            # creating the status file, used to indicate that this task has been completed
            fh = open(statusFileFullPath, 'w')
            fh.close()
            self.logger.debug("placeholder has been created {0}".format(statusFileFullPath))
        else:
            msg = 'placehlder already exists {0}'.format(statusFileFullPath)
            self.logger.debug(msg)
            
    def updateFMW(self, fmwFileFullPath, FMEServRepoName_current, FMEServRepoName_dest):
        '''
        Will remove the current fmw that exists on fme server and replace it 
        with the new version.  
        
        :param  fmwFileFullPath: The full path and name of the fmw file that is 
                                 to replace the existing version on fme server
        :type fmwFileFullPath: str(path)
        :param  FMEServRepoName_current: The name of the fme server repository that
                                         the job is currently located in
        :type FMEServRepoName_current: str
        :param  FMEServRepoName_dest: The name of the fme server repository that 
                                      the new version of the fme script will be
                                      located in.
        :type FMEServRepoName_dest: str
        '''
        fmwFile = os.path.basename(fmwFileFullPath)
        repo = self.fmeServer.getRepository()
        wrkSpcsCurrent = repo.getWorkspaces(FMEServRepoName_current)
        wrkSpcsNew = repo.getWorkspaces(FMEServRepoName_dest)
        
        statusDir = os.path.join(os.path.dirname(fmwFileFullPath), self.statusFMWUpdtDir)
        nosuffix, suffix = os.path.splitext(fmwFile)
        statusFile = "{0}.{1}".format(nosuffix, self.statusFMWUpdtRepoSuffix)
        statusFileFullPath = os.path.join(statusDir, statusFile)
        self.logger.debug("status file {0} used to indicate if fmw has been updated".format(statusFileFullPath))
        
        if not os.path.exists(statusFileFullPath):
            if not os.path.exists(statusDir):
                os.mkdir(statusDir)
            # remove existing job
            if not wrkSpcsCurrent.exists(fmwFile):
                msg = 'The workspace {0} does not exist in the repository {1}. ' +\
                      'Raising a warning here.  Might have been replaced earlier'
                msg = msg.format(fmwFile, FMEServRepoName_current)
                self.logger.warning(msg)
            else:
                # delete it
                msg = "removing the fmw {0} from the repo {1} on fme server"
                msg = msg.format(fmwFile, FMEServRepoName_current)
                self.logger.debug(msg)
                wrkSpcsCurrent.deleteWorkspace(fmwFile)
                self.logger.debug("{0} is now deleted!".format(fmwFile))
            if wrkSpcsNew.exists(fmwFile):
                msg = 'The fmw job ({0}) already exists in the repository ({1}), ' + \
                      'assuming that the job has already been copied'
                msg = msg.format(fmwFile, FMEServRepoName_dest)
                self.logger.warning(msg)
                # TODO: could do an update of the fmw here.  Leaving it for now
                #       will see how it goes
            else:
                msg = "copying the script {0} to the repository {1}"
                msg = msg.format(fmwFile, FMEServRepoName_dest)
                self.logger.debug(msg)
                repo.copy2Repository(FMEServRepoName_dest, fmwFileFullPath)
                wrkSpcsNew.registerWithJobSubmitter(fmwFile)
                self.logger.debug("script is now copied")
                
            # creating the file placeholder that is used to identify quicklywhether this 
            # step has been completed
            self.logger.debug("creating the placeholder to indicate the fmw's are up to date {0}".format(statusFileFullPath))
            fh = open(statusFileFullPath, 'w')
            fh.close()
            self.logger.debug("placeholder created")
        else:
            msg = 'placeholder file exists {0} indicating the fmw has already been updated.'
            msg = msg.format(statusFileFullPath)
            self.logger.debug(msg)
            
    def archiveExistingFMW(self, fmwFileFullPath, repository):
        '''
        Gets the name of an fmwFile and a repository. Cleans the 
        fmw path leaving just the name of the file. Checks to verify
        that the fmw file exists in the given repository.  If it does
        then it is downloaded into an archive directory.
        
        :param  fmwFile: The full path and name of the fmw file that is 
                         going to replace the existing on on fme server
        :type fmwFile: str(path)
        :param  repository: The repository name that the fmw job is currently
                            located in on FME Server
        :type repository: str
        '''
        fmwFile = os.path.basename(fmwFileFullPath)
        archiveDir = os.path.join(os.path.dirname(fmwFileFullPath), self.FMEArchiveDir)
        archiveFile = os.path.join(archiveDir, fmwFile)
        msg = 'going to archive the existing fmw in fme server to the path {0}'
        msg = msg.format(archiveFile)
        self.logger.debug(archiveFile)
        if not os.path.exists(archiveFile):
            msg = "the fmw {0} has not been archived!"
            msg = msg.format(fmwFile)
            self.logger.debug(msg)
            if not os.path.exists(archiveDir):
                os.mkdir(archiveDir)
            repo = self.fmeServer.getRepository()
            wrkSpcs = repo.getWorkspaces(repository)
            if not wrkSpcs.exists(fmwFile):
                msg = 'The fmw file {0} does not exist in the repository {1}'
                msg = msg.format(fmwFile,repository)
                raise ValueError, msg
            msg = "downloading the fmw file {0} to {1}"
            msg = msg.format(fmwFile, archiveFile )
            self.logger.debug(msg)
            wrkSpcs.downloadWorkspace(fmwFile, archiveFile)
            self.logger.debug("the fmw file {0} was successfully archived".format(fmwFile))
        else:
            self.logger.info("The fmw file {0} was already archived".format(fmwFile))        
                
    def getCronAndCategoryAndScheduleName(self, fmwFileFullPath, fmwRepo):
        '''
        Gets the name of an fmwFile, then 
        
        :param  fmwFile: param description
        :type fmwFile: enter type
        :param  fmwRepo: param description
        :type fmwRepo: enter type
        '''
        cronDir = os.path.join(os.path.dirname(fmwFileFullPath), self.crondir)
        self.logger.debug("cache for cron and other info directory {0}".format(cronDir))
        if not os.path.exists(cronDir):
            os.mkdir(cronDir)
        justFMWNoExt, ext = os.path.splitext(os.path.basename(fmwFileFullPath))
        justFMWNoExt = '{0}.{1}'.format(justFMWNoExt, self.cronCacheSuffix)
        cronCacheFile = os.path.join(cronDir, justFMWNoExt)
        
        cronStr = None
        
        if os.path.exists(cronCacheFile):
            msg = "retrieving information from cached cron file {0}"
            msg = msg.format(cronCacheFile)
            self.logger.debug(msg)
            # file exists, just retrieve the cron string from the file
            fh = open(cronCacheFile, 'r')
            cronStr = fh.readline()
            cronStr = cronStr.strip()
            category = fh.readline()
            category = category.strip()
            schedName = fh.readline()
            schedName = schedName.strip()
            fh.close()
        else:
            msg = 'getting the cron and other info from fme server. ' + \
                  'This query takes about 2-3 minutes...'
            self.logger.debug(msg)

            # cached cron does not exist. so getting the schedule
            # and then caching it
            sched = self.__getSchedule(cronDir)
            cronStr = sched.getFMWCronSchedule(fmwFileFullPath, fmwRepo)
            cronStr = cronStr.strip()
            
            category = sched.getFMWScheduleCategory(fmwFileFullPath, fmwRepo)
            category = category.strip()
            
            schedName = sched.getFMWScheduleName(fmwFileFullPath, fmwRepo)
            schedName = schedName.strip()
            
            # got the cron string now cache it, and at the same time the 
            # schedule catagory
            fh = open(cronCacheFile, 'w')
            fh.write(cronStr + '\n')
            fh.write(category + '\n')
            fh.write(schedName + '\n')
            fh.close()
        self.logger.debug("params retrieved by this method:")
        self.logger.debug("cronStr: {0}".format(cronStr))
        self.logger.debug("category: {0}".format(category))
        self.logger.debug("schedName: {0}".format(schedName))

        return cronStr, category, schedName
    
    def __getSchedule(self, scheduleCachePickleDir):
        if not self.FMESchedule:
            self.FMESchedule = Schedules(self.fmeServer, pickleDir=scheduleCachePickleDir)
        return self.FMESchedule
    
    def testRun(self, fmwFileFullPath, refresh=False):
        fmwFile = os.path.basename(fmwFileFullPath)
        repo = self.fmeServer.getRepository()
        descr = r'temporary repository used for testing template v2 scripts'
        
        # using this file as a placeholder to keep track of whether the tests
        # have been run on a particular fmw.
        testRunDir = os.path.join(os.path.dirname(fmwFileFullPath), self.statusTestDir)
        noSuffix, suffix = os.path.splitext(fmwFile)
        testFile = '{0}.{1}'.format(noSuffix, self.statusTestSuffix)
        testFileFullPath = os.path.join(testRunDir, testFile)
        self.logger.debug("testFileFullPath: {0}".format(testFileFullPath))
        
        if refresh:
            if os.path.exists(testFileFullPath):
                os.remove(testFileFullPath)
                msg = "refresh was set to true so the placeholder file is being deleted {0}"
                msg = msg.format(testFileFullPath)
                self.logger.info(msg)
        
        if not os.path.exists(testFileFullPath):
            msg = 'The placholder file {0} that is used to indicate whether this test ' + \
                  'has already been run does not exist.  Proceeding with loading the ' + \
                  'fmw {1} to fme server for testing'
            msg = msg.format(testFileFullPath, fmwFile)
            self.logger.debug(msg)
            # first make the test dir if it does not already exist
            if not os.path.exists(testRunDir):
                os.mkdir(testRunDir)
            if not repo.exists(self.FMWTestRepo):
                repo.create(self.FMWTestRepo, descr)
                msg = "creating the test repository {0}".format(self.FMWTestRepo)
                self.logger.debug(msg)
            wrkspcs = repo.getWorkspaces(self.FMWTestRepo)
            if wrkspcs.exists(fmwFile):
                #wrkspcs.deleteWorkspace(fmwFile)
                repo.updateRepository(self.FMWTestRepo, fmwFileFullPath)
                wrkspcs.registerWithJobSubmitter(fmwFile)
                msg = 'The fmw {0} has been updated on fme server in the test repo {1}'
                msg = msg.format(fmwFile, self.FMWTestRepo)
                self.logger.debug(msg)
            elif not wrkspcs.exists(fmwFile):
                repo.copy2Repository(self.FMWTestRepo, fmwFileFullPath)
                wrkspcs.registerWithJobSubmitter(fmwFile)
                msg = 'The fmw {0} has been copied to fme server in the test repo {1}'
                msg = msg.format(fmwFile, self.FMWTestRepo)
                self.logger.debug(msg)
            # try to run it
            jobs  = self.fmeServer.getJobs()
            self.logger.debug("sending the job {0} to fme server".format(fmwFile))
            response = jobs.submitJob(self.FMWTestRepo, fmwFile, sync=True)
            self.logger.debug("job has completed")
            #print 'response', response
            if response['status'].upper() <> 'SUCCESS':
                msg = 'Attempted to run {0}.{1} and received a status {2}. ' + \
                      'Complete response {3}'
                msg = msg.format(self.FMWTestRepo, fmwFile, response['status'], response)
                raise ValueError, msg
            msg = "test has been completed, creating the placeholder file {0}"
            self.logger.info(msg.format(testFileFullPath))
            fh = open(testFileFullPath, 'w')
            fh.close()
            self.logger.debug("testing completed")
    
    def getFMWs(self):
        '''
        goes to the source directory for a list of fmws' to process
        '''
        if not os.path.isdir(self.params.srcTemplate2FMWDirectory):
            msg = 'The source directory {0} does not exist'
            self.logger.error(msg.format(self.params.srcTemplate2FMWDirectory))
            raise ValueError, msg.format(self.params.srcTemplate2FMWDirectory)
        
        allFiles = os.listdir(self.params.srcTemplate2FMWDirectory)
        print allFiles
        fmwFiles = []

        for curFile in allFiles:
            junk, ext = os.path.splitext(curFile)
            if ext.lower() == '.fmw':
                fmwFiles.append(os.path.join(self.params.srcTemplate2FMWDirectory, curFile))
        
        return fmwFiles
    
class Schedules(T1T2ConversionConstants):
    '''
    An interface to FME Schedules.  When schedules are returned
    to us using high detail this is the structure of the returned 
    object:
    
     {   u'begin': u'2010-11-10T18:15:00',
        u'category': u'HDMS',
        u'cron': u'0 15 18 ? * 2,3,4,5,6',
        u'description': u'',
        u'enabled': True,
        u'name': u'wq_wqo_rpt_index_sp_staging_gdb_bcgw.PROD',
        u'recurrence': u'cron',
        u'repository': u'BCGW_REP_SCHEDULED',
        u'request': {   u'FMEDirectives': {   },
                        u'NMDirectives': {   u'failureTopics': [],
                                             u'successTopics': []},
                        u'TMDirectives': {   u'priority': 100},
                        u'publishedParameters': [   {   u'name': u'User_ID',
                                                        u'value': u'whse_water_management'},
                                                    {   u'name': u'DestFeature1',
                                                        u'value': u'WQ_WQO_RPT_INDEX_SP'},
                                                    {   u'name': u'Dest_Instance',
                                                        u'value': u'bcgw.bcgov'},
                                                    {   u'name': u'SourceDataset',
                                                        u'value': u'\\\\data.bcgov\\data_staging\\BCGW\\fresh_water_and_marine\\WaterQualityObjectivesReports.gdb'},
                                                    {   u'name': u'Dest_Password',
                                                        u'value': u'wm4whse'},
                                                    {   u'name': u'Dest_Instance_Connect',
                                                        u'value': u'port:5153'},
                                                    {   u'name': u'Dest_Server',
                                                        u'value': u'bcgw.bcgov'}],
                        u'subsection': u'REST_SERVICE',
                        u'workspacePath': u'BCGW_REP_SCHEDULED/wq_wqo_rpt_index_sp_staging_gdb_bcgw/wq_wqo_rpt_index_sp_staging_gdb_bcgw.fmw'},
        u'workspace': u'wq_wqo_rpt_index_sp_staging_gdb_bcgw.fmw'}
    
    :ivar cachedSchedFile: Describe the variable here!
    :ivar fmeServer: Describe the variable here!
    :ivar logger: Describe the variable here!
    :ivar refresh: Describe the variable here!
    :ivar scheds: Describe the variable here!
    '''
    # TODO: once This script is complete it should be getting the schedule
    #       from fme server and not using the cached version.  Cache is there
    #       because the schedule retrieval takes so long
    def __init__(self, fmeServer, pickleDir=None, refresh=False):
        T1T2ConversionConstants.__init__(self)
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.scheds = None
        self.fmeServer = fmeServer
        self.refresh = refresh
        if not pickleDir:
            pickleDir = os.path.join(os.path.dirname(__file__))
        self.cachedSchedFile = os.path.join(pickleDir, self.schedsPickleFile)
            
    def getFMWCronSchedule(self, fmwPath, fmeRepository):
        fmwFile = os.path.basename(fmwPath)
        sched = self.getStruct(fmwPath, fmeRepository)
        retCron = None
        if sched:
            retCron = sched['cron']
        else:
            msg = 'There is no schedule currently defined for the fmw {0} in the schedulel {1}'
            raise ValueError, msg.format(fmwFile, fmeRepository)
        return retCron
    
    def getStruct(self, fmwPath, fmeRepository):
        '''
        Does a search of the schedule data and retrieves the full data structure 
        if the schedule for the given fmw / repository
        
        :param  fmwPath: The full path to the fmw who's schedule information
                         you want to retrieve.  Can be just the name of the
                         the fmw, but the full path will work also.
        :type fmwPath: str(path)
        :param  fmeRepository: The name of the repository that the fmw that is 
                               referenced by the schedule is in.
        :type fmeRepository: str
        '''
        scheds = self.__getSchedules()
        print 'scheds:', scheds
        justFMWFile = os.path.basename(fmwPath)
        retStruct = {}
        for sched in scheds:
            if sched['workspace'].lower() == justFMWFile.lower():
                # match now check repo
                if sched['repository'].lower() == fmeRepository.lower():
                    # bingo get the cron and return it
                    retStruct = sched
                    break
        return retStruct
    
    def getFMWScheduleCategory(self, fmwFullPath, fmeRepository):
        fmwFile = os.path.basename(fmwFullPath)
        sched = self.getStruct(fmwFullPath, fmeRepository)
        retCat = None
        if sched:
            retCat = sched['category']
        else:
            msg = 'There is no schedule currently defined for the fmw {0} in the schedulel {1}'
            raise ValueError, msg.format(fmwFile, fmeRepository)
        return retCat
    
    def getFMWScheduleName(self, fmwFullPath, fmeRepository):
        fmwFile = os.path.basename(fmwFullPath)
        sched = self.getStruct(fmwFullPath, fmeRepository)
        retCat = None
        if sched:
            retCat = sched['name']
        else:
            msg = 'There is no schedule currently defined for the fmw {0} in the schedulel {1}'
            raise ValueError, msg.format(fmwFile, fmeRepository)
        return retCat
            
    def __getSchedules(self):
        if not self.scheds:
            if self.refresh:
                self.scheds = self.__getSchedsFromFmeServer()
            else:
                self.scheds = self.__getSchedulesFromCacheFile()
        return self.scheds
    
    def __getSchedulesFromCacheFile(self):
        scheds = None
        if not self.refresh and not os.path.exists(self.cachedSchedFile):
            msg = "have to get the data from fme server as the cache file {0} " + \
                  'does not exist'
            msg = msg.format(self.cachedSchedFile)
            self.logger.info(msg)
            scheds = self.__getSchedsFromFmeServer()
        else:
            msg = "getting the schedule from the pickle cache file {0}"
            msg = msg.format(self.cachedSchedFile)
            self.logger.debug(msg)
            scheds = pickle.load( open( self.cachedSchedFile, 'rb' ) )
        return scheds
            
    def __getSchedsFromFmeServer(self):
        self.logger.info("going to fme server to load the schedules to memory, this will take about 2-3 minutes...")
        fmeSrvrSched = self.fmeServer.getSchedules()
        scheds = fmeSrvrSched.getSchedules()
        self.logger.info("schedule has been loaded")
        if os.path.exists(self.cachedSchedFile):
            os.remove(self.cachedSchedFile)
        msg = 'Caching the schedule information in the pickle file {0}'
        msg = msg.format(self.cachedSchedFile)
        self.logger.debug(msg)
        pickle.dump( scheds, open( self.cachedSchedFile, "wb" ) )
        return scheds
            
if __name__ == '__main__':
    sourceDir = r'\\data.bcgov\work\Projects\FYE2017\DWACT-497_RMP_WHSE\fmws\converted\TEST'
    currentFMEServerRepo = 'BCGW_REP_SCHEDULED'
    destinationFMEServerRepo = 'BCGW_SCHEDULED'
    srvr = FMEServerInteraction(fmwDir=sourceDir, fmwServCurRepo=currentFMEServerRepo, fmwDestRepo=destinationFMEServerRepo)
    srvr.iterator()
    
    
        
        
        
        
        
        
        