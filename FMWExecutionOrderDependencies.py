'''
Created on May 16, 2017

@author: kjnether
'''
import DataBCFMWTemplate
import DBCFMEConstants
import logging
import FMEUtil.PyFMEServerV2
import os.path
import pprint
import datetime


class ExecutionOrderConstants(object):
    # make sure the statuses are in upper case
    successStatus = ['SUCCESS']


class ExecutionOrder(object):
    '''
    This class was created to support the dependency functionality
    in the FME Framework.  The startup method will handle the retrieval
    of all the dependency parameters from either the default configuration
    or from published parameters.

    These parameter then get passed to this class.  This class is designed
    so it doesn't have to know anything about FME, published parameters or
    default configs.  All information required to run is sent to this
    class in the constructor
    '''

    def __init__(self, dependencyList, depTimeWindow, depMaxRetries, depWaitTime, fmeBaseUrl, fmeToken):
        '''

        This
            DEP_TIMEWINDOW
            DEP_WAITTIME
            DEP_MAXRETRIES

        '''

        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)

        self.const = DBCFMEConstants.TemplateConstants()
        self.execConst = ExecutionOrderConstants()

        # first retrieve the parameters:
        # self.params = DataBCFMWTemplate.GetPublishedParams(self.fme.macroValues)

        self.dependentFMW = dependencyList
        self.depStruct = {}
        self.fms = FMEUtil.PyFMEServerV2.FMEServer(fmeBaseUrl, fmeToken)

        self.depTimeWindow = depTimeWindow

        # convert the depTimeWindow to a timedelta object, and then
        # calculate the datetime back for that time delta
        self.timeBack = datetime.timedelta(seconds=self.depTimeWindow)
        self.nowBack = datetime.datetime.now() - self.timeBack

        self.jobsPerPage = 250
        self.noRepoString = 'norepo'

        # need to keep track of what fmw's have been observed,
        # only interested in the most recent status where
        # two runs have been found
        self.alreadySeen = {}

        # last thing to do here, leave at bottom, reads the
        # dependency data structure and formats it, populating
        # the property self.depStruct
        self.parseDependencies()

        '''
        Example job data struct:
        {   175847: {   u'description': u'',
                u'engineHost': u'arneb',
                u'engineName': u'ARNEB_Engine1',
                u'id': 175847,
                u'request': {   u'FMEDirectives': {   },
                                u'NMDirectives': {   u'failureTopics': [],
                                                     u'successTopics': []},
                                u'TMDirectives': {   u'description': u'',
                                                     u'priority': 100,
                                                     u'rtc': False,
                                                     u'tag': u''},
                                u'publishedParameters': [   {   u'name': u'DEST_SCHEMA',
                                                                u'raw': u'WHSE_FOREST_VEGETATION'},
                                                            {   u'name': u'DEST_DB_ENV_KEY',
                                                                u'raw': u'PRD'},
                                                            {   u'name': u'SRC_ORA_SERVICENAME_2',
                                                                u'raw': u'$(DEST_SERVICENAME)'},
                                                            {   u'name': u'FME_SECURITY_USER',
                                                                u'raw': u'dwrs'},
                                                            {   u'name': u'SRC_ORA_PROXY_SCHEMA',
                                                                u'raw': u'MSRM$SNP1'},
                                                            {   u'name': u'SRC_ORA_SCHEMA',
                                                                u'raw': u'GYS'},
                                                            {   u'name': u'FME_SECURITY_ROLES',
                                                                u'raw': u'fmeadmin fmesuperuser'},
                                                            {   u'name': u'SRC_FEATURE_2',
                                                                u'raw': u'GRY_PLOT_RADIUS_XREF'},
                                                            {   u'name': u'SRC_FEATURE_1',
                                                                u'raw': u'GYS_SAMPLE_REPLICATE_VW'},
                                                            {   u'name': u'SRC_HOST',
                                                                u'raw': u'nrk1-scan.bcgov'},
                                                            {   u'name': u'SRC_HOST_2',
                                                                u'raw': u'$(DEST_HOST)'},
                                                            {   u'name': u'SRC_ORA_SERVICENAME',
                                                                u'raw': u'dbp07.nrs.bcgov'},
                                                            {   u'name': u'SRC_ORA_SCHEMA_2',
                                                                u'raw': u'WHSE_FOREST_VEGETATION'},
                                                            {   u'name': u'DEST_FEATURE_1',
                                                                u'raw': u'GRY_PERMANENT_SAMPLE_PLOT_POLY'}],
                                u'subsection': u'SERVER_SCHEDULER',
                                u'workspacePath': u'"BCGW_SCHEDULED/gry_permanent_sample_plot_poly_dbp07_odb_bcgw/gry_permanent_sample_plot_poly_dbp07_odb_bcgw.fmw"'},
                u'result': {   u'id': 175847,
                               u'numFeaturesOutput': 7581,
                               u'priority': 100,
                               u'requesterHost': u'142.34.140.26',
                               u'requesterResultPort': -1,
                               u'status': u'SUCCESS',
                               u'statusMessage': u'Translation Successful',
                               u'timeFinished': u'2017-05-30T02:31:55',
                               u'timeRequested': u'2017-05-30T02:30:00',
                               u'timeStarted': u'2017-05-30T02:30:00'},
                u'status': u'SUCCESS',
                u'timeDelivered': u'2017-05-30T02:31:55',
                u'timeFinished': u'2017-05-30T02:31:55',
                u'timeQueued': u'2017-05-30T02:30:00',
                u'timeStarted': u'2017-05-30T02:30:00',
                u'timeSubmitted': u'2017-05-30T02:30:00'},
    175855: {   u'description': u'',
        '''

    def parseDependencies(self):
        '''
        Takes the dependencies and parses them out into a datastructure that will
        make it easy to do a comparison with.
        '''
        if not self.dependentFMW:
            msg = 'You specified the dependency published parameter, which kicks ' + \
                  'off the dependency checking functionality, however you did not ' + \
                  'define any FMW\'s in the parameter dependency parameter {0}'
            msg = msg.format(self.const.FMWParams_Deps_fmwList)
            self.logger.error(msg)
            raise KeyError, msg

        for dep in self.dependentFMW:
            depParamList = dep.split('/')
            self.logger.debug("depParamList: {0}".format(depParamList))
            if len(depParamList) > 2:
                msg = 'You specified a dependency in a format that is not supported. ' + \
                      'dependencies should be supplied a comma delimited string.  If ' + \
                      'you are specifying a repository name it should be delimited with ' + \
                      'a forward slash, example: repository/fmwName.fmw.  Your dependency ' + \
                      'list does not seem to comply: {0}'
                msg = msg.format(self.dependentFMW)
                self.logger.error(msg)
                raise ValueError, msg
            elif len(depParamList) == 2:
                self.logger.debug("found 2 params")
                repo = depParamList[0].strip()
                fmw = depParamList[1].strip()
            else:
                repo = self.noRepoString
                fmw = depParamList[0].strip()
            self.depStruct[repo.lower()] = {}
            self.depStruct[repo.lower()][fmw.lower()] = False

    def haveDependenciesBeenFound(self, repo, fmw, status):
        '''
        Iterates through the self.depStruct structure, updates
        the structure if the fmw sent as an arg is defined in this
        structure.

        Then iterates over the self.depStruct to determine if all
        the dependency requirements have been met and returns a boolean
        value if they have.

        is the fmw and repo described as a dependency, if so update
        the status of dependencies, then return a boolean value
        that indicates the status of all dependencies

        '''
        for depStructRepo in self.depStruct:
            repo2Use = None
            if depStructRepo == self.noRepoString:
                # ignore the repo and only search for the fmw
                repo2Use = self.noRepoString.lower()
            elif repo.lower() in self.depStruct:
                repo2Use = depStructRepo.lower()
            if repo2Use:
                if fmw.lower() in self.depStruct[repo2Use]:
                    self.logger.info("Job has been found current status: {0}".format(status))
                    if status.upper() in self.execConst.successStatus:
                        self.logger.info("fmw status has been marked as satisfied")
                        self.depStruct[repo2Use][fmw.lower()] = True

        retVal = True
        for depStructRepo in self.depStruct:
            for depStructFMW in self.depStruct[depStructRepo]:
                if not self.depStruct[depStructRepo][depStructFMW]:
                    msg = "dependency, repo: {0} fmw: {1} has not been satisfied"
                    msg = msg.format(repo, fmw)
                    self.logger.info(msg)
                    retVal = False
                    break
            if not retVal:
                break
        return retVal

    def hasJobBeenSeen(self, fmwJob):
        '''
        Keeps track of the fmw / repository combination in a specific job,
        and if the job implements the template, this is detected and distinguishes
        between the destenv keys, so a prd job is different from a dlv job.

        storing the jobs in self.alreadySeen property
        '''
        wsPath = fmwJob[self.const.FMEServerParams_request][self.const.FMEServerParams_wsPath]
        fmw, repo = self.parseFmwPath(wsPath)
        pubParams = fmwJob[self.const.FMEServerParams_request][self.const.FMEServerParams_pubParams]
        destEnvKey = 'NotDefined'
        jobsubmitTime = fmwJob[self.const.FMEServerParams_timeSubmitted]
        jobsubmitTime = datetime.datetime.strptime(jobsubmitTime, self.const.FMEServer_DatetimeFormat)
        alreadySeen = False
        jobStatus = fmwJob[self.const.FMEServerParams_status]
        # published parameters, checking for the dest env key, if it does not
        # exist then will use a dummy parameter for it
        for param in pubParams:
            if param['name'].upper() == self.const.FMWParams_DestKey:
                destEnvKey = param['raw'].upper()
                break

        # if the repo is not in the dictionary then add it.
        if repo in self.alreadySeen:
            self.logger.debug("the repo: {0} has already been seen".format(repo))
        else:
            self.alreadySeen[repo] = {}

        # if the fmw is not in the dictionary then add it.
        if fmw in self.alreadySeen[repo]:
            self.logger.debug("the fmw {0} has already been seen".format(fmw))
        else:
            self.alreadySeen[repo][fmw] = {}

        # if the destEnv key is in the dictionary, then check the submit times.
        # if the current job has a submit time greater than the cached time
        # then use that time.
        if destEnvKey in self.alreadySeen[repo][fmw]:
            self.logger.debug("dest env key: {0} already seen".format(destEnvKey))
            msg = "current job time: {0} cached time: {1}".format(jobsubmitTime, self.alreadySeen[repo][fmw][destEnvKey])
            self.logger.debug(msg)
            # finally check to see if this job has been executed since the last one
            if jobsubmitTime >= self.alreadySeen[repo][fmw][destEnvKey]:
                alreadySeen = True
                self.alreadySeen[repo][fmw][destEnvKey] = jobsubmitTime
        else:
            self.alreadySeen[repo][fmw][destEnvKey] = jobsubmitTime
        if not alreadySeen:
            self.logger.debug("the fmw {0}/{1} has NOT already been seen".format(fmw, repo))
        return alreadySeen

    def checkForDependencies(self):
        '''
        Issues the default getJobs command, continues to get jobs until
        either all the deps are found or the age of the jobs exceeds the
        depTimeWindow parameter.

        depTimeWindow is number of seconds back in time, code converts this
        to a date object for comparison.

        '''
        pp = pprint.PrettyPrinter(indent=4)
        self.logger.debug("getting jobs")
        jobObj = self.fms.getJobs()
        jobs = jobObj.getJobs(limit=self.jobsPerPage, detail='high')
        # pp.pprint(jobs)
        getAnotherPage = True
        dependenciesSatisfied = False

        while getAnotherPage:
            jobKeys = jobs.keys()
            self.logger.debug("{0} is the max {1} is the min".format(max(jobKeys), min(jobKeys)))
            jobKeys.sort(reverse=True)
            for job in jobKeys:

                # only proceed if the fmw / repo associated with the job has not
                # already been seen
                if not self.hasJobBeenSeen(jobs[job]):
                    self.logger.debug("job object: {0}".format(job))
                    # example of string format from FME Server:
                    #     u'timeFinished': u'2017-05-26T01:34:02',
                    jobDateSubmitted = jobs[job][self.const.FMEServerParams_timeSubmitted]
                    dateFormat = self.const.FMEServer_DatetimeFormat
                    jobSubmitDate = datetime.datetime.strptime(jobDateSubmitted, dateFormat)
                    # this will be in the format of repository name / fmw no suffix dir / fmw.fmw
                    fmwPath = jobs[job][self.const.FMEServerParams_request][self.const.FMEServerParams_wsPath]
                    fmw, repo = self.parseFmwPath(fmwPath)
                    self.logger.debug("fmw: {0} repo: {1}".format(fmw, repo))
                    status = jobs[job][self.const.FMEServerParams_result][self.const.FMEServerParams_status]
                    self.logger.debug("job status: {0}".format(status))
                    if self.haveDependenciesBeenFound(repo, fmw, status):
                        # dependencies found and satisfied proceed
                        msg = 'The dependencies have been satisfied can now proceed'
                        self.logger.info(msg)
                        getAnotherPage = False
                        dependenciesSatisfied = True
                        break
                    if jobSubmitDate < self.nowBack:
                        msg = 'Retrieved jobs up to the specified time window of {0} seconds'
                        self.logger.info(msg.format(self.depTimeWindow))
                        getAnotherPage = False
                        break
            if getAnotherPage:
                self.logger.debug("getting another page of fme server jobs...")
                offset = len(jobKeys) - 1
                jobs = jobObj.getJobs(offset=offset, limit=self.jobsPerPage, detail='high')
        return dependenciesSatisfied

    def parseFmwPath(self, fmwPath):
        fmw = os.path.basename(fmwPath)
        repo = os.path.dirname(os.path.dirname(fmwPath))
        # can't have quotes in fmw name
        fmw = fmw.replace("'", '').replace('"', '')
        repo = repo.replace("'", '').replace('"', '')
        return fmw, repo

    def isParentsComplete(self):
        depsSatisfied = self.checkForDependencies()
        return depsSatisfied

