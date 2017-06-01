'''
Created on May 25, 2017

@author: kjnether
'''
import unittest
import FMWExecutionOrderDependencies
import DataBCFMWTemplate
import json
import logging
import datetime

class Test_FMWExecutionOrder(unittest.TestCase):


    def setUp(self):
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)
        self.logger = logger
        #self.execOrder = FMWExecutionOrderDependencies.ExecutionOrder()
        #macroFile = r'./testData/fmeMacros4Tests.json'
        #with open(macroFile) as data_file:    
        #    fmeMacros = json.load(data_file)
        depList = ['test1.fmw', 'test2.fmw']
        depWin = 14400 # 4 hours
        depMaxRetries = 2
        depWaitTime = 5
        fmeBaseUrl = 'http://arneb.dmz'
        
        config = DataBCFMWTemplate.TemplateConfigFileReader('DLV')
        fmeHost = config.getFMEServerHost()
        fmeToken = config.getFMEServerToken()
        
        self.execOrder = FMWExecutionOrderDependencies.ExecutionOrder(depList, depWin, depMaxRetries, depWaitTime, fmeHost, fmeToken)
        # put in a test to verify that fails when a fmw is called that is incorrect
        self.jobData = {    u'description': u'',
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
                                            u'publishedParameters': [   {   u'name': u'SRC_DATASET_FGDB_1',
                                                                            u'raw': u'\\\\server\\share\\dir\\anotherdir\\agdb.gdb'},
                                                                        {   u'name': u'FME_SECURITY_ROLES',
                                                                            u'raw': u'fmeadmin fmesuperuser'},
                                                                        {   u'name': u'DEST_DB_ENV_KEY',
                                                                            u'raw': u'PRD'},
                                                                        {   u'name': u'DEST_SCHEMA',
                                                                            u'raw': u'whse_some_schema'},
                                                                        {   u'name': u'FILE_CHANGE_DETECTION',
                                                                            u'raw': u'TRUE'},
                                                                        {   u'name': u'SRC_FEATURE_1',
                                                                            u'raw': u'ALR_Arcs'},
                                                                        {   u'name': u'DEST_FEATURE_1',
                                                                            u'raw': u'OATS_ALR_BOUNDARY_LINES'}],
                                            u'subsection': u'REST_SERVICE',
                                            u'workspacePath': u'"REPO/fmwPath/fmwPath.fmw"'},
                            u'result': {   u'id': 175323,
                                           u'numFeaturesOutput': 53925,
                                           u'priority': 100,
                                           u'requesterHost': u'99.99.99.99',
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
                            u'timeSubmitted': u'2017-05-29T01:30:00'}
        
    def tearDown(self):
        pass

    def test_checkForDependencies(self):
        # another possible job to use in a test
        # DWRSPUB/fme_logger/fme_logger.fmw
        #
        #fme_logger.fmw" repo: "DWRSPUB
        #depList = ['BCGW_REP_OTHER/prot_current_fire_points_ladder_sde_ftp.fmw']
        depList = ['DWRSPUB/fme_logger.fmw']
        self.execOrder.dependentFMW = depList
        self.execOrder.depStruct = {}
        self.execOrder.parseDependencies()
        retVal = self.execOrder.checkForDependencies()
        msg = 'The dependent FMW has not been found therefor the dependency is not satisfied: {0}'
        msg = msg.format(depList)
        self.assertTrue(retVal, msg)
        
        # now redo with a typo so the job is not found:
        depList = ['BCGW_REP_OTHER/prot_current_fire_points_ladder_sde_ftpdoesnotexist.fmw']
        self.execOrder.dependentFMW = depList
        self.execOrder.depStruct = {}
        self.execOrder.parseDependencies()
        retVal = self.execOrder.checkForDependencies()
        msg = 'The dependent FMW has been found but it should not have been {0}'
        msg = msg.format(depList)
        self.assertFalse(retVal, msg)
        
    def test_parseFmwPath(self):
        testFMWPath = 'Repo/fmwName/fmwName.fmw'
        expectRepo = 'Repo'
        expectFMW = 'fmwName.fmw'
        retFMW, retRepo = self.execOrder.parseFmwPath(testFMWPath)
        msg = 'Didnt return the expected {3}, returned: {0}, expected {1}, rawstring {2}'
        msgFormated = msg.format(retRepo, expectRepo, testFMWPath, 'Repository')
        self.assertEqual(expectRepo, retRepo, msgFormated)
        msgFormated = msg.format(retFMW, expectFMW, testFMWPath, 'FMW Path')
        self.assertEqual(expectRepo, retRepo, msgFormated)

    def test_parseDependencies(self):
        # setting up the test
        testData1 = ['repo/fmw.fmw']
        expectedDepStruct = {'repo': {'fmw.fmw': False}}
        self.execOrder.dependentFMW = testData1
        self.execOrder.depStruct = {}
        self.execOrder.parseDependencies()
        msg = 'Did not properly parse the struct'
        #self.assertIn(repo, container, msg)
        self.logger.debug("{0}".format(self.execOrder.depStruct))
        self.assertEqual(self.execOrder.depStruct, expectedDepStruct, msg)
        
        testData1 = ['repo/fmw.fmw', 'justfmw.fmw', 'somerepo/myfmw.fmw']
        expectedDepStruct = {'repo': {'fmw.fmw': False}, 
                             'somerepo': {'myfmw.fmw': False}, 
                             'norepo': {'justfmw.fmw': False}}
        self.execOrder.dependentFMW = testData1
        self.execOrder.depStruct = {}
        self.execOrder.parseDependencies()
        msg = 'Did not properly parse the struct'
        #self.assertIn(repo, container, msg)
        self.logger.debug("{0}".format(self.execOrder.depStruct))
        self.assertEqual(self.execOrder.depStruct, expectedDepStruct, msg)

        testData1 = ['repo/fmw.fmw', 'justfmw.fmw', 'somerepo/myfmw.fmw', 'antherFMW.fmw', 'somerepo/mysecondfmw.fmw']
        # expected is always lower case to eliminate case sensitivity
        expectedDepStruct = {'repo': {'fmw.fmw': False}, 
                             'somerepo': {'myfmw.fmw': False},
                             'somerepo': {'mysecondfmw.fmw': False},
                             'norepo': {'justfmw.fmw': False}, 
                             'norepo': {'antherfmw.fmw': False}}
        self.execOrder.dependentFMW = testData1
        self.execOrder.depStruct = {}
        self.execOrder.parseDependencies()
        msg = 'Did not properly parse the struct'
        #self.assertIn(repo, container, msg)
        self.logger.debug("{0}".format(self.execOrder.depStruct))
        self.logger.debug("{0}".format(expectedDepStruct))
        self.assertEqual(self.execOrder.depStruct, expectedDepStruct, msg)
        
    def test_haveDependenciesBeenFound(self):
        depList = ['DWRSPUB/fme_logger.fmw']
        self.execOrder.dependentFMW = depList
        self.execOrder.depStruct = {}
        self.execOrder.parseDependencies()
        repo = 'blah_blah'
        fmw = 'somethingblah.fmw'
        status = 'SUCCESS'
        retVal = self.execOrder.haveDependenciesBeenFound(repo, fmw, status )
        msg = 'Should have returned False'
        self.assertFalse(retVal, msg)
        
        depList = ['DWRSPUB/fme_logger.fmw', 'anotherrepo/anotherjob.fmw']
        self.execOrder.dependentFMW = depList
        self.execOrder.depStruct = {}
        self.execOrder.parseDependencies()
        repo = 'DWRSPUB'
        fmw = 'fme_logger.fmw'
        status = 'SUCCESS'
        retVal = self.execOrder.haveDependenciesBeenFound(repo, fmw, status )
        self.assertFalse(retVal, msg)
        repo = 'anotherrepo'
        fmw = 'anotherjob.fmw'
        status = 'SUCCESS'
        retVal = self.execOrder.haveDependenciesBeenFound(repo, fmw, status )
        self.assertTrue(retVal, msg)
        
        
    def test_hasJobBeenSeen(self):
        const = DataBCFMWTemplate.TemplateConstants()
        # test if the job has been seen, should not
        retVal = self.execOrder.hasJobBeenSeen(self.jobData)
        msg = "The job has been reported as been seen incorrectly"
        self.assertFalse( retVal, msg)
        retVal = self.execOrder.hasJobBeenSeen(self.jobData)
        
        # now that the job has been run through the same data should now
        # report that it has been seen
        msg = "The job has incorrectly been reporting as having not been seen"
        retVal = self.execOrder.hasJobBeenSeen(self.jobData)
        
        # move the date to now, which should cause the job to have been
        # seen, as the date is more recent.
        jobsubmitTime = datetime.datetime.now()
        jobsubmitTimeString = jobsubmitTime.strftime(const.FMEServer_DatetimeFormat)
        job = self.jobData.copy()
        job[const.FMEServerParams_timeSubmitted] = jobsubmitTimeString
        retVal = self.execOrder.hasJobBeenSeen(job)
        msg = 'should have returned not seen as the job date has been modified to now'
        self.assertTrue(retVal, msg)
        
        # now take the same job and change dest env key, now the 
        # job should report false
        pubParms = job[const.FMEServerParams_request][const.FMEServerParams_pubParams]
        for paramCnt in range(0, len(pubParms)):
            if pubParms[paramCnt]['name'] == const.FMWParams_DestKey:
                pubParms[paramCnt]['raw'] = 'NEW'
        job[const.FMEServerParams_request][const.FMEServerParams_pubParams] = pubParms
        retVal = self.execOrder.hasJobBeenSeen(job)
        msg = 'The job pub param was changed, should have returned false'
        self.assertFalse(retVal, msg)
        
        msg = 'Sent the same job back should now be true'
        retVal = self.execOrder.hasJobBeenSeen(job)
        self.assertTrue(retVal, msg)
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test_FMWExecutionOrder.test_hasJobBeenSeen']
    unittest.main()
    