'''
Created on Jan 14, 2016

@author: kjnether
'''
import unittest
import sys
import ChangeDetectLib

import datetime
import pytz
import time
import os.path
import shutil

class Test_ChangeDetect(unittest.TestCase):


    def setUp(self):
        
        self.fmeMacroValues_fileSrc = {  'DEST_DB_ENV_KEY': 'DELIV',
                        'DEST_FEATURE_1': 'CLAB_INDIAN_RESERVES',
                        'DEST_SCHEMA': 'WHSE_ADMIN_BOUNDARIES',
                        'DEST_TYPE': 'SDE30',
                        'FME_BASE': 'no',
                        'FME_BUILD_DATE': '20141210',
                        'FME_BUILD_DATE_ENCODED': '20141210',
                        'FME_BUILD_NUM': '14440',
                        'FME_BUILD_NUM_ENCODED': '14440',
                        'FME_CF_DIR': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate/',
                        'FME_CF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<solidus>',
                        'FME_CF_DIR_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate/',
                        'FME_CF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<solidus>',
                        'FME_DESKTOP': 'no',
                        'FME_HOME': 'E:\\sw_nt\\FME2014\\',
                        'FME_HOME_DOS': 'E:\\sw_nt\\FME2014',
                        'FME_HOME_DOS_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014',
                        'FME_HOME_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014<backslash>',
                        'FME_HOME_UNIX': 'E:/sw_nt/FME2014',
                        'FME_HOME_UNIX_ENCODED': 'E:<solidus>sw_nt<solidus>FME2014',
                        'FME_HOME_USERTYPED': 'E:\\sw_nt\\FME2014\\',
                        'FME_HOME_USERTYPED_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014<backslash>',
                        'FME_MF_DIR': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate/',
                        'FME_MF_DIR_DOS': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate',
                        'FME_MF_DIR_DOS_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate',
                        'FME_MF_DIR_DOS_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate',
                        'FME_MF_DIR_DOS_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate',
                        'FME_MF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<solidus>',
                        'FME_MF_DIR_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate/',
                        'FME_MF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<solidus>',
                        'FME_MF_DIR_MASTER_USERTYPED': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate/',
                        'FME_MF_DIR_MASTER_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<solidus>',
                        'FME_MF_DIR_UNIX': 'Z:/Workspace/kjnether/proj/FMETemplateRevision/wrk/newTemplate',
                        'FME_MF_DIR_UNIX_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether<solidus>proj<solidus>FMETemplateRevision<solidus>wrk<solidus>newTemplate',
                        'FME_MF_DIR_UNIX_MASTER': 'Z:/Workspace/kjnether/proj/FMETemplateRevision/wrk/newTemplate',
                        'FME_MF_DIR_UNIX_MASTER_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether<solidus>proj<solidus>FMETemplateRevision<solidus>wrk<solidus>newTemplate',
                        'FME_MF_DIR_USERTYPED': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate/',
                        'FME_MF_DIR_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<solidus>',
                        'FME_MF_NAME': 'clab_indian_reserves_staging_fgdb_bcgwdlv_Development.fmw',
                        'FME_MF_NAME_ENCODED': 'wb-xlate-1449856936276_1128',
                        'FME_MF_NAME_MASTER': 'clab_indian_reserves_staging_fgdb_bcgwdlv_Development.fmw',
                        'FME_MF_NAME_MASTER_ENCODED': 'wb-xlate-1449856936276_1128',
                        'FME_PRODUCT_NAME': 'FME(R) 2014 SP5',
                        'FME_PRODUCT_NAME_ENCODED': 'FME<openparen>R<closeparen><space>2014<space>SP5',
                        'SRC_FEATURE_1': 'CLAB_INDIAN_RESERVES',
                        'SRC_FILEGDB_1': '\\\\data.bcgov\\data_staging_ro\\BCGW\\administrative_boundaries\\Federal_IRs.gdb'}
        self.fmeMacroValues = self.fmeMacroValues_fileSrc
        
        self.testChangeControlFile = os.path.join(os.path.dirname(__file__), 'testData', 'etlFileChangeLog.txt')
        self.chngObj = ChangeDetectLib.ChangeDetect(self.fmeMacroValues)

    def tearDown(self):
        pass

    def test_getFileModificationTime(self):
        print 'getting here'
        file = '//data.bcgov/data_staging/bcgw/physical_infrastructure/diagnostic_facilities_data_structure.csv'
        modDate = self.chngObj.getFileModificationUTCDateTime(__file__)
        print 'modDate', modDate, type(modDate)
        
    def test_getMostRecentModificationDate(self):
        self.chngObj.changeLogFilePath = self.testChangeControlFile
        self.chngObj.fmeMacroValues['DEST_DB_ENV_KEY'] = 'prod'
        srcFile = r'\\data.bcgov\data_staging\BCGW\administrative_boundaries\cbd_boundary_poly.shp'
        modDate = self.chngObj.getFileModificationUTCDateTime(srcFile)
        srcFile = r'\\data.bcgov\data_staging\BCGW\fresh_water_and_marine\FloodProtectionWorks.gdb'
        modDate = self.chngObj.getFileModificationUTCDateTime(srcFile)
        srcFile = r'\\data.bcgov\data_staging\BCGW\physical_infrastructure\ownership_lut.csv'
        modDate = self.chngObj.getFileModificationUTCDateTime(srcFile)
        print modDate
        
    def test_readChangeLog(self):
        print 'self.testChangeControlFile', self.testChangeControlFile
        changeCache = self.chngObj.readChangeLog(self.testChangeControlFile)
        for path in changeCache.changeDict.keys():
            print 'path', path
            chng = changeCache.hasChanged(path)
            print 'chng', chng
            
    def test_getChangeLogFilePath(self):
        chngLog = self.chngObj.getChangeLogFilePath()
        chngLog = os.path.normpath(chngLog)
        if os.path.exists(chngLog):
            print 'changLog is', chngLog
            #os.rmdir(os.path.dirname(chngLog))
            shutil.rmtree(os.path.dirname(chngLog))
        chngLog = self.chngObj.getChangeLogFilePath(create=True)
        msg = 'Should have created the directory {0} but didn\'t'
        msg = msg.format(chngLog)
        self.assertTrue(os.path.exists(chngLog), msg)
        
    def test_TimeStampLogicConfirmation(self):
        # not an actual test, but instead code used to figure out 
        # the best way to deal with datetime objects used for 
        # change detection
        inFile = os.path.join(os.path.dirname(__file__), 'testData', 'etlFileChangeLog.txt')
        modificationTimeStamp = os.path.getmtime(inFile)
        print 'modificationTimeStamp', modificationTimeStamp
        localDateTime = datetime.datetime.fromtimestamp(modificationTimeStamp)
        # this way you KNOW you have utc.
        # localtime, its more difficult to get what the acutal 
        # offset is.  UTC youknow you have utc time.  From that 
        # you can convert to pst.
        utcDateTime = datetime.datetime.utcfromtimestamp(modificationTimeStamp)
        print 'utc timestamp', time.mktime(utcDateTime.timetuple())
        print datetime.datetime.strftime(localDateTime, '%Y-%m-%d %H:%M:%S')
        print datetime.datetime.strftime(utcDateTime, '%Y-%m-%d %H:%M:%S')
                
        # makeing aware of timezone
        localDateTime = pytz.utc.localize(localDateTime)
        utcDateTime = pytz.utc.localize(utcDateTime)
        
        pacificfromutc = utcDateTime.astimezone(pytz.timezone('US/Pacific'))
        print 'pacific from utc', datetime.datetime.strftime(pacificfromutc, '%Y-%m-%d %H:%M:%S')


        print 'tzname', localDateTime.tzname()
        print 'tzname', utcDateTime.tzname()
            
        print 'local timestamp', time.mktime(localDateTime.timetuple())
        print 'utc timestamp', time.mktime(utcDateTime.timetuple())

    def test_truncDateTime(self):
        dt = datetime.datetime.now()
        print 'tzname', dt.tzname()
        # assign UTC time zone
        dt = pytz.utc.localize(dt)
        # create from timestamp
        #dt = datetime.datetime.utcfromtimestamp(1453312757.93874)
        #dt = pytz.utc.localize(dt)

        print datetime.datetime.strftime(dt, '%Y-%m-%d %H:%M:%S %f')

        #1453312757
        print time.mktime(dt.timetuple())
        dt = dt.replace( microsecond=0)
        print 'tzname', dt.tzname()

        print time.mktime(dt.timetuple())
        print datetime.datetime.strftime(dt, '%Y-%m-%d %H:%M:%S %f')
        chngCache = ChangeDetectLib.ChangeCache(self.chngObj, {})
        
        startTimeZone = dt.tzname()

        dt = chngCache.truncDateTime(dt)
        timeZone = dt.tzname()
        msg = 'truncDateTime removed the timezone from the input ' +\
              'datetime object'
        self.assertNotEqual(None, timeZone, msg)
        
        msg = 'truncDatetime method changed the timzeone for the ' + \
              'input datetime object.  original timezone: {0} ' +\
              'new/changed datetime object {1}'
        msg = msg.format(startTimeZone, timeZone)
        self.assertEqual(startTimeZone, timeZone, msg)
               
        msg = 'the truncate method did not truncate.  The datetime ' + \
              'object has microseconds not equal to 0 after the ' + \
              'truncate.  microseconds: {0}'
        msg = msg.format(dt.microsecond)
        self.assertEqual(0, dt.microsecond, msg)
        
class Test_ChangeLog(unittest.TestCase):
    
    def setUp(self):
        # testData\outputs\changeLogs\FileChangeLog_1.txt
        self.changeLogFile = os.path.join(os.path.dirname(__file__), 'testData',  'outputs', 'changeLogs', 'FileChangeLog_1.txt')
        
    def test_LoadLogFile(self):
        changeLog = ChangeDetectLib.ChangeLog(self.changeLogFile)
        
        

if __name__ == "__main__":
    sys.argv = ['', 'Test_ChangeLog.test_LoadLogFile']
    unittest.main()
    
    