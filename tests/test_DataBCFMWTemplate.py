'''
Created on Dec 11, 2015

@author: kjnether
'''
import os
import sys
import platform
# pathList = os.environ['PATH'].split(';')
# pathList.insert(0, r'E:\sw_nt\FME2015')
# sys.path.insert(0, r'E:\sw_nt\FME2015\fmeobjects\python27')
# sys.path.insert(0, r'\\data.bcgov\work\scripts\python\DataBCPyLib')
# sys.path.insert(0, r'\\data.bcgov\work\Workspace\kjnether\proj\FMETemplateRevisions_Python')

# os.environ['PATH'] = ';'.join(pathList)

import unittest
import site
import pprint
import DataBCFMWTemplate
import logging

class Test_CalcParams(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        #ch = logging.StreamHandler()
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        #ch.setFormatter(formatter)
        #self.logger.addHandler(ch)

        # dummy data used for testing.
        # these are for a file source
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

        # database source
        self.fmeMacroValues_DBSrc = {     'DEST_DB_ENV_KEY': 'DEV',
                                    'DEST_FEATURE_1': 'AEI_2000_AIR_PERMIT_POINTS_SP',
                                    'DEST_INSTANCE': 'bcgw.bcgov',
                                    'DEST_SCHEMA': 'whse_environmental_monitoring',
                                    'FME_BASE': 'no',
                                    'FME_BUILD_DATE': '20141210',
                                    'FME_BUILD_DATE_ENCODED': '20141210',
                                    'FME_BUILD_NUM': '14440',
                                    'FME_BUILD_NUM_ENCODED': '14440',
                                    'FME_CF_DIR': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_CF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_CF_DIR_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_CF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_DESKTOP': 'no',
                                    'FME_HOME': 'E:\\sw_nt\\FME2014\\',
                                    'FME_HOME_DOS': 'E:\\sw_nt\\FME2014',
                                    'FME_HOME_DOS_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014',
                                    'FME_HOME_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014<backslash>',
                                    'FME_HOME_UNIX': 'E:/sw_nt/FME2014',
                                    'FME_HOME_UNIX_ENCODED': 'E:<solidus>sw_nt<solidus>FME2014',
                                    'FME_HOME_USERTYPED': 'E:\\sw_nt\\FME2014\\',
                                    'FME_HOME_USERTYPED_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014<backslash>',
                                    'FME_MF_DIR': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_MF_DIR_DOS': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws',
                                    'FME_MF_DIR_DOS_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws',
                                    'FME_MF_DIR_DOS_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws',
                                    'FME_MF_DIR_DOS_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws',
                                    'FME_MF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_MF_DIR_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_MF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_MF_DIR_MASTER_USERTYPED': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_MF_DIR_MASTER_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_MF_DIR_UNIX': 'Z:/Workspace/kjnether/proj/FMETemplateRevision/wrk/newTemplate/fmws',
                                    'FME_MF_DIR_UNIX_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether<solidus>proj<solidus>FMETemplateRevision<solidus>wrk<solidus>newTemplate<solidus>fmws',
                                    'FME_MF_DIR_UNIX_MASTER': 'Z:/Workspace/kjnether/proj/FMETemplateRevision/wrk/newTemplate/fmws',
                                    'FME_MF_DIR_UNIX_MASTER_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether<solidus>proj<solidus>FMETemplateRevision<solidus>wrk<solidus>newTemplate<solidus>fmws',
                                    'FME_MF_DIR_USERTYPED': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_MF_DIR_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_MF_NAME': 'aei_2000_air_permit_points_sp_air_odb_bcgw.fmw',
                                    'FME_MF_NAME_ENCODED': 'wb-xlate-1450398403413_5388',
                                    'FME_MF_NAME_MASTER': 'aei_2000_air_permit_points_sp_air_odb_bcgw.fmw',
                                    'FME_MF_NAME_MASTER_ENCODED': 'wb-xlate-1450398403413_5388',
                                    'FME_PRODUCT_NAME': 'FME(R) 2014 SP5',
                                    'FME_PRODUCT_NAME_ENCODED': 'FME<openparen>R<closeparen><space>2014<space>SP5',
                                    'LogURL': 'http://arneb.dmz/fmejobsubmitter/DWRSPUB/fme_logger.fmw',
                                    'NotificationEmail': 'dataetl@gov.bc.ca',
                                    'SRC_FEATURE_1': 'I2K_PERMIT',
                                    'SRC_ORA_SERVICENAME': 'airprod1.nrs.bcgov',
                                    'SRC_ORA_SCHEMA': 'inventory2000',
                                    'SRC_SS_PROXY_SCHEMA': 'PROXY_MINFILE_BCGW',
                                    'SRC_SS_SCHEMA': 'Minfile',
                                    'SRC_SS_DBNAME': 'Minfile',
                                    'SRC_HOST': 'apocalypse.idir.bcgov',
                                    'SRC_PORT': 443
                                    }


        self.fmeMacroValues = self.fmeMacroValues_fileSrc
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)

        self.logger = logging.getLogger()

        otherLogger = logging.getLogger('DataBCFMWTemplate')
        otherLogger.setLevel(logging.DEBUG)

        self.logger.setLevel(logging.DEBUG)
        
        print 'getting the handlers, '
        hasStream = False
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                hasStream = True
            #print handler
        if not hasStream:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
            otherLogger.addHandler(ch)
        self.logger = logging.getLogger()
        self.logger.debug("this is the the first log message")

    def tearDown(self):
        pass

    def test_getDestServer(self):
        host = self.calcParams.getDestinationHost()
        print 'host', host
        
    def test_getDestinationTables(self):
        tabs = self.calcParams.getDestinationTables()
        expect = ['CLAB_INDIAN_RESERVES']
        msg = 'did not properly retrieve the destination tables, got {0}' + \
              'expected {1}'
        msg = msg.format(tabs, expect)
        self.assertEqual(tabs, expect,  msg)
        
        tabs = self.calcParams.getDestinationTables(includeSchemaPrefix=True)
        expect = ['WHSE_ADMIN_BOUNDARIES.CLAB_INDIAN_RESERVES']
        msg = 'did not properly retrieve the destination tables, got {0}' + \
              'expected {1}'
        msg = msg.format(tabs, expect)
        self.assertEqual(set(tabs), set(expect),  msg)
        
        self.calcParams.fmeMacroVals['DEST_FEATURE_2'] = 'TEST123'
        self.calcParams.fmeMacroVals['DEST_SCHEMA_2'] = 'SCHEMATEST'
        tabs = self.calcParams.getDestinationTables(includeSchemaPrefix=True)
        expect = ['WHSE_ADMIN_BOUNDARIES.CLAB_INDIAN_RESERVES', 'SCHEMATEST.TEST123']
        msg = 'did not properly retrieve the destination tables, got {0}' + \
              'expected {1}'
        msg = msg.format(tabs, expect)
        self.assertEqual(set(tabs), set(expect),  msg)
        
        self.calcParams.fmeMacroVals['DEST_FEATURE_5'] = 'TEST5555'
        self.calcParams.fmeMacroVals['DEST_SCHEMA_5'] = 'SCHEMATEST_5555'
        tabs = self.calcParams.getDestinationTables(includeSchemaPrefix=True)
        expect = ['WHSE_ADMIN_BOUNDARIES.CLAB_INDIAN_RESERVES', 'SCHEMATEST.TEST123', 'SCHEMATEST_5555.TEST5555']
        msg = 'did not properly retrieve the destination tables, got {0}' + \
              'expected {1}'
        msg = msg.format(tabs, expect)
        self.assertEqual(set(tabs), set(expect),  msg)
        
        self.calcParams.fmeMacroVals['DEST_FEATURE_6'] = 'TEST_NUM6'
        tabs = self.calcParams.getDestinationTables(includeSchemaPrefix=True)
        expect = ['WHSE_ADMIN_BOUNDARIES.CLAB_INDIAN_RESERVES', 'SCHEMATEST.TEST123', 'SCHEMATEST_5555.TEST5555', 'WHSE_ADMIN_BOUNDARIES.TEST_NUM6']
        msg = 'did not properly retrieve the destination tables, got {0}' + \
              'expected {1}'
        msg = msg.format(tabs, expect)
        self.assertEqual(set(tabs), set(expect),  msg)
        
        tabs = self.calcParams.getDestinationTables(includeSchemaPrefix=False)
        expect = ['CLAB_INDIAN_RESERVES', 'TEST123', 'TEST5555', 'TEST_NUM6']
        msg = 'did not properly retrieve the destination tables, got {0}' + \
              'expected {1}'
        msg = msg.format(tabs, expect)
        self.assertEqual(set(tabs), set(expect),  msg)

    def test_getDestinationPassword(self):
        passw = self.calcParams.getDestinationPassword()
        print 'passw', passw

    def test_getSQLServerSchema(self):
        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)
        schema = self.calcParams.getSrcSQLServerSchema()
        expectedSchema = 'Minfile'
        # schema = self.calcParams.getSQLServerSchemaForPasswordRetrieval()
        msg = 'schema expected: {0}, recieved {1}, position param is null'
        assertMsg = msg.format(expectedSchema, schema)
        self.assertEqual(schema, expectedSchema, msg)
        print 'schema', schema
        # should raise an error
        self.assertRaises(KeyError, lambda: self.calcParams.getSrcSQLServerSchema(1))

    def test_getMacroValueUsingPosition(self):
        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)
        # list of macro value, position, expected values

        testValues = [
                ['SRC_FEATURE_1', 1, 'SRC_FEATURE_1'],
                ['SRC_FEATURE_3', 1, 'SRC_FEATURE_1'],
                ['SRC_FEATURE_1', 4, 'SRC_FEATURE_4'],
                ['SRC_FEATURE_', 4, 'SRC_FEATURE_4'],
                ['SRC_FEATURE_', 1, 'SRC_FEATURE_1'],
                ['SRC_FEATURE_', 3, 'SRC_FEATURE_3'],
                ['SRC_ORA_SCHEMA', 3, 'SRC_ORA_SCHEMA_3'],
                ['SRC_ORA_SCHEMA', 1, 'SRC_ORA_SCHEMA_1'],
                ['SRC_ORA_SCHEMA_4', 1, 'SRC_ORA_SCHEMA_1'],
                ['SRC_ORA_SCHEMA_4', 2, 'SRC_ORA_SCHEMA_2']
                      ]
        for testVals in testValues:
            self.logger.info("current values: {0}".format(testVals))
            retVal = self.calcParams.getMacroKeyForPosition(testVals[0], testVals[1])
            msg = 'sent {0}, position {1}, returned {2}, expected {3}'
            msg = msg.format(testVals[0], testVals[1], retVal, testVals[2])
            self.assertEqual(testVals[2], retVal, msg)

    def test_getSourcePassword(self):
        msg = "unable to retrieve the password for schema {0} and " + \
              'instance {1} using source password retrieval methods'

        # test get dbc passwords as source, requires that dbc get picked up as
        # a defined database.  Defined databases can be used as either sources
        # OR destinations

        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] = 'APP_CCF'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'DBCPRD.BCGOV'
        self.calcParams.fmeMacroVals['DEST_DB_ENV_KEY'] = 'DLV'
        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME']))

        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] = 'APP_CCF'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'DBCTST.BCGOV'
        self.calcParams.fmeMacroVals['DEST_DB_ENV_KEY'] = 'DLV'
        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME']))


        msg = "unable to retrieve the password for schema {0} and " + \
              'instance {1} using source password retrieval methods'

        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)
        # spass = self.calcParams.getSourcePassword()
        # print 'src pass', spass
        # CWI_SPI_OPD@ENVPROD1.NRS.BCGOV
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] = 'CWI_SPI_OPD'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'ENVPROD1'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'ENVPROD1'
        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME']))

        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] = 'WHSE_CORP'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'IDWPROD1.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'IDWPROD1.BCGOV'
        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME']))

        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] = 'WHSE_CORP'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'BCGW.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'BCGW.BCGOV'

        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME']))

        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_1'] = 'WHSE_CORP'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE_1'] = 'BCGW.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_1'] = 'BCGW.BCGOV'

        spass = self.calcParams.getSourcePassword(1)
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_1'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_1']))

        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_3'] = 'WHSE_CORP'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE_3'] = 'BCGW.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_3'] = 'BCGW.BCGOV'
        spass = self.calcParams.getSourcePassword(3)
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_3'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_3']))

        self.calcParams.fmeMacroVals['SRC_ORA_PROXY_SCHEMA_4'] = 'DBLINK_FISS'
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_4'] = 'FISS'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE_4'] = 'RIBPROD1.NRS.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_4'] = 'RIBPROD1.NRS.BCGOV'
        spass = self.calcParams.getSourcePassword(4)
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_4'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_4']))

        self.calcParams.fmeMacroVals['SRC_ORA_PROXY_SCHEMA'] = 'DBLINK_FISS'
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] = 'FISS'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'RIBPROD1.NRS.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'RIBPROD1.NRS.BCGOV'
        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_4'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_4']))

    def test_getSourcePasswordHeuristic(self):
        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] = 'CWI_SPI_OPD'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'ENVPROD1'
        self.calcParams.plugin.currentPMPResource = 'ETL_OPERATIONAL_DBLINKS'
        spass = self.calcParams.getSourcePasswordHeuristic()
        print 'pass is:', spass

        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_5'] = 'CWI_SPI_OPD'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_5'] = 'ENVPROD1'
        spass = self.calcParams.getSourcePasswordHeuristic(5)
        print 'pass is:', spass

    def test_getDatabaseConnectionFilePath(self):
        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.fmeMacroValues['DEST_DB_ENV_KEY'] = 'DLV'
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)
        connFile = self.calcParams.getDestDatabaseConnectionFilePath()
        print 'connFile', connFile

    def test_getFailedFeaturesFile(self):
        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.fmeMacroValues['DEST_DB_ENV_KEY'] = 'DLV'
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)
        failedFeats = self.calcParams.getFailedFeaturesFile()
        failedFeatsDir = os.path.dirname(failedFeats)
        msg = 'The output directory {0} for failed features ' + \
              'was not created'
        msg = msg.format(failedFeatsDir)
        self.assertTrue(os.path.exists(failedFeatsDir), msg)

    def test_getSrcSqlServerPassword(self):
        self.logger.debug("starting the test: test_getSrcSqlServerPassword")
        fmeMacroValues = self.fmeMacroValues_DBSrc
        fmeMacroValues['DEST_DB_ENV_KEY'] = 'DLV'
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        sqlServrPasswd = calcParams.getSrcSqlServerPassword()
        msg = 'Returned a null value for a password'
        self.assertIsNotNone(sqlServrPasswd, msg)

        # now test that can retrieve without the domain
        # 'SRC_SS_PROXY_SCHEMA': 'PROXY_MINFILE_BCGW',
        # 'SRC_SS_SCHEMA': 'Minfile',
        # 'SRC_SS_DBNAME': 'Minfile',
        # 'SRC_HOST': 'apocalypse.idir.bcgov',
        # 'SRC_PORT': 443
        fmeMacroValues['SRC_HOST'] = 'apocalypse'
        fmeMacroValues['SRC_SS_DBNAME'] = 'Minfile.somesuffix'
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        sqlServrPasswd = calcParams.getSrcSqlServerPassword()
        self.assertIsNotNone(sqlServrPasswd, msg)

        # finally need to test for a different position password
        fmeMacroValues['SRC_HOST_6'] = 'apocalypse'
        fmeMacroValues['SRC_SS_DBNAME_6'] = 'Minfile.somesuffix'
        fmeMacroValues['SRC_SS_SCHEMA_6'] = 'PROXY_MINFILE_BCGW'
        del fmeMacroValues['SRC_SS_PROXY_SCHEMA']
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        sqlServrPasswd = calcParams.getSrcSqlServerPassword(6)
        self.assertIsNotNone(sqlServrPasswd, msg)

        # now try to proxy with a position also
        fmeMacroValues['SRC_SS_PROXY_SCHEMA_6'] = 'PROXY_MINFILE_BCGW'
        fmeMacroValues['SRC_SS_SCHEMA_6'] = 'Minfile'
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        sqlServrPasswd = calcParams.getSrcSqlServerPassword(6)
        self.assertIsNotNone(sqlServrPasswd, msg)

    def test_getDependencyParams(self):
        # tests to write here:
        # - get the DEP_FMW parameter, when its populated
        # - test the exists method for this parameter
        # - test the defaults that should get parameter from
        #   config file for timewindow, max times, wait time
        #   DEP_TIMEWINDOW, DEP_WAITTIME, DEP_MAXRETRIES
        # - test getting these from pub params
        # - test sanity checking of these parameters

        #----------------------------------------
        # -     DEP_FMW TESTS
        #----------------------------------------
        self.logger.debug("starting the test: test_getDependencyParams")
        fmeMacroValues = self.fmeMacroValues_DBSrc
        fmeMacroValues['DEP_FMW'] = 'job1.fmw, job2.fmw'
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        deps = calcParams.getDependentFMWs()
        self.logger.debug("deps: {0}".format(deps))
        msg = 'expected {0} in the depedency list, instead got {1}'
        msg = msg.format('job1.fmw', deps)
        self.assertIn('job1.fmw', deps)

        depsExist = calcParams.existsDependentFMWSs()
        msg = 'dependencies should exist but exists method returns FALSE'
        self.assertTrue(depsExist, msg)

        #----------------------------------------
        # -     DEP_TIMEWINDOW TESTS
        #----------------------------------------
        expectDepWindow = 5400
        depWindow = calcParams.getDependencyTimeWindow()
        msg = 'The dependency window expected is {0} but got {1}, check ' + \
              'the value in the config file, and see that it matches the ' + \
              'the expected value, or change the expected value to match'
        msg = msg.format(expectDepWindow, depWindow)
        self.assertEqual(expectDepWindow, depWindow, msg)

        # now set the default time window
        fmeMacroValues = self.fmeMacroValues_DBSrc
        expectedValue = '7777'
        fmeMacroValues['DEP_TIMEWINDOW'] = expectedValue
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        depWindow = calcParams.getDependencyTimeWindow()
        msg = 'dependency window is set in the parameter {0} as ' + \
              ' {1}, but the method getDependencyTimeWindow returned ' + \
              '{2}'
        msg = msg.format('DEP_TIMEWINDOW', expectedValue, depWindow)

        #----------------------------------------
        # -     DEP_WAITTIME TESTS
        #----------------------------------------
        waitTimeExpected = 900
        waitTimeReturned = calcParams.getDependencyWaitTime()
        msg = 'The wait time expected is {0} but got {1}, check ' + \
              'the value in the config file, and see that it matches the ' + \
              'the expected value, or change the expected value to match'
        msg = msg.format(waitTimeExpected, waitTimeReturned)
        self.assertEqual(waitTimeExpected, waitTimeReturned, msg)

        fmeMacroValues = self.fmeMacroValues_DBSrc
        expectedValue = 22
        # macro values can only be stored as strings
        fmeMacroValues['DEP_WAITTIME'] = str(expectedValue)
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        depWaitTime = calcParams.getDependencyWaitTime()
        msg = 'wait time is set in the parameter {0} as ' + \
              ' {1}, but the method getDependencyWaitTime returned ' + \
              '{2}'
        msg = msg.format('DEP_WAITTIME', expectedValue, depWaitTime)
        self.assertEqual(depWaitTime, expectedValue, msg)

        fmeMacroValues = self.fmeMacroValues_DBSrc
        expectedValue = '22abc'
        fmeMacroValues['DEP_WAITTIME'] = expectedValue
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        msg = 'set the wait time to a non numeric value.  The method ' + \
              'getDependencyWaitTime, should raise an error when this ' + \
              'occurs however it did not.'
        msg = msg.format('DEP_WAITTIME', expectedValue, depWaitTime)
        self.assertRaises(ValueError, lambda: calcParams.getDependencyWaitTime())

        #----------------------------------------
        # -     DEP_MAXRETRIES TESTS
        #----------------------------------------
        maxRetryExpected = 5
        maxRetryReturned = calcParams.getDependencyMaxRetries()
        msg = 'The max retries expected is {0} but got {1}, check ' + \
              'the value in the config file, and see that it matches the ' + \
              'the expected value, or change the expected value to match'
        msg = msg.format(maxRetryExpected, maxRetryReturned)
        self.assertEqual(maxRetryExpected, maxRetryReturned, msg)

        fmeMacroValues = self.fmeMacroValues_DBSrc
        expectedValue = 22
        # macro values can only be stored as strings
        fmeMacroValues['DEP_MAXRETRIES'] = expectedValue
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        returnValue = calcParams.getDependencyMaxRetries()
        msg = 'max retries is set in the parameter {0} as ' + \
              ' {1}, but the method getDependencyWaitTime returned ' + \
              '{2}'
        msg = msg.format('DEP_MAXRETRIES', expectedValue, returnValue)
        self.assertEqual(returnValue, expectedValue, msg)

        fmeMacroValues = self.fmeMacroValues_DBSrc
        expectedValue = '22abc'
        fmeMacroValues['DEP_MAXRETRIES'] = expectedValue
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger
        msg = 'set the max retries to a non numeric value.  The method ' + \
              'getDependencyWaitTime, should raise an error when this ' + \
              'occurs however it did not.'
        msg = msg.format('DEP_MAXRETRIES', expectedValue, depWaitTime)
        self.assertRaises(ValueError, lambda: calcParams.getDependencyMaxRetries())

        

class Test_CalcParamsDevel(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        #ch = logging.StreamHandler()
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        #ch.setFormatter(formatter)
        #self.logger.addHandler(ch)

        self.fmeMacroValues_DBSrc = {
                                    'DEST_DB_ENV_KEY': 'DEV',
                                    'DEST_FEATURE_1': 'AEI_2000_AIR_PERMIT_POINTS_SP',
                                    'DEST_ORA_SERVICENAME': 'bcgw.bcgov',
                                    'DEST_SCHEMA': 'whse_environmental_monitoring',
                                    'FME_BASE': 'no',
                                    'FME_BUILD_DATE': '20141210',
                                    'FME_BUILD_DATE_ENCODED': '20141210',
                                    'FME_BUILD_NUM': '14440',
                                    'FME_BUILD_NUM_ENCODED': '14440',
                                    'FME_CF_DIR': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_CF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_CF_DIR_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_CF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_DESKTOP': 'no',
                                    'FME_HOME': 'E:\\sw_nt\\FME2014\\',
                                    'FME_HOME_DOS': 'E:\\sw_nt\\FME2014',
                                    'FME_HOME_DOS_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014',
                                    'FME_HOME_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014<backslash>',
                                    'FME_HOME_UNIX': 'E:/sw_nt/FME2014',
                                    'FME_HOME_UNIX_ENCODED': 'E:<solidus>sw_nt<solidus>FME2014',
                                    'FME_HOME_USERTYPED': 'E:\\sw_nt\\FME2014\\',
                                    'FME_HOME_USERTYPED_ENCODED': 'E:<backslash>sw_nt<backslash>FME2014<backslash>',
                                    'FME_MF_DIR': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_MF_DIR_DOS': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws',
                                    'FME_MF_DIR_DOS_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws',
                                    'FME_MF_DIR_DOS_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws',
                                    'FME_MF_DIR_DOS_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws',
                                    'FME_MF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_MF_DIR_MASTER': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_MF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_MF_DIR_MASTER_USERTYPED': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_MF_DIR_MASTER_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_MF_DIR_UNIX': 'Z:/Workspace/kjnether/proj/FMETemplateRevision/wrk/newTemplate/fmws',
                                    'FME_MF_DIR_UNIX_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether<solidus>proj<solidus>FMETemplateRevision<solidus>wrk<solidus>newTemplate<solidus>fmws',
                                    'FME_MF_DIR_UNIX_MASTER': 'Z:/Workspace/kjnether/proj/FMETemplateRevision/wrk/newTemplate/fmws',
                                    'FME_MF_DIR_UNIX_MASTER_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether<solidus>proj<solidus>FMETemplateRevision<solidus>wrk<solidus>newTemplate<solidus>fmws',
                                    'FME_MF_DIR_USERTYPED': 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws/',
                                    'FME_MF_DIR_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>FMETemplateRevision<backslash>wrk<backslash>newTemplate<backslash>fmws<solidus>',
                                    'FME_MF_NAME': 'aei_2000_air_permit_points_sp_air_odb_bcgw.fmw',
                                    'FME_MF_NAME_ENCODED': 'wb-xlate-1450398403413_5388',
                                    'FME_MF_NAME_MASTER': 'aei_2000_air_permit_points_sp_air_odb_bcgw.fmw',
                                    'FME_MF_NAME_MASTER_ENCODED': 'wb-xlate-1450398403413_5388',
                                    'FME_PRODUCT_NAME': 'FME(R) 2014 SP5',
                                    'FME_PRODUCT_NAME_ENCODED': 'FME<openparen>R<closeparen><space>2014<space>SP5',
                                    'LogURL': 'http://arneb.dmz/fmejobsubmitter/DWRSPUB/fme_logger.fmw',
                                    'NotificationEmail': 'dataetl@gov.bc.ca',
                                    'SRC_FEATURE_1': 'I2K_PERMIT',
                                    'SRC_ORA_INSTANCE': 'airprod1.nrs.bcgov',
                                    'SRC_ORA_SERVICENAME': 'airprod1.nrs.bcgov',
                                    'SRC_ORA_SCHEMA': 'inventory2000',
                                    'SRC_SS_PROXY_SCHEMA': 'PROXY_MINFILE_BCGW',
                                    'SRC_SS_SCHEMA': 'minfile',
                                    'SRC_SS_DBNAME':  'Minfile',
                                    'SRC_HOST': 'apocalypse.idir.bcgov',
                                    'SRC_PORT': 443 }

        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        # for these tests going to change the FME_MF_DIR value for testing
        self.fmeMacroValues['FME_MF_DIR'] = os.path.join(os.path.dirname(__file__), 'testData')
        self.logger.debug("credentials file path: {0}".format(self.fmeMacroValues['FME_MF_DIR']))
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues, True)

    def test_getSourcePassword(self):
        expectedPassword = 'thisIsNotThePassword'
        const = DataBCFMWTemplate.TemplateConstants()

        dummyFMWDir = os.path.join(os.path.dirname(__file__), 'testData', 'config')
        self.logger.debug("dummyFMWDir {0}".format(dummyFMWDir))
        self.logger.debug("fme parameter {0} was set to {1}".format(const.FMWMacroKey_FMWDirectory, dummyFMWDir))
        msg = 'The method {0} returned {1}, but the test expects it to return {2}'
        self.fmeMacroValues[const.FMWMacroKey_FMWDirectory] = dummyFMWDir
        self.logger.debug("fme parameter {0} was set to {1}".format(const.FMWMacroKey_FMWDirectory, dummyFMWDir))
        calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues, True)

        # Verify that changing the path to a config file that does not
        # exist raises an error
        # ----- commented out this test as there should never be a case where the
        # ----- fmw directory points to something that does not exist.
        # origVal = self.fmeMacroValues[const.FMWMacroKey_FMWDirectory]
        # self.fmeMacroValues[const.FMWMacroKey_FMWDirectory] = os.path.dirname(self.fmeMacroValues[const.FMWMacroKey_FMWDirectory]) + 'randomText'
        # print 'fmw dir that does not exist: ', self.fmeMacroValues[const.FMWMacroKey_FMWDirectory]
        # self.assertRaises(ValueError, lambda: DataBCFMWTemplate.CalcParams(self.fmeMacroValues, True))

        # forcing the test to use the test password file

        develSrcPass = calcParams.getSourcePassword()
        msg = msg.format('getSourcePassword', develSrcPass, expectedPassword)
        self.assertEqual(expectedPassword, develSrcPass, msg)

        develSrcPass = calcParams.getSourcePasswordHeuristic()
        msg = msg.format('getSourcePasswordHeuristic', develSrcPass, expectedPassword)
        self.assertEqual(expectedPassword, develSrcPass, msg)

        # should return nothing
        calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'airprod1'
        calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'airprod1'
        # develSrcPasswd = calcParams.getSourcePassword()
        # msg = 'Trying to retrieve the password for {0}, but there is only ' + \
        #      'There is no specific entry for that source database so should ' + \
        #      'return \'None\' but intead its returning {1}'
        # msg = msg.format(calcParams.fmeMacroVals['SRC_INSTANCE'], develSrcPasswd)
        # self.assertRaises(ValueError, lambda:  calcParams.getSourcePassword())
        self.assertEqual(expectedPassword, develSrcPass, msg)
        develSrcPass = calcParams.getSourcePasswordHeuristic()
        msg = msg.format('getSourcePasswordHeuristic', develSrcPass, expectedPassword)
        self.assertEqual(expectedPassword, develSrcPass, msg)

        calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'airprod99'
        self.assertRaises(ValueError, lambda:  calcParams.getSourcePassword())
        self.assertRaises(ValueError, lambda:  calcParams.getSourcePasswordHeuristic())

        calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'envprod1'
        calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'envprod1'
        calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] = 'doodoo'
        calcParams.fmeMacroVals['SRC_ORA_PROXY_SCHEMA'] = 'USERNAME'
        develSrcPass = calcParams.getSourcePassword()
        msg = msg.format('getSourcePassword', develSrcPass, expectedPassword)
        self.assertEqual(expectedPassword, develSrcPass, msg)

    def test_getDestinationPassword(self):
        expectedPassword = 'thisIsNotThePassword'
        const = DataBCFMWTemplate.TemplateConstants()
        calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues, True)
        develDestPass = calcParams.getDestinationPassword()
        print 'develDestPass', develDestPass
        msg = 'Retrieving the password for the user {0} and the ' + \
              'servicename {1}.  Expecting {2}, but returned {3}'
        msg = msg.format(calcParams.fmeMacroVals[const.FMWParams_DestSchema],
                         calcParams.fmeMacroVals[const.FMWParams_DestServiceName],
                         expectedPassword,
                         develDestPass)
        self.assertEqual(develDestPass, expectedPassword, msg)

        origSchema = calcParams.fmeMacroVals[const.FMWParams_DestSchema]
        calcParams.fmeMacroVals[const.FMWParams_DestSchema] = 'NOTSCHEMA'
        self.assertRaises(ValueError, lambda: calcParams.getDestinationPassword())

        # test file doesn't have passwords for the test instance so
        # should raise an error.
        self.fmeMacroValues['DEST_DB_ENV_KEY'] = 'TEST'
        calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues, True)
        self.assertRaises(ValueError, lambda: calcParams.getDestinationPassword())

    def test_getFailedFeaturesFile(self):
        calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues, True)
        # forcing the use of this simple root logger in the class, messy I know
        # but can't think any other way to get it working quickly.
        calcParams.logger = self.logger
        calcParams.plugin.logger = self.logger

        failedFeatsFile = calcParams.getFailedFeaturesFile()
        print 'failedFeatsFile', failedFeatsFile
        failedFeatsDir = os.path.dirname(failedFeatsFile)
        msg = 'Development mode failed features directory {0}' + \
              'should have been created, but it was not'
        msg = msg.format(failedFeatsDir)
        self.assertTrue(os.path.exists(failedFeatsDir), msg)

    def test_getDbCredsFile(self):
        self.calcParams.plugin.getDbCredsFile()

    def test_getSrcSqlServerPassword(self):
        # should raise an error.
        self.assertRaises(ValueError, lambda: self.calcParams.getSrcSqlServerPassword())
        fmeMacroValues = self.fmeMacroValues
        const = DataBCFMWTemplate.TemplateConstants()
        # update the macros for this test
        fmeMacroValues[const.FMWParams_SrcHost] = 'databaseHost'
        fmeMacroValues[const.FMWParams_SrcSSDbName] = 'nameOfSqlServerDB'
        fmeMacroValues[const.FMWParams_SrcSSSchema] = 'UsernameOrSchemaName'
        fmeMacroValues[const.FMWParams_SrcProxySSSchema] = 'UsernameOrSchemaName'
        del fmeMacroValues[const.FMWParams_SrcProxySSSchema]
        # update the macros in the various test objects
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues, True)
        calcParams.plugin.fmeMacroVals = fmeMacroValues
        retrievedPassword = calcParams.getSrcSqlServerPassword()
        expectedPassword = 'thisIsNotThePassword'
        msg = 'Development password retrieval returned {0} but expected {1} '
        self.assertEqual(retrievedPassword, expectedPassword, msg.format(retrievedPassword, expectedPassword))

        # now set things up for a proxy schema
        fmeMacroValues[const.FMWParams_SrcSSSchema] = 'dontUsethis'
        fmeMacroValues[const.FMWParams_SrcProxySSSchema] = 'UsernameOrSchemaName'
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues, True)
        calcParams.plugin.fmeMacroVals = fmeMacroValues
        retrievedPassword = calcParams.getSrcSqlServerPassword()
        msg = 'Development password retrieval returned {0} but expected {1} '
        self.assertEqual(retrievedPassword, expectedPassword, msg.format(retrievedPassword, expectedPassword))

        # test to trigger the heuristic
        fmeMacroValues[const.FMWParams_SrcHost] = 'databaseHost.domain.suffix'
        fmeMacroValues[const.FMWParams_SrcSSDbName] = 'nameOfSqlServerDB.domain.suffix'
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues, True)
        calcParams.plugin.fmeMacroVals = fmeMacroValues
        retrievedPassword = calcParams.getSrcSqlServerPassword()
        self.assertEqual(retrievedPassword, expectedPassword, msg.format(retrievedPassword, expectedPassword))

        # test a numbered account
        proxyKey = calcParams.getMacroKeyForPosition(const.FMWParams_SrcProxySSSchema, 4)
        schemaKey = calcParams.getMacroKeyForPosition(const.FMWParams_SrcSSSchema, 4)
        hostKey = calcParams.getMacroKeyForPosition(const.FMWParams_SrcHost, 4)
        dbNameKey = calcParams.getMacroKeyForPosition(const.FMWParams_SrcSSDbName, 4)

        fmeMacroValues[proxyKey] = 'username_4'
        fmeMacroValues[schemaKey] = 'dontUsethis'
        fmeMacroValues[hostKey] = 'hostEntry4'
        fmeMacroValues[dbNameKey] = 'serverEntry4'
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues, True)
        calcParams.plugin.fmeMacroVals = fmeMacroValues
        retrievedPassword = calcParams.getSrcSqlServerPassword(4)

    def test_getSrcSqlConnectStr(self):
        fmeMacroValues = self.fmeMacroValues
        const = DataBCFMWTemplate.TemplateConstants()
        # update the macros for this test
        fmeMacroValues[const.FMWParams_SrcHost] = 'databaseHost'
        fmeMacroValues[const.FMWParams_SrcSSDbName] = 'nameOfSqlServerDB'
        fmeMacroValues[const.FMWParams_SrcSSSchema] = 'UsernameOrSchemaName'
        fmeMacroValues[const.FMWParams_SrcPort] = '6666'
        fmeMacroValues[const.FMWParams_SrcProxySSSchema] = 'UsernameOrSchemaName'
        # del fmeMacroValues[const.FMWParams_SrcProxySSSchema]
        # update the macros in the various test objects
        calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues, True)
        calcParams.plugin.fmeMacroVals = fmeMacroValues
        retrievedPassword = calcParams.getSrcSQLServerConnectString()
        expectedConnectStr = 'databaseHost,6666'
        msg = 'expecting the values {0} but recieved {1}'
        self.assertEqual(retrievedPassword, expectedConnectStr, msg.format(expectedConnectStr, retrievedPassword))


        # expectedPassword = 'thisIsNotThePassword'
        # 3msg = 'Development password retrieval returned {0} but expected {1} '
        # self.assertEqual(retrievedPassword, expectedPassword, msg.format(retrievedPassword, expectedPassword))


class Test_TemplateConfigFileReader(unittest.TestCase):

    def setUp(self):
        confFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\wrk\newTemplate\templateDefaults.config'
        self.confFileReader = DataBCFMWTemplate.TemplateConfigFileReader('DLV')
        # self.fmeMacroValues = self.fmeMacroValues_fileSrc
        # self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
# 
#         otherLogger = logging.getLogger('DataBCFMWTemplate')
#         otherLogger.setLevel(logging.DEBUG)
# 
#         self.logger.setLevel(logging.DEBUG)
# 
#         ch = logging.StreamHandler()
#         formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
#         ch.setFormatter(formatter)
#         self.logger.addHandler(ch)
#         self.logger = logging.getLogger()
#         self.logger.debug("this is the the first log message")
# 
#         otherLogger.addHandler(ch)

    def test_getDestinationDatabaseKey(self):
        testList = []
        testList.append(['delivery', 'DELIV', 'del', 'bcgwdlv', 'bcgwdlv1', 'bcgwdlvr1', 'bcgwdlvr', 'dlvr', 'dlv' ])
        testList.append(['test', 'tst', 'bcgwtst', 'bcgwtest', 'bcgwtest1', 'idwtest1', 'idwtest'])
        testList.append(['idwprod1, idwprod1, idwprd1, idwprod, idwprd, prod, production, prd, prdct'])

        for testKeys in testList:
            retValList = []
            for key in testKeys:
                authKey = self.confFileReader.getDestinationDatabaseKey(key)
                retValList.append(authKey)
            retValList = list(set(retValList))
            msg = 'problem with the getDestinationDatabaseKey, should consitently ' + \
                  'return the same value.  But its not, unique values are: {0}'
            msg = msg.format(retValList)
            self.assertEqual(len(retValList), 1, msg)
            # modify the last value should cause a failure
            failkey = testKeys[0] + 'blahblahblah'
            authKey = self.confFileReader.getDestinationDatabaseKey(failkey)
            msg = 'returning a valid key for a value that it should not ' + \
                  'returned key is: {0}, sent key is {1}'
            msg = msg.format(authKey, failkey)
            self.assertFalse(authKey, msg)

    def test_getServer(self):
        host = self.confFileReader.getDestinationHost()
        print 'host', host

    def test_getPmpResource(self):
        testDBEnv = 'prd'
        testValue = 'DUMMY'
        expectedValue = [testValue]
        testParamEntry = self.confFileReader.const.ConfFileSection_pmpResKey
        self.confFileReader.parser.set(testDBEnv, testParamEntry, testValue)
        pmpRes = self.confFileReader.getDestinationPmpResource(testDBEnv)
        msg = 'test should return {0} but instead returned {1}'
        msg = msg.format(expectedValue, pmpRes)
        self.assertEqual(expectedValue, pmpRes, msg)
        
        msg = 'expecting method to return type: str but instead got a {0}. ' + \
              'value returned is: {1}'
        msg = msg.format(type(pmpRes), pmpRes)
        self.assertTrue(isinstance(pmpRes, list), msg)
        
        testValue = 'DUMMY, DUMMY'
        expectedValue = ['DUMMY', 'DUMMY']
        self.confFileReader.parser.set(testDBEnv, testParamEntry, testValue)
        pmpRes = self.confFileReader.getDestinationPmpResource(testDBEnv)
        msg = 'test should return {0} but instead returned {1}'
        msg = msg.format(expectedValue, pmpRes)
        self.assertEqual(expectedValue, pmpRes, msg)
        
        msg = 'expecting method to return type: list but instead got a {0}. ' + \
              'value returned is: {1}'
        msg = msg.format(type(pmpRes), pmpRes)
        self.assertTrue(isinstance(pmpRes, list), msg)

    def test_getOraclePort(self):
        oraPort = self.confFileReader.getDestinationOraclePort()
        print 'oraPort', oraPort

    def test_getSDEPort(self):
        sdePort = self.confFileReader.getDestinationSDEPort()
        print 'sdePort', sdePort

    def test_getInstance(self):
        inst = self.confFileReader.getDestinationServiceName()
        print 'instance', inst

    def test_getPmpToken(self):
        pmptoken = self.confFileReader.getPmpToken('matar')
        print 'pmptoken', pmptoken
        self.assertRaises(ValueError, lambda: self.confFileReader.getPmpToken('junk'))

    def test_getValidKeys(self):
        keys = self.confFileReader.getValidKeys()

    def test_validateKey(self):
        self.confFileReader.validateKey('dlv')
        self.assertRaises(ValueError, lambda: self.confFileReader.validateKey('dlvv'))
        self.confFileReader.validateKey('deliv')
        # self.confFileReader.validateKey('dlvv')

    def test_getFMEServerNode(self):
        isdatabcFMEServer = self.confFileReader.isDataBCFMEServerNode()
        node = platform.node()
        print 'is a fme server node: {0}'.format(isdatabcFMEServer)

    def test_isDataBCNode(self):
        isdatabc = self.confFileReader.isDataBCNode()
        node = platform.node()
        print 'is a fme server node: {0}'.format(isdatabc)

    def test_getDWMDestinationKey(self):
        dwmKey = self.confFileReader.getDWMDestinationKey('dbcdlv')
        print 'dwmKey', dwmKey

    def test_calcPuttyExecPath(self):
        # fmeMacroValues = self.fmeMacroValues_DBSrc
        # calcParams = DataBCFMWTemplate.CalcParams(fmeMacroValues)
        # calcParams.logger = self.logger

        puttyPath = self.confFileReader.calcPuttyExecPath()
        self.logger.debug("puttyPath %s", puttyPath)
        puttyPathExists = os.path.exists(puttyPath)
        msg = 'The the puttin file cannot be found.  This is a problem ' + \
              "for any scripts that require putty to build ssh tunnels"
        self.assertTrue(puttyPathExists, msg)

class Test_Startup(unittest.TestCase):
    def setUp(self):

        class fme(object):
            def __init__(self):
                self.macroValues = {  'DEST_DB_ENV_KEY': 'DELIV',
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
        self.fme = fme()
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

    def test_Startup(self):
        print 'present!'
        start = DataBCFMWTemplate.Start(self.fme)
        start.startup()

class Test_Shutdown(unittest.TestCase):
    def setUp(self):
        class fme(object):
            def __init__(self):
                pass
        self.fme = fme()
        self.fme.macroValues = {  'DEST_DB_ENV_KEY': 'DELIV',
                                'DEST_FEATURE_1': 'CLAB_INDIAN_RESERVES',
                                'DEST_SCHEMA': 'WHSE_ADMIN_BOUNDARIES',
                                'DEST_TYPE': 'SDE30',
                                'DATASET_KEYWORD_FILEGDB_1': 'FILEGDB_1',
                                'DATASET_KEYWORD_SDE30_1': 'SDE30_1',
                                'DEST_DB_ENV_KEY': 'DELIV',
                                'FME_BASE': 'no',
                                'DEST_INSTANCE': 'bcgwldv.bcgov',
                                'DEST_PASSWORD': 'lat#49',
                                'DEST_PORT': 'port:5156',
                                'DEST_SCHEMA': 'WHSE_ADMIN_BOUNDARIES',
                                'DEST_SERVER': 'gis.bcgov',
                                'DEST_TYPE': 'SDE30',
                                'DestDataset': '',
                                'DestDataset_SDE30_1': 'bcgw.bcgov',
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
        self.fme.elapsedRunTime = 5.1848905
        self.fme.featuresRead = {   'CLAB_INDIAN_RESERVES': 1588L}
        self.fme.featuresWritten = {}
        self.fme.logFileName = 'Z:\\Workspace\\kjnether\\proj\\FMETemplateRevision\\wrk\\newTemplate\\fmws\\outputs\\log\\clab_indian_reserves_staging_fgdb_bcgwdlv_Development.log'
        self.fme.mappingFileId = 'clab_indian_reserves_staging_fgdb_bcgw'
        self.fme.numFeaturesLogged = 0L
        self.fme.status = True
        self.fme.totalFeaturesRead = 1588L
        self.fme.totalFeaturesWritten = 0L
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

    def test_shutdown(self):
        print 'present!'
        # self.fme.logFileName = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\wrk\newTemplate\fmws\outputs\log\clab_indian_reserves_staging_fgdb_bcgwdlv_Development.log'
        # shutdown = DataBCFMWTemplate.Shutdown(self.fme)
        # shutdown.shutdown()
        self.logger.info("running the test of shutdown")
        # now try with the dbcdlv key
        self.fme.macroValues['DEST_DB_ENV_KEY'] = 'DLV'
        shutdown = DataBCFMWTemplate.Shutdown(self.fme)
        shutdown.shutdown()

    def test_dbConn(self):
        shutdown = DataBCFMWTemplate.Shutdown(self.fme)
        dwmWriter = DataBCFMWTemplate.DWMWriter(self.fme)
        self.fme.macroValues['DEST_DB_ENV_KEY'] = 'DBCDLV'
        dwmWriter = DataBCFMWTemplate.DWMWriter(self.fme)

class Test_PMPHelper(unittest.TestCase):

    def setUp(self):
        self.confFileReader = DataBCFMWTemplate.TemplateConfigFileReader('DLV')
        # pmpUrl = self.confFileReader.getPmpBaseUrl()
        # pmpRestDir = self.confFileReader.getPmpRestDir()
        # pmpToken = self.confFileReader.getPmpToken()

        self.pmpHelper = DataBCFMWTemplate.PMPHelper(self.confFileReader, 'DLV')

    def test_getSSHKey(self):
        sshKeyFile = self.pmpHelper.getSSHKey()
        keyExists = os.path.exists(sshKeyFile)
        msg = 'The key file {0} does not exist, therefor was not created'
        msg = msg.format(sshKeyFile)
        self.assertTrue(keyExists, msg)
        
        if os.path.exists(sshKeyFile):
            os.remove(sshKeyFile)
        self.pmpHelper.getSSHKey(sshKeyFile)
        keyExists = os.path.exists(sshKeyFile)
        msg = 'The key file when provided as an arg is not created.  keyfile:' + \
              ' {0} does not exist'
        msg = msg.format(sshKeyFile)
        self.assertTrue(keyExists, msg)

if __name__ == "__main__":
    import sys
    
    # logging config
    logger = logging.getLogger()

    #otherLogger = logging.getLogger('DataBCFMWTemplate')
    #otherLogger.setLevel(logging.DEBUG)

    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.debug("this is the the first log message")

    #otherLogger.addHandler(ch)



    # sys.argv = ['', 'Test_TemplateConfigFileReader.test_getDestinationDatabaseKey',
    #                       'Test_TemplateConfigFileReader.test_validateKey']
    # sys.argv = ['', 'Test_Shutdown.test_dbConn']
    # sys.argv = ['', 'Test_Shutdown.test_shutdown']
    # sys.argv = ['','Test_TemplateConfigFileReader.test_shutdown']
    # sys.argv = ['','Test_CalcParams.test_getSQLServerSchemaForPasswordRetrieval']
    # sys.argv = ['','Test_CalcParamsDevel.test_getSrcSqlServerPassword']


    #sys.argv =  ['', 'Test_TemplateConfigFileReader.test_getPmpResource']
    #sys.argv = ['', 'Test_TemplateConfigFileReader.test_calcPuttyExecPath']
    # sys.argv = ['','Test_Shutdown.test_shutdown']


    # sys.argv = ['', 'Test_TemplateConfigFileReader.test_isDataBCNode', 'Test_TemplateConfigFileReader.test_getFMEServerNode']

    # 'Test_CalcParams.test_getFailedFeaturesFile',
    #
    
    unittest.main()

    # suite = unittest.TestSuite()
    # suite.addTest(Test_CalcParams('test_getSourcePasswordHeuristic'))
    # suite.addTest(Test_TemplateConfigFileReader('test_validateKey'))
    # suite.addTest(Test_CalcParamsDevel('test_getDestinationPassword'))

    # unittest.TextTestRunner().run(suite)


