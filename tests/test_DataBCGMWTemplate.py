'''
Created on Dec 11, 2015

@author: kjnether
'''
import os
import sys

pathList = os.environ['PATH'].split(';')
pathList.insert(0, r'E:\sw_nt\FME2015')
sys.path.insert(0, r'E:\sw_nt\FME2015\fmeobjects\python27')
sys.path.insert(0, r'\\data.bcgov\work\scripts\python\DataBCPyLib')
sys.path.insert(0, r'\\data.bcgov\work\Workspace\kjnether\proj\FMETemplateRevisions_Python')

os.environ['PATH'] = ';'.join(pathList)

import unittest
import site
import pprint
import DataBCFMWTemplate
import logging

class Test_CalcParams(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        
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
                                    'SRC_ORA_SCHEMA': 'inventory2000'}
        self.fmeMacroValues = self.fmeMacroValues_fileSrc
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)

    def tearDown(self):
        pass

    def test_getDestServer(self):
        host = self.calcParams.getDestinationHost()
        print 'host', host
        
    def test_getDestinationPassword(self):
        
        passw = self.calcParams.getDestinationPassword()
        print 'passw', passw
        
    def test_getSourcePassword(self):
        
        msg = "unable to retrieve the password for schema {0} and " + \
              'instance {1} using source password retrieval methods'

        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)
        #spass = self.calcParams.getSourcePassword()
        #print 'src pass', spass
        # CWI_SPI_OPD@ENVPROD1.NRS.BCGOV
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] =  'CWI_SPI_OPD'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'ENVPROD1'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'ENVPROD1'
        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME']))

        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] =  'WHSE_CORP'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'IDWPROD1.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'IDWPROD1.BCGOV'
        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME']))

        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] =  'WHSE_CORP'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'BCGW.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'BCGW.BCGOV'

        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME']))
        
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_1'] =  'WHSE_CORP'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE_1'] = 'BCGW.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_1'] = 'BCGW.BCGOV'

        spass = self.calcParams.getSourcePassword(1)
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_1'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_1']))
        
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_3'] =  'WHSE_CORP'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE_3'] = 'BCGW.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_3'] = 'BCGW.BCGOV'
        spass = self.calcParams.getSourcePassword(3)
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_3'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_3']))
        
        self.calcParams.fmeMacroVals['SRC_ORA_PROXY_SCHEMA_4'] =  'DBLINK_FISS'
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_4'] =  'FISS'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE_4'] = 'RIBPROD1.NRS.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_4'] = 'RIBPROD1.NRS.BCGOV'
        spass = self.calcParams.getSourcePassword(4)
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_4'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_4']))
        
        self.calcParams.fmeMacroVals['SRC_ORA_PROXY_SCHEMA'] =  'DBLINK_FISS'
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] =  'FISS'
        self.calcParams.fmeMacroVals['SRC_ORA_INSTANCE'] = 'RIBPROD1.NRS.BCGOV'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'RIBPROD1.NRS.BCGOV'
        spass = self.calcParams.getSourcePassword()
        self.assertIsNotNone(spass, msg.format(self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_4'], self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME_4']))
        
    def test_getSourcePasswordHeuristic(self):
        self.fmeMacroValues = self.fmeMacroValues_DBSrc
        self.calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues)
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA'] =  'CWI_SPI_OPD'
        self.calcParams.fmeMacroVals['SRC_ORA_SERVICENAME'] = 'ENVPROD1'
        self.calcParams.plugin.currentPMPResource = 'ETL_OPERATIONAL_DBLINKS'
        spass = self.calcParams.getSourcePasswordHeuristic()
        print 'pass is:', spass
        
        self.calcParams.fmeMacroVals['SRC_ORA_SCHEMA_5'] =  'CWI_SPI_OPD'
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

   
class Test_CalcParamsDevel(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        
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
                                    'SRC_ORA_SCHEMA': 'inventory2000'}
        
        
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
        self.logger.debug("fme parameter {0} was set to {1}".format(const.FMWMacroKey_FMWDirectory,dummyFMWDir))
        msg = 'The method {0} returned {1}, but the test expects it to return {2}'
        self.fmeMacroValues[const.FMWMacroKey_FMWDirectory] = dummyFMWDir
        self.logger.debug("fme parameter {0} was set to {1}".format(const.FMWMacroKey_FMWDirectory,dummyFMWDir))
        calcParams = DataBCFMWTemplate.CalcParams(self.fmeMacroValues, True)

        # Verify that changing the path to a config file that does not
        # exist raises an error
        # ----- commented out this test as there should never be a case where the 
        # ----- fmw directory points to something that does not exist.
        #origVal = self.fmeMacroValues[const.FMWMacroKey_FMWDirectory]
        #self.fmeMacroValues[const.FMWMacroKey_FMWDirectory] = os.path.dirname(self.fmeMacroValues[const.FMWMacroKey_FMWDirectory]) + 'randomText'
        #print 'fmw dir that does not exist: ', self.fmeMacroValues[const.FMWMacroKey_FMWDirectory]
        #self.assertRaises(ValueError, lambda: DataBCFMWTemplate.CalcParams(self.fmeMacroValues, True))
        
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
        #develSrcPasswd = calcParams.getSourcePassword()
        #msg = 'Trying to retrieve the password for {0}, but there is only ' + \
        #      'There is no specific entry for that source database so should ' + \
        #      'return \'None\' but intead its returning {1}'
        #msg = msg.format(calcParams.fmeMacroVals['SRC_INSTANCE'], develSrcPasswd)
        #self.assertRaises(ValueError, lambda:  calcParams.getSourcePassword())
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
        msg = 'Retrieving the password for the user {0} and the ' +\
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

    
class Test_TemplateConfigFileReader(unittest.TestCase):
    
    def setUp(self):
        confFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\wrk\newTemplate\templateDefaults.config'
        self.confFileReader = DataBCFMWTemplate.TemplateConfigFileReader('DLV')
    
    def test_getDestinationDatabaseKey(self):
        testList = []
        testList.append( ['delivery', 'DELIV', 'del', 'bcgwdlv', 'bcgwdlv1', 'bcgwdlvr1', 'bcgwdlvr', 'dlvr', 'dlv' ])
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
        pmpRes =  self.confFileReader.getDestinationPmpResource()
        print 'pmpres', pmpRes
        
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
        #self.confFileReader.validateKey('dlvv')
    

class Test_Startup(unittest.TestCase):
    def setUp(self):
        
        class fme(object):
            def __init__(self):
                self.macroValues =  {  'DEST_DB_ENV_KEY': 'DELIV',
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
        self.fme.macroValues =  {  'DEST_DB_ENV_KEY': 'DELIV',
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
            
    def test_shutdown(self):
        print 'present!'
        self.fme.logFileName = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\wrk\newTemplate\fmws\outputs\log\clab_indian_reserves_staging_fgdb_bcgwdlv_Development.log'
        shutdown = DataBCFMWTemplate.Shutdown(self.fme)
        shutdown.shutdown()
        
    def test_dbConn(self):
        shutdown = DataBCFMWTemplate.Shutdown(self.fme)
        dwmWriter = DataBCFMWTemplate.DWMWriter(self.fme)
 
if __name__ == "__main__":
    import sys
    #sys.argv = ['', 'Test_TemplateConfigFileReader.test_getDestinationDatabaseKey', 
    #                       'Test_TemplateConfigFileReader.test_validateKey']
    #sys.argv = ['', 'Test_Shutdown.test_dbConn']
    #sys.argv = ['', 'Test_Shutdown.test_shutdown']
    #sys.argv = ['','Test_TemplateConfigFileReader.test_shutdown']
    #sys.argv = ['','Test_CalcParamsDevel.test_getDbCredsFile']

    # 'Test_CalcParams.test_getFailedFeaturesFile',
    # 
    unittest.main()
    
    #suite = unittest.TestSuite()
    #suite.addTest(Test_CalcParams('test_getSourcePasswordHeuristic'))
    #suite.addTest(Test_TemplateConfigFileReader('test_validateKey'))
    #suite.addTest(Test_CalcParamsDevel('test_getDestinationPassword'))
    
    #unittest.TextTestRunner().run(suite)
    
    
