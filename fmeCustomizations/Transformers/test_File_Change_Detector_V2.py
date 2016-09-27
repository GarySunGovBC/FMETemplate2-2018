
'''
Created on Jan 22, 2016

@author: kjnether
'''


import unittest
#import File_Change_DetectorV2
import logging.config

class Test_FileChangeDetector(unittest.TestCase):
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
        #self.fileChng = File_Change_DetectorV2.ChangeFlagFetcher()
        
    def test_Init(self):
        filechangeConfFile = r'Z:\scripts\python\DataBCFmeTemplate2\fmeCustomizations\Transformers\File_Change_DetectorV2.logconfig'
        logging.config.fileConfig(filechangeConfFile)
        


if __name__ == '__main__':
    import sys
    sys.argv = ['', 'Test_FileChangeDetector']
    
    unittest.main()
