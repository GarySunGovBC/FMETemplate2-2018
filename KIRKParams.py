'''
Created on Sep 12, 2018

@author: kjnether

This package provides a easy to use interface to retrieving parameters related to KIRK
'''

import KirkUtil.PyKirk
import KirkUtil.constants
import DataBCFMWTemplate
import logging


class KIRKParams(object):
    '''
    
    :ivar fmeMacros: A reference to the FME macros / published parameters as defined:
                     
    '''
    
    def __init__(self, FMEMacros):
        self.fmeMacros = FMEMacros
        self.fmeFrameworkConfig = None
        self.kirk = None
        self.pubParams = GetPublishedParams(self.fmeMacros)
        self.readFrameworkConfig()
        self.jobId = None
        self.source = None
        self.job = None
        
    def getJobId(self):
        '''
        retrieves the job id and populates the property jobId
        
        '''
        if self.jobId is None:
            self.jobId = self.pubParams.getKirkJobId()
        return self.jobId
        
    def readFrameworkConfig(self):
        '''
        many settings are defined through the config file.  This method creates 
        a reference to the class that is used to retrieve methods from the 
        framework config file.
        '''
        # template config file reading requires a dest_db_env_key be provided when 
        # it gets created.  With kirk the dest_db_env_key is defined with the record
        # associated with the job. For this reason the initial template config reader
        # gets created with a dummy key, then after the actual key is retrieved it 
        # repopulates the dest db env key with the correct value.
        self.fmeFrameworkConfig = DataBCFMWTemplate.TemplateConfigFileReader('DLV')
        
    def getKirk(self):
        if self.kirk is None:
            kirkUrl = self.fmeFrameworkConfig.getKirkUrl()
            kirkToken = self.fmeFrameworkConfig.getKirkToken()
            self.kirk = KirkUtil.PyKirk.Kirk(kirkUrl, kirkToken)
        
    def getJob(self):
        if self.job is None:
            jobId = self.getJobId()
            
            self.getKirk()
            kirkJobs = self.kirk.getJobs()
            jobData = kirkJobs.getJob(jobId)
            self.job = KirkJob(jobData)
    
    def getDestDbEnvKey(self):
        '''
        Using the job id, queries Kirk through the api to determine what the 
        DEST_DB_ENV_KEY is configured for the job
        '''
        # macros {'FME_MF_DIR_MASTER_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws<solidus>', 'FME_HOME_UNIX_USERTYPED': 'E:/sw_nt/FME2017', 'FME_MF_NAME_MASTER_ENCODED': 'wb-xlate-1536775436785_9920', 'FME_HOME_ENCODED': 'E:<backslash>sw_nt<backslash>FME2017<backslash>', 'FME_MF_NAME_MASTER': 'APP_KIRK__FGDB.fmw', 'FME_MF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws<solidus>', 'FME_MF_DIR_UNIX_MASTER': 'Z:/Workspace/kjnether/proj/APP_KIRK/src/fmws', 'FME_CF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws<solidus>', 'FME_HOME': 'E:\\sw_nt\\FME2017\\', 'KIRK_JOBID': '1', 'SRC_FEATURE_1': 'community_watersheds_cancelled', 'FME_HOME_DOS': 'E:\\sw_nt\\FME2017', 'FME_HOME_UNIX_ENCODED': 'E:<solidus>sw_nt<solidus>FME2017', 'FME_PRODUCT_NAME': 'FME(R) 2017.1.0.0', 'FME_MF_DIR_USERTYPED': 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/', 'FME_MF_DIR_MASTER': 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/', 'FME_MF_NAME': 'APP_KIRK__FGDB.fmw', 'FME_MF_DIR_UNIX': 'Z:/Workspace/kjnether/proj/APP_KIRK/src/fmws', 'FME_CF_DIR': 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/', 'FME_MF_DIR_DOS': 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws', 'FME_HOME_USERTYPED': 'E:\\sw_nt\\FME2017\\', 'FME_MF_DIR_UNIX_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether<solidus>proj<solidus>APP_KIRK<solidus>src<solidus>fmws', 'FME_MF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws<solidus>', 'FME_MF_DIR_DOS_MASTER': 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws', 'FME_BASE': 'no', 'FME_MF_DIR': 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/', 'FME_MF_DIR_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws<solidus>', 'FME_MF_NAME_ENCODED': 'wb-xlate-1536775436785_9920', 'DestDataset_GEODATABASE_FILE': '$(FME_MF_DIR)out4.gdb', 'FME_CF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws<solidus>', 'SRC_DATASET_FGDB_1': '\\\\data.bcgov\\data_staging_ro\\BCGW\\administrative_boundaries\\Community_Watersheds.gdb', 'FME_CF_DIR_MASTER': 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/', 'FME_BUILD_NUM': '17539', 'FME_HOME_DOS_ENCODED': 'E:<backslash>sw_nt<backslash>FME2017', 'FME_HOME_USERTYPED_ENCODED': 'E:<solidus>sw_nt<solidus>FME2017', 'FME_MF_DIR_DOS_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws', 'FME_DESKTOP': 'no', 'FME_BUILD_DATE': '20170731', 'FME_MF_DIR_DOS_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws', 'FME_HOME_UNIX': 'E:/sw_nt/FME2017', 'FME_MF_DIR_MASTER_USERTYPED': 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/', 'FME_PRODUCT_NAME_ENCODED': 'FME<openparen>R<closeparen><space>2017.1.0.0', 'FME_BUILD_NUM_ENCODED': '17539', 'FME_MF_DIR_UNIX_MASTER_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether<solidus>proj<solidus>APP_KIRK<solidus>src<solidus>fmws', 'FME_BUILD_DATE_ENCODED': '20170731', 'SCHEMA_MAPPER_CSV': '$(FME_MF_DIR)schemaMapper.csv'}
        self.getJob()
        destDbEnv = self.job.getDestDbEnvKey()
        return destDbEnv
    
    def getSource(self):
        if self.source is None:
            self.getKirk()
            jobId = self.getJobId()
            kirkJobs = self.kirk.getJobs()
            jobSources = kirkJobs.getJobSources(jobId)
            if len(jobSources) > 1:
                msg = 'There is more than one source configured for the job {0} there should only be ' + \
                      'one source configured per job'
                msg = msg.format()
                self.logger.error(msg)
                raise ValueError, msg
            src = KirkSources(jobSources[0])
            self.source = src
    
    def getSourceFilePath(self):
        '''
        :return: the path to the source data set
        '''
        self.getSource()
        srcPath = self.source.getSourcePath()
        return srcPath
    
    def getSourceFeatureClass(self):
        '''
        :return: the source table associated with this job
        '''
        self.getSource()
        srcFc = self.source.getSourceFeatureClass()
        return srcFc

    def getDestinationSchema(self):
        '''
        :return: the destination schema for this job
        '''
        self.getJob()
        destSchema = self.job.getDestSchema()
        return destSchema
    
    def getDestFeatureClass(self):
        '''
        :return: the destination table for this job
        '''
        self.getJob()
        destFeatureClass = self.job.getDestTable()
        return destFeatureClass


class KirkSources(object):
    '''
    interface to source objects returned by kirk api
    '''

    def __init__(self, sourceData):
        self.logger = logging.getLogger(__name__)
        self.srcConst = KirkUtil.constants.SourceProperties
        self.sourceData = sourceData
        
    def getSourcePath(self):
        '''
        :return: the source file path
        '''
        return self.sourceData[self.srcConst.sourceFilePath.name]
    
    def getSourceFeatureClass(self):
        return self.sourceData[self.srcConst.sourceTable.name]

        
class KirkJob(object):
    '''
    interface to the data returned about a job in the kirk api
    example of a job object:
     {
        "jobid": 132,
        "jobStatus": "PENDING",
        "jobLabel": "wq_wqo_rpt_index_sp_staging_gdb_bcgw",
        "cronStr": "0 15 18 ? * 2,3,4,5,6",
        "date_created": "2018-09-12T00:22:38.709167Z",
        "date_modified": "2018-09-12T00:22:38.717165Z",
        "sources": [
            {
                "sourceid": 131,
                "jobid": 132,
                "sourceTable": "WQO_Polygons",
                "sourceType": "FGDB",
                "sourceDBSchema": null,
                "sourceDBName": null,
                "sourceDBHost": null,
                "sourceDBPort": null,
                "sourceFilePath": "\\\\data.bcgov\\data_staging\\BCGW\\fresh_water_and_marine\\WaterQualityObjectivesReports.gdb"
            }
        ],
        "owner": "spock",
        "destField": "DLV",
        "destTableName": "WQ_WQO_RPT_INDEX_SP",
        "destSchema": "WHSE_WATER_MANAGEMENT"
    }
    '''
    
    def __init__(self, jobData):
        self.logger = logging.getLogger(__name__)
        self.jobData = jobData
        self.jobConst = KirkUtil.constants.JobProperties
        
    def getDestDbEnvKey(self):
        '''
        :return: The destination database environment key that has been configured 
                 for the job.   Dest env key is an lookup value that is used to look 
                 up database connection parameters that correspond with the key word
                 that was configured for that database.  Frequent keys are: 
                   - DLV
                   - TST
                   - PRD
        '''
        return self.jobData[self.jobConst.destField.name]
        
    def getDestTable(self):
        '''
        :return: the destination table for this job
        '''
        return self.jobData[self.jobConst.destTableName.name]
    
    def getDestSchema(self):
        '''
        :return: the destination schema for this job
        '''
        return self.jobData[self.jobConst.destSchema.name]
        
        
class GetPublishedParams(DataBCFMWTemplate.GetPublishedParams):
    '''
    Extending the published parameters to include the 
    
    '''
    
    def __init__(self, fmeMacros):
        DataBCFMWTemplate.GetPublishedParams.__init__(self, fmeMacros)
        
    def getKirkJobId(self):
        kirkJobIdKey = self.const.FMWParams_KirkJobId
        kirkJobIdKey = self.getMacroKeyForPosition(kirkJobIdKey)
        kirkJobId = self.getFMEMacroValue(kirkJobIdKey)
        return kirkJobId
        
        
