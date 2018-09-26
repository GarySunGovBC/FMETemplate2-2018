'''
Created on Sep 12, 2018

@author: kjnether

This package provides a easy to use interface to retrieving parameters
related to KIRK
'''
import os.path

import KirkUtil.PyKirk
import KirkUtil.constants
import DataBCFMWTemplate
import logging
import csv


class KIRKParams(object):
    '''

    :ivar fmeMacros: A reference to the FME macros / published parameters
                     as defined by the fmw
    :ivar fmeFrameworkConfig: a reference to the template config file
                              reader object.  Used as an interface to
                              config file information.
    :type fmeFrameworkConfig: DataBCFMWTemplate.TemplateConfigFileReader
    :ivar ivar: the kirk api wrapper
    :type kirk: KirkUtil.PyKirk.Kirk
    :ivar pubParams: object used for retrieving published parameters defined
                     in the fme macros parameter
    :ivar jobId: the kirk job id
    :ivar source: source object that describes the sources associated with
                  the current job
    :ivar job: the kirk job data, and methods for extracting it.
    :ivar fieldMap: the kirk fieldmap info for the current job
    '''

    def __init__(self, FMEMacros):
        self.logger = logging.getLogger(__name__)
        self.fmeMacros = FMEMacros
        self.fmeFrameworkConfig = None
        self.kirk = None
        self.pubParams = GetPublishedParams(self.fmeMacros)
        self.readFrameworkConfig()
        self.jobId = None
        self.source = None
        self.job = None
        self.fieldMap = None
        self.counterTransformers = None

    def getJobId(self):
        '''
        :return: job id and populates the property jobId

        '''
        if self.jobId is None:
            self.jobId = self.pubParams.getKirkJobId()
        return self.jobId

    def readFrameworkConfig(self):
        '''
        many settings are defined through the config file.  This method
        creates a reference to the class that is used to retrieve methods
        from the framework config file.
        '''
        # template config file reading requires a dest_db_env_key be
        # provided when it gets created.  With kirk the dest_db_env_key is
        # defined with the record associated with the job. For this reason
        # the initial template config reader gets created with a dummy key,
        # then after the actual key is retrieved it repopulates the dest db
        # env key with the correct value.
        if self.fmeFrameworkConfig is None:
            self.fmeFrameworkConfig = \
                DataBCFMWTemplate.TemplateConfigFileReader('DLV')

    def getKirk(self):
        '''
        populates the property kirk with a reference to the base class
        used to communicate with the kirk api:
        KirkUtil.PyKirk.Kirk
        '''
        if self.kirk is None:
            kirkUrl = self.fmeFrameworkConfig.getKirkUrl()
            kirkToken = self.fmeFrameworkConfig.getKirkToken()
            self.kirk = KirkUtil.PyKirk.Kirk(kirkUrl, kirkToken)

    def getJob(self):
        '''
        When this class is instantiated it receives the macros that have been
        populated in the FMW, the jobid is contained in those macros, this
        method identifies the jobid from the macros and populates the
        property self.job with a KirkJob object that can be used to
        retrieve the metadata about the replication task.
        '''
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
        # macros {'FME_MF_DIR_MASTER_USERTYPED_ENCODED': 'Z:<backslash>Work
        # space<backslash>kjnether<backslash>proj<backslash>APP_KIRK<backsl
        # ash>src<backslash>fmws<solidus>', 'FME_HOME_UNIX_USERTYPED':
        # 'E:/sw_nt/FME2017', 'FME_MF_NAME_MASTER_ENCODED': 'wb-xlate-
        # 1536775436785_9920', 'FME_HOME_ENCODED':
        # 'E:<backslash>sw_nt<backslash>FME2017<backslash>',
        # 'FME_MF_NAME_MASTER': 'APP_KIRK__FGDB.fmw', 'FME_MF_DIR_ENCODED':
        # 'Z:<backslash>Workspace<backslash>kjnether<backslash>proj<backsla
        # sh>APP_KIRK<backslash>src<backslash>fmws<solidus>',
        # 'FME_MF_DIR_UNIX_MASTER':
        # 'Z:/Workspace/kjnether/proj/APP_KIRK/src/fmws',
        # 'FME_CF_DIR_ENCODED': 'Z:<backslash>Workspace<backslash>kjnether<
        # backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmws<so
        # lidus>', 'FME_HOME': 'E:\\sw_nt\\FME2017\\', 'KIRK_JOBID': '1',
        # 'SRC_FEATURE_1': 'community_watersheds_cancelled',
        # 'FME_HOME_DOS': 'E:\\sw_nt\\FME2017', 'FME_HOME_UNIX_ENCODED':
        # 'E:<solidus>sw_nt<solidus>FME2017', 'FME_PRODUCT_NAME': 'FME(R)
        # 2017.1.0.0', 'FME_MF_DIR_USERTYPED':
        # 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/',
        # 'FME_MF_DIR_MASTER':
        # 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/',
        # 'FME_MF_NAME': 'APP_KIRK__FGDB.fmw', 'FME_MF_DIR_UNIX':
        # 'Z:/Workspace/kjnether/proj/APP_KIRK/src/fmws', 'FME_CF_DIR':
        # 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/',
        # 'FME_MF_DIR_DOS':
        # 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws',
        # 'FME_HOME_USERTYPED': 'E:\\sw_nt\\FME2017\\',
        # 'FME_MF_DIR_UNIX_ENCODED': 'Z:<solidus>Workspace<solidus>kjnether
        # <solidus>proj<solidus>APP_KIRK<solidus>src<solidus>fmws',
        # 'FME_MF_DIR_MASTER_ENCODED': 'Z:<backslash>Workspace<backslash>kj
        # nether<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>
        # fmws<solidus>', 'FME_MF_DIR_DOS_MASTER':
        # 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws', 'FME_BASE':
        # 'no', 'FME_MF_DIR':
        # 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/',
        # 'FME_MF_DIR_USERTYPED_ENCODED': 'Z:<backslash>Workspace<backslash
        # >kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backsla
        # sh>fmws<solidus>', 'FME_MF_NAME_ENCODED': 'wb-xlate-
        # 1536775436785_9920', 'DestDataset_GEODATABASE_FILE':
        # '$(FME_MF_DIR)out4.gdb', 'FME_CF_DIR_MASTER_ENCODED': 'Z:<backsla
        # sh>Workspace<backslash>kjnether<backslash>proj<backslash>APP_KIRK
        # <backslash>src<backslash>fmws<solidus>', 'SRC_DATASET_FGDB_1': '\
        # \\\data.bcgov\\data_staging_ro\\BCGW\\administrative_boundaries\\
        # Community_Watersheds.gdb', 'FME_CF_DIR_MASTER':
        # 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/',
        # 'FME_BUILD_NUM': '17539', 'FME_HOME_DOS_ENCODED':
        # 'E:<backslash>sw_nt<backslash>FME2017',
        # 'FME_HOME_USERTYPED_ENCODED': 'E:<solidus>sw_nt<solidus>FME2017',
        # 'FME_MF_DIR_DOS_MASTER_ENCODED': 'Z:<backslash>Workspace<backslas
        # h>kjnether<backslash>proj<backslash>APP_KIRK<backslash>src<backsl
        # ash>fmws', 'FME_DESKTOP': 'no', 'FME_BUILD_DATE': '20170731',
        # 'FME_MF_DIR_DOS_ENCODED': 'Z:<backslash>Workspace<backslash>kjnet
        # her<backslash>proj<backslash>APP_KIRK<backslash>src<backslash>fmw
        # s', 'FME_HOME_UNIX': 'E:/sw_nt/FME2017',
        # 'FME_MF_DIR_MASTER_USERTYPED':
        # 'Z:\\Workspace\\kjnether\\proj\\APP_KIRK\\src\\fmws/',
        # 'FME_PRODUCT_NAME_ENCODED':
        # 'FME<openparen>R<closeparen><space>2017.1.0.0',
        # 'FME_BUILD_NUM_ENCODED': '17539',
        # 'FME_MF_DIR_UNIX_MASTER_ENCODED': 'Z:<solidus>Workspace<solidus>k
        # jnether<solidus>proj<solidus>APP_KIRK<solidus>src<solidus>fmws',
        # 'FME_BUILD_DATE_ENCODED': '20170731', 'SCHEMA_MAPPER_CSV':
        # '$(FME_MF_DIR)schemaMapper.csv'}
        self.getJob()
        destDbEnv = self.job.getDestDbEnvKey()
        # the way that kirk works is the dest_db_env_key is not set when the
        # job is initiated.  It gets retrieve by this method.  In order to
        # retrieve it we need to read the TemplateConfig file. The api was
        # created so that it requires a env key to be provided when a
        # template config is created.  Kirk creates the template config
        # reader with a dummy value then when the env key gets retrieved
        # in this method we are populating the template config reader
        # with the correct value for the current job.
        self.fmeFrameworkConfig.setDestinationDatabaseEnvKey(destDbEnv)
        return destDbEnv

    def getSource(self):
        '''
        gets the source object from the kirk api, and stores the subsequent
        data in a KirkSources object.  This is then available to subsequent
        methods that may need source information.  Source object is stored
        in an object property
        '''
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
                raise ValueError(msg)
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
    
    def getSourceProjection(self):
        '''
        :return: the source projection
        '''
        self.getSource()
        srcProj = self.source.getSourceProjection()
        return srcProj

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

    def getFieldMapCSVPath(self):
        '''
        :return: the path to the where the field map file will be stored if
                 one exists for the current job, Each job will have its
                 own directory for where output cache files are stored.

                 output path is:
                   <framework root>/outputs/kirk/<jobid>/<file...file>
        '''
        tmpltRootDir = self.fmeFrameworkConfig.getTemplateRootDirectory()
        outputDir = self.fmeFrameworkConfig.getOutputsDirectory()
        kirkDir = self.fmeFrameworkConfig.getKirkOutputsDirectory()
        self.logger.debug("kirkDir: %s", kirkDir)

        # path to kirk outputs
        fullPathKirkDir = os.path.join(tmpltRootDir, outputDir, kirkDir)
        fullPathKirkDir = os.path.realpath(fullPathKirkDir)
        self.logger.info("fullPathKirkDir: %s", fullPathKirkDir)
        if not os.path.exists(fullPathKirkDir):
            os.mkdir(fullPathKirkDir)
            self.logger.info("created the kirk outputs path: %s",
                             fullPathKirkDir)

        # path for this jobs output
        jobid = self.getJobId()
        jobDir = os.path.join(fullPathKirkDir, unicode(jobid))
        if not os.path.exists(jobDir):
            os.mkdir(jobDir)
            msg = "created the job specific outputs directory: %s"
            self.logger.info(msg, jobDir)
        csvFileName = KirkUtil.constants.FIELDMAP_CSV.format(jobid)
        csvFileFullPath = os.path.join(jobDir, csvFileName)
        return csvFileFullPath

    def getFieldMapAsCSV(self, includeHeader=False, refresh=True):
        '''
        Determines if a field map exists. If it does the standardized path
        to the field map is calculated, and the field map is dumped to that
        path.

        :param includeHeader: used to determine whether the output csv
                              should include a header or not
        :type includeHeader: bool
        :param refresh: if the field map file already exists should it get
                        recreated, or re-used.  refresh=True is the default
                        behaviour and will result in the fieldmap file being
                        deleted and recreated.
        :type refresh: bool

        '''

        csvFile = self.getFieldMapCSVPath()
        if os.path.exists(csvFile) and refresh:
            os.remove(csvFile)
        if not os.path.exists(csvFile):
            # create it
            self.getFieldMap()
            fm = self.fieldMap.getFieldMapsAsCSV()
            with open(csvFile, 'wb') as f:
                writer = csv.writer(f)
                if includeHeader:
                    writer.writerow(['SRC_COLUMN', 'DEST_COLUMN'])
                writer.writerows(fm)
        return csvFile

    def getFieldMap(self):
        '''
        This method is called by any other methods that require access
        to fieldmap information.  It will retrieve and cache the
        fieldmaps for the current job in the property: 'fieldMap'
        '''
        if not self.fieldMap:
            self.getKirk()
            jobId = self.getJobId()
            kirkJobs = self.kirk.getJobs()
            jobFieldmaps = kirkJobs.getJobFieldMaps(jobId)
            fldMaps = KirkFieldMaps(jobFieldmaps)
            self.fieldMap = fldMaps

    def getFieldMapCount(self):
        '''
        :return: the number of fieldmaps that exist for the current job
        '''
        self.getFieldMapAsCSV()
        return len(self.fieldMap)

    def getCounterTransformers(self):
        '''
        Loads the counter transformers into memory if they haven't been
        loaded already.
        '''
        if self.counterTransformers is None:
            self.getKirk()
            kirkJobs = self.kirk.getJobs()
            jobId = self.getJobId()
            self.counterTransformers = kirkJobs.getCounterTransfomers(jobId)

    def getCounterTransformersCount(self):
        '''
        :return: the number of counter transformers that have been define
                 for this job
        '''
        self.getCounterTransformers()
        return len(self.counterTransformers)

    def getCounterTransformerParameters(self, paramType):
        '''
        :param paramType: the counter parameter that you would like to
                          have returned.  Returns None if no counters
                          are configured
        :type paramType: KirkUtil.constants.
        :return: the value that corresponds with the supplied paramType
        '''
        retVal = 0
        # verify the paramType is valid
        if paramType not in KirkUtil.constants.CounterTransformerMap.__members__:  # @IgnorePep8
            msg = 'you requested a return type of {0}, valid options ' + \
                  'include: {1}'
            params = KirkUtil.constants.CounterTransformerMap.__members__.keys()  # @UndefinedVariable @IgnorePep8
            msg = msg.format(paramType, params)
            raise ValueError(msg)
        self.logger.debug("counter param: %s", paramType)

        #
        self.getCounterTransformers()
        if len(self.counterTransformers) > 1:
            msg = 'There are {0} counters configured for this job, kirk ' + \
                  'currently only supports 1 per job... jobid: {1}'
            jobId = self.getJobId()
            msg = msg.format(len(self.counterTransformers), jobId)
            raise ValueError(msg)
        if len(self.counterTransformers) == 1:
            counter = self.counterTransformers[0]
            totParamCnt = KirkUtil.constants.TRANSFORMERS_DYNAMICFIELDS_LENGTH + 1  # @IgnorePep8
            nameTmplt = KirkUtil.constants.TRANSFORMER_NAME_TMPLT
            valueTmplt = KirkUtil.constants.TRANSFORMER_VALUE_TMPLT

            for paramCnt in range(1, totParamCnt):
                # turns into ts#_name and ts#_value
                paramName_name = nameTmplt.format(paramCnt)
                paramValue_name = valueTmplt.format(paramCnt)

                # now if the paramName_name == the parameter that was
                # requested
                if counter[paramName_name] == paramType:
                    retVal = counter[paramValue_name]
        return retVal


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

    def getSourceProjection(self):
        '''
        :return: the projection of the source data... pretty straight forward!
        '''
        return self.sourceData[self.srcConst.sourceProjection.name]

class KirkFieldMaps(object):

    def __init__(self, fieldMaps):
        self.logger = logging.getLogger(__name__)
        self.fieldMaps = fieldMaps
        self.fmConst = KirkUtil.constants.FieldmapProperties

    def getFieldMapsAsCSV(self, includeHeader=False):
        '''
        converts the fieldmap struct as a list of lists
        '''
        csvStruct = []
        # if there is no data then just return an empty list
        if self.fieldMaps:
            # this col list identifies what properties and what order the
            # fieldmap values should be included in the csv struct.
            cols2Include = [self.fmConst.sourceColumnName.name,
                            self.fmConst.destColumnName.name]
            if includeHeader:
                csvStruct.append(cols2Include)
            for fldMap in self.fieldMaps:
                lineList = []
                for colName in cols2Include:
                    lineList.append(fldMap[colName])
                csvStruct.append(lineList)
        return csvStruct

    def __len__(self):
        return len(self.fieldMaps)


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
                "sourceFilePath": "\\\\data.bcgov\\data_staging\\BCGW\\fresh_water_and_marine\\WaterQualityObjectivesReports.gdb"  # @IgnorePep8
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
        :return: The destination database environment key that has been
                 configured for the job.   Dest env key is an lookup value
                 that is used to look up database connection parameters that
                 correspond with the key word that was configured for that
                 database.  Frequent keys are:
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
        '''
        :returns" the job id, by extracting it from the fme macros
        '''
        kirkJobIdKey = self.const.FMWParams_KirkJobId
        kirkJobIdKey = self.getMacroKeyForPosition(kirkJobIdKey)
        kirkJobId = self.getFMEMacroValue(kirkJobIdKey)
        return kirkJobId

