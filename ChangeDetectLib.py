"""

About
=========
:synopsis:     A library containing functionality required to implement
               file change detection for source FMW files
:moduleauthor: Kevin Netherton
:date:         1-13-2015
:description:  This library contains all the functionality necessary
               to implement file change detection custom transformer.
               This library should be kept in the same folder as the 
               fmw containing the custom transformer.
               
   
:comment:  You can enter comments in anywhere!  this is just and 
           example of how you can enter them!



.. note:: 

      This is an example of a note

.. Does your script create any outputs.  If so describe those outputs 
   here.
..

Dependencies:
-------------------
DataBCFmeTemplate - Uses this module to extract parameters from 
                    the global template config file, and to get
                    access to template constants.

      

API DOC:
===============     
"""
import DataBCFMWTemplate
import BCDCUtil.BCDCRestQuery  # @UnresolvedImport
import os.path
import datetime
import time
import logging
import pytz
import requests
import pprint

class ChangeDetect(object):
    
    def __init__(self, fmeMacroValues, changeControlLog=None):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.fmeMacroValues = fmeMacroValues
        self.const = DataBCFMWTemplate.TemplateConstants
        #self.__initself.logger()
        self.paramObj = DataBCFMWTemplate.TemplateConfigFileReader(self.fmeMacroValues[self.const.FMWParams_DestKey])
        if changeControlLog:
            self.changeLogFilePath = changeControlLog
        else:
            # create the paths if they don't exist
            self.changeLogFilePath = self.getChangeLogFilePath(True)
        msg = "Change control file is: {0}".format(self.changeLogFilePath)
        self.logger.debug(msg)
        #         changeCache = ChangeCache(self, struct)
        self.changeCache = ChangeCache(self)
        self.logger.debug("Change Detect module location {0}".format(__file__))
            
    def getChangeLogFilePath(self, create=False):
        '''
        Calculated the log file path from the parameters 
        defined in the config file.  if the create 
        arg is set to true the method will also create
        the fmw sub directory and the change log file.
        
        If the root change log directory as defined in 
        the config file does not exist will throw an 
        exception.
        
        :param  create: boolean parameter that controls 
                        whether the script will create 
                        the fme sub directory and a blank
                        change control file if it does not
                        exist.
        :type create: bool
        
        :returns: The path to the calculated change log
        :rtype: str(path)
        '''
        #changeLogRootDir = self.paramObj.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_changeLogDir)
        changeLogRootDir = self.paramObj.getChangeLogsDirFullPath()
        if create:
            if not os.path.exists(changeLogRootDir):
                msg = 'The root directory: {0} for file change logs does not exist' + \
                      'Either create it or change the parameter in the global section ' + \
                      'called {1} in the config file {2}'
                msg = msg.format(changeLogRootDir, self.const.ConfFileSection_global_changeLogDir, 
                                 self.paramObj.confFile)
                self.logger.error(msg)
                raise ValueError, msg
        fmwFileName = self.fmeMacroValues[self.const.FMWMacroKey_FMWName]
        fmwFileNameNoSuffix, suffix = os.path.splitext(fmwFileName)
        changeLogFMWDir = os.path.join(changeLogRootDir, fmwFileNameNoSuffix)
        if create:
            if not os.path.exists(changeLogFMWDir):
                msg = 'The FMW directory for change logs {0} does not exist.' + \
                      'Creating the directory now'
                msg = msg.format(changeLogFMWDir)
                self.logger.info(msg)
                os.mkdir(changeLogFMWDir)
        
        #changeLogFileName = self.paramObj.parser.get(self.const.ConfFileSection_global, self.const.ConfFileSection_global_changeLogFileName)
        changeLogFileName = self.paramObj.getChangeLogFile()
        fullPathToChangeLog = os.path.join(changeLogFMWDir, changeLogFileName)
        if create:
            if not os.path.exists(fullPathToChangeLog):
                msg = 'There is no file change log currently created for the ' + \
                      'fmw: {0}, creating one now!  The full path to the file that ' + \
                      'is being created is {1}'
                msg = msg.format(fmwFileName, fullPathToChangeLog)
                self.logger.info(msg)
                fh = open(fullPathToChangeLog, 'w')
                fh.close()
        return fullPathToChangeLog
    
    def getFileModificationUTCDateTime(self, inFilePath):
        '''
        Reads the file modification times of the file path
        that was provided as an arg. Returns a datetime
        object defining the modification date
        
        :param  inFilePath: string describing an input file path
        :type inFilePath: string
        
        :returns: datetime object that contains the file modification 
                  date
        :rtype: datetime
        '''
        # convert to date
        #TODO: Should I ever be worried about the timestamp returning 
        #      as something other than pacific standard time.
        # getmtime should return UTC (aka gmt) time.
        # http://stackoverflow.com/questions/11405049/how-to-get-the-file-modification-date-in-utc-from-python
        modTimeStamp = os.path.getmtime(inFilePath)
        #mtime = os.stat(inFilePath)[stat.ST_MTIME]
        # fromtimestamp - returns local time
        # utcfromtimestamp - returns datetime in utc time zone
        #timeStamp = datetime.datetime.utcfromtimestamp(modTimeStamp)
        timeStamp = datetime.datetime.utcfromtimestamp(modTimeStamp)
        timeStamp = pytz.utc.localize(timeStamp)
        return timeStamp
        
    def readChangeLog(self, changeLogPath=None):
        '''
        reads newly formatted change logs
        
        '''
        self.logger.debug("reading the changelog")
        struct = {}
        if not changeLogPath:
            changeLogPath = self.changeLogFilePath
        self.logger.info('change log: {0}'.format(changeLogPath))
        with open(changeLogPath, 'r') as changeFile:
            for line in changeFile:
                line = line.strip()
                lineElems = line.split(',')
                replicated = lineElems[7]
                if replicated.lower() == 'false':
                    replicated = False
                else:
                    replicated = True
                    
                # if the dataset was not replicated then 
                # shouldn't be considered
                if replicated:
                    sourceFilePath = lineElems[3].strip()
                    #sourceFilePathNorm = os.path.normpath(sourceFilePath)
                    #sourceFilePathNorm = os.path.normcase(sourceFilePathNorm)
                    sourceFilePathNorm = self.formatDataSet(sourceFilePath)
                    sourceFileTimeStamp = int(lineElems[4])
                    sourceFileDateTime = datetime.datetime.utcfromtimestamp(sourceFileTimeStamp)
                    sourceFileDateTime = pytz.utc.localize(sourceFileDateTime)
                    if struct.has_key(sourceFilePathNorm):
                        if sourceFileDateTime > struct[sourceFilePathNorm]:
                            struct[sourceFilePathNorm] = sourceFileDateTime
                    else:
                        struct[sourceFilePathNorm] = sourceFileDateTime
        #changeCache = ChangeCache(self, struct)
        self.changeCache.repopulateChangeDict(struct)
        return self.changeCache
    
    def readOldChangeLog(self, changeLogPath=None):
        '''
        Reads the old change log format that is made up
        of the following columns: 
           Date string - datestring for when the change detection
                         was run
           FMW Name - not the full path, just the name of the 
                      fmw.
           source data - path to the source data
           source data time stamp - the modification date on 
                                    the source file at the time
                                    of running this script.  this
                                    is the local time timestamp
          Change detected - a boolean value that indicates whether change
                             was detected or not.
           
        '''
        self.logger.debug("reading the changelog")
        struct = {}
        if not changeLogPath:
            changeLogPath = self.changeLogFilePath
        self.logger.info('change log: {0}'.format(changeLogPath))
        with open(changeLogPath, 'r') as changeFile:
            for line in changeFile:
                line = line.strip()
                lineElems = line.split(',')
                replicated = lineElems[4]
                if replicated.lower() == 'false':
                    replicated = False
                else:
                    replicated = True
                # if the dataset was not replicated then 
                # shouldn't be considered
                if replicated:
                    sourceFilePath = lineElems[2].strip()
                    #sourceFilePathNorm = os.path.normpath(sourceFilePath)
                    #sourceFilePathNorm = os.path.normcase(sourceFilePathNorm)
                    sourceFilePathNorm = self.formatDataSet(sourceFilePath)
                    sourceFileTimeStamp = int(lineElems[3])
                    sourceFileDateTime = datetime.datetime.fromtimestamp(sourceFileTimeStamp)

                    if struct.has_key(sourceFilePathNorm):
                        if sourceFileDateTime > struct[sourceFilePathNorm]:
                            struct[sourceFilePathNorm] = sourceFileDateTime
                    else:
                        struct[sourceFilePathNorm] = sourceFileDateTime
        changeCache = ChangeCache(self, struct)
        return changeCache
                    
    def hasSameDestination(self, changeLogRecordList):
        # 0 - date of last change detection attempt
        # 1 - source fmw
        # 2 - source dataset
        # 3 - source dataset last mod timestamp
        # 4 - was replicated at this date
        # 5 - destination key word.  If its not there assumption
        #     is the destination is prod.
        retVal = False
        self.paramObj
        if len(changeLogRecordList) < 6:
            destKey = self.paramObj.getDestinationDatabaseKey('prod')
        else:
            destKey = self.paramObj.getDestinationDatabaseKey(changeLogRecordList[5].strip())
        curDestKey = self.fmeMacroValues[self.const.FMWParams_DestKey]
        curDestKey = self.paramObj.getDestinationDatabaseKey(curDestKey)
        if curDestKey.lower() == destKey.lower():
            retVal = True
        return retVal
    
    def formatDataSet(self, fmeDataSet):
        '''
        Thsi is the parameter that is extracted from an fme objects
        feature object, using the feature.getattribute('fme_dataset')
        
        needs formatting so that it can be referred to consistently
        
        Was having problems when the paths were provided using 
        \\ charcters.  When trying to retrieve a value in a 
        dictionary and the dictioanry key had \\ in it.  Sometimes
        lookups failed because the \\ were being interpreted as\\\\
        
        All analysis of windows file paths is now being completed 
        using / slashes.  os.path functions all work consistently 
        wiht the forward slash syntax, example exists, isdir, ...
        '''
        #self.logger.debug("orignal path: {0}".format(fmeDataSet))
        dataPath = os.path.normcase(fmeDataSet)
        #self.logger.debug("case normalized path: {0}".format(fmeDataSet))
        dataPath = os.path.normpath(dataPath)
        #self.logger.debug("normalized path: {0}".format(fmeDataSet))
        dataPath = dataPath.replace('\\', '/')
        #self.logger.debug("path with forward slashes: {0}".format(dataPath))
        return dataPath
    
#     def normalizeFeatureClassName(self, fcName):
#         self.logger.debug("unmodified feature path: {0}".format(fcName))
#         fcName = fcName.encode('string-escape')
#         self.logger.debug("converted to raw string: {0}".format(fcName))
#         fcName = os.path.normcase(fcName)
#         self.logger.debug("normalized case path: {0}".format(fcName))
#         #dataPath = os.path.normcase(dataPath)
#         #dataPath = os.path.normpath(dataPath)
#         fcName = os.path.normpath(fcName)
#         self.logger.debug("normalized path: {0}".format(fcName))
#         # return fcName.encode('string-escape')
#         return fcName
    
class ChangeDetectBCDC(ChangeDetect):
    
    def __init__(self, fmeMacroValues, changeControlLog=None):
        ChangeDetect.__init__(self, fmeMacroValues, changeControlLog)
        self.changeCache = ChangeCache(self)
    
    def getFileModificationUTCDateTime(self, inFilePath):
        '''
        queries the rest api for the revision date and returns it 
        as a utc datetime object
        '''
        # validate the source path
        # TODO: should think about this transformer!  Will it work external
        # to dat
        #self.logger.debug("Source dataset is {0}".format(inFilePath))
        revisionUTCTimeStamp = None
        resp = requests.head(inFilePath)
        if resp.status_code <> 200:
            msg = 'Unable to connect to the source dataset {0}'
            raise ValueError, msg.format(inFilePath)
        else:
            revisionDate = self.getResourceRevisionDate(inFilePath)
            # if need the timestamp this is how to get it
            #revisionUTCTimeStamp = time.mktime(revisionDate.timetuple())
        #return revisionUTCTimeStamp
        return revisionDate
            
    def getResourceRevisionDate(self, resourceName):
        '''
        Recieves the name of a file that exists in BCDC as a 
        download.  Searches for the resource by querying the url
        field of the BCDC resources for records that contain the 
        file. Having determined the resource, the revision 
        object of the resource is then determined and queried.
        the revision timestamp is then extracted, converted to 
        a datetime object and returned.
        
        :param  resourceName: the name of the file that can be 
                              downloaded from BCDC. 
        :type resourceName: string
        
        :returns: revision date of the file extracted from BCDC
        :rtype: datetime
        '''
        bcdc = BCDCUtil.BCDCRestQuery.BCDCRestQuery('PROD')
        resourceNameJustFile = os.path.basename(resourceName)
        resourceDict = bcdc.getResource('url', resourceNameJustFile)
        if len(resourceDict['result']['results']) > 1:
            msg = 'When searching BCDC for a resource that contains the ' + \
                  'file {0} found {1} records.  Should only have found ' + \
                  'one.'
            msg = msg.format(resourceName, len(resourceDict['result']['results']))
            # TODO: Define appropriate error
            raise ValueError, msg
        elif len(resourceDict['result']['results']) == 0:
            msg = 'Did not find any resource records in BCDC that are ' + \
                  'associated with the file: {0} url: {1}'
            msg = msg.format(resourceNameJustFile, resourceName)
            raise ValueError, msg
        revision_id = resourceDict['result']['results'][0]['revision_id']
        revision = bcdc.getRevision(revision_id)
        revisionDate = revision['result']['timestamp']
        # example format: u'2015-07-24T19:57:35.259549'},
        # Convert to datetime and return the datetime object.
        revisionDateTime = datetime.datetime.strptime(revisionDate, '%Y-%m-%dT%H:%M:%S.%f')
        revisionDateTime = revisionDateTime.replace(tzinfo=pytz.utc)
        return revisionDateTime

    def formatDataSet(self, fmeDataSet):
        '''
        recieves a path that looks like: 
         https://catalogue.data.gov.bc.ca/dataset/5ee75843-06b3-4767-b6c2 ...
           -3248289f5e8d/resource/1dfd3f35-1ab3-4366-b2ae-9af51bf429b7/ ...
           download/nrsmajorprojectstracking.xls\nrsmajorprojectstracking.xls
        
        need to remove the repeat of the csv at the end of the path,not sure 
        why fme repeats it.
        
        '''
        retVal = fmeDataSet
        urlSplitOnBackslash = fmeDataSet.split('\\')
        if len(urlSplitOnBackslash) == 2:
            retVal = urlSplitOnBackslash[0]
        return retVal
    
#     def normalizeFeatureClassName(self, fcName):
#         self.logger.debug("fcName is going to remain unmodified: {0}".format(fcName.encode('string-escape')))
#         return fcName.encode('string-escape')

class ChangeCache():
    
    def __init__(self, changeObj, changeDict={}):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)

        self.changeObj = changeObj
        
        # saving some typing and creating pointers to objects that 
        # are in the changeObj.
        self.const = self.changeObj.const
        
        # where the change information that is retrieved from the change log file
        # gets stored
        self.changeDict = changeDict
        # dictionary that keeps track of whether change has been detected on a source
        self.changeCache = {}
        # dictionary keeping track of the features that passed through change detection
        # each source key is equal to a list.  
        #   Elem 0 = features that went through the change control
        #   Elem 1 = features where change was detected.  
        # This allows for a double check to be certain that 
        # the number of features that passed through is the 
        # same as the number that are associated with change
        # detection.
        self.featureCounts = {}
        # used to cache source data modification times 
        self.lastModifiedDateTimes = {}
        
    def repopulateChangeDict(self, newChangeDict):
        #self.changeCache = newChangeDict
        self.changeDict = newChangeDict
        self.logger.debug("Change cache data struct:")
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.changeDict)
        
    def truncDateTime(self, dt ):
        """
        truncate a datetime object to any time laps in seconds
        dt : datetime.datetime object, default now.
        roundTo : Closest number of seconds to round to, default 1 minute.
        Author: Thierry Husson 2012 - Use it as you want but don't blame me.
        """
        self.logger.debug("dt: {0} type: {1}".format(dt, type(dt)))
        dt = dt.replace( microsecond=0)
        return dt
        
    def hasChanged(self, currentDataSet):
        retVal = False
        #if currentDataSet[0:4].lower() <> 'http':
        #    currentDataSet = os.path.normcase(currentDataSet)
        #    currentDataSet = os.path.normpath(currentDataSet)
        currentDataSet = self.changeObj.formatDataSet(currentDataSet)
        #self.logger.debug("hasChanged path: {0}".format(currentDataSet))
        if self.changeCache.has_key(currentDataSet):
            # already encountered this source, returning the cached 
            # value determining if change has been detected.
            retVal = self.changeCache[currentDataSet]
            #self.logger.debug("already encountered this path: {0}".format(currentDataSet))
        else:
            if self.changeDict.has_key(currentDataSet):
                # is the "currentDataSet" described in the change log
                # if so compare the last modified time stamp with 
                # current time stamp
                fileModDate = self.changeObj.getFileModificationUTCDateTime(currentDataSet)
                # Rounding to nearest second as file change log
                # only tracks to the nearest second.
                fileModDate = self.truncDateTime(fileModDate)
                
                self.changeDict[currentDataSet] = self.truncDateTime(self.changeDict[currentDataSet])
                #print 'fileModDate', fileModDate
                #self.logger.debug("fileModDate: {0}".format(fileModDate))
                #print 'self.changeDict[currentDataSet]', self.changeDict[currentDataSet]
                #self.logger.debug("current data set: {0}".format(self.changeDict[currentDataSet]))
                if fileModDate > self.changeDict[currentDataSet]:
                    self.changeCache[currentDataSet] = True
                    self.lastModifiedDateTimes[currentDataSet] = fileModDate
                    retVal = True
                else:
                    self.changeCache[currentDataSet] = False
                    self.lastModifiedDateTimes[currentDataSet] =  self.changeDict[currentDataSet]
                    retVal = False
            else:
                #print 'no key for ', currentDataSet
                #self.logger.debug("no key for {0}".format(currentDataSet))
                self.changeCache[currentDataSet] = True
                fileModDate = self.changeObj.getFileModificationUTCDateTime(currentDataSet)
                fileModDate = self.truncDateTime(fileModDate)
                self.lastModifiedDateTimes[currentDataSet] =  fileModDate
                retVal = True
        return retVal
    
    def addFeatureIn(self, dataPath):
        #self.logger.debug('original datapath: {0}'.format(dataPath))
        dataPath = self.changeObj.formatDataSet(dataPath)
        #self.logger.debug('normalized datapath: {0}'.format(dataPath))
        if not self.featureCounts.has_key(dataPath):
            self.featureCounts[dataPath] = [1, 0]
        else:
            self.featureCounts[dataPath][0] += 1
    
    def addFeatureChange(self, dataPath):
        #self.changeObj.normalizeFeatureClassName(dataPath)
        dataPath = self.changeObj.formatDataSet(dataPath)
        #self.logger.debug("feature key: {0}".format(dataPath))
        if not self.featureCounts.has_key(dataPath):
            self.featureCounts[dataPath] = [0, 1]
        else:
            self.featureCounts[dataPath][1] += 1
        
    def updateFileChangeLog(self, fileChangeDetectionEnabled='TRUE'):
        # for each feature make sure that the features in 
        # is the same as the features changed
        # 06/25/2015 02:45:11,acdf_ownership_codes_staging_csv_bcgw,\\data.bcgov\data_staging\BCGW\physical_infrastructure\Diagnostic_Facilities_Data_Structure.csv,1428967194,True
        
        # fileChangeDetectionEnabled is the value that comes from the 
        # published parameter FILE_CHANGE_DETECTION.  This parameter
        # is used to disable the file change routine.
        # the lines below convert the string to a boolean
        if fileChangeDetectionEnabled.upper().strip() == 'FALSE':
            fileChangeDetectionEnabled = False
        else:
            fileChangeDetectionEnabled = True
            
        # getting a template constants object
        self.const.FMWMacroKey_FMWName
        self.logger.debug("change log path: {0}".format(self.changeObj.changeLogFilePath))
        fh = open(self.changeObj.changeLogFilePath, 'a')
        
        currentUTCDateTime = datetime.datetime.utcnow().replace(tzinfo=pytz.utc) 
        fmwName = self.changeObj.fmeMacroValues[self.const.FMWMacroKey_FMWName]
        
        currentLocalDateTime = currentUTCDateTime.astimezone(pytz.timezone(self.const.LocalTimeZone))
        currentLocalDateTimeStr = currentLocalDateTime.strftime(self.const.FMELogDateFormatString)
        currentUTCDateTimeStamp = int(time.mktime(currentUTCDateTime.timetuple()))
        # TODO: Should probably have used the CSV module to write to the log file
        cnt = 1
        for featPathOrig in self.featureCounts:
            # don't need to normalize featPath as it was normalized when entered
            # into data struct
            # features changed should be the same as the features that went in.
            self.logger.debug("iteration: {0}, rawfeat path {1}".format(cnt, featPathOrig))
            #featPath = self.changeObj.normalizeFeatureClassName(featPathOrig)
            featPath = self.changeObj.formatDataSet(featPathOrig)
            #featPath = featPath.encode('string-escape')
            self.logger.debug("fcName after normalize: {0}".format(featPath))

            elems = []
            if not fileChangeDetectionEnabled:
                # if fileChangeDetectionEnabled was set to false (disable file change)
                # then the feature was not checked for its modification date and 
                # therefor does not exist in the self.lastModifiedDateTimes dictionary.
                # the lines below get the file mod date for the current feature
                # and add it to that dictionary so the file modification date can 
                # be added to the change log.
                fileModDate = self.changeObj.getFileModificationUTCDateTime(featPath)
                fileModDate = self.truncDateTime(fileModDate)
                self.lastModifiedDateTimes[featPath] = fileModDate
                # indicating that a replication has taken place on the dataset in 'featPath'
                self.changeCache[featPath] = True        
            try:
                # convert last modified to utc time stamp
                self.logger.debug("feat path: {0}".format(featPath)) 
                modificationUTCTimeStamp = int(time.mktime(self.lastModifiedDateTimes[featPath].timetuple()))
                # convert last modified to datetime in local time zone
                modificationLocalDateTime = self.lastModifiedDateTimes[featPath].astimezone(pytz.timezone(self.const.LocalTimeZone))
                # convert local datetime object to string representation
                modificationLocalDateTimeStr = modificationLocalDateTime.strftime(self.const.FMELogDateFormatString)
            except KeyError, e:
                lastModKeys = self.lastModifiedDateTimes.keys()
                msg = "The key: {0} was not found in the lastModifiedDateTimes " + \
                      "dictionary, valid keys include {1}"
                msg = msg.format(featPath, ','.join(lastModKeys))
                self.logger.error(msg)
                raise
                
            destinationKeyWord = self.changeObj.fmeMacroValues[self.const.FMWParams_DestKey]
            
            elems.append(currentLocalDateTimeStr)
            elems.append(str(currentUTCDateTimeStamp))
            elems.append(fmwName)
            elems.append(featPath)
            elems.append(str(modificationUTCTimeStamp))
            elems.append(modificationLocalDateTimeStr)
            elems.append(destinationKeyWord)

            if self.featureCounts[featPath][0] == self.featureCounts[featPath][1]:
                elems.append(str(self.changeCache[featPath]))
            else:
                msg = 'input features is nto the same as the change ' +\
                      'features.  input: {0}, change {1}'
                msg = msg.format(self.featureCounts[featPath][0], self.featureCounts[featPath][1])
                self.logger.warning(msg)
                elems.append(str(False))
            # TODO: might want write this to memory then if the fmw completes successfully it gets written
            LineToAdd = ','.join(elems) + '\n'
            fh.write(LineToAdd)
            cnt += 1
        fh.close()
            
            