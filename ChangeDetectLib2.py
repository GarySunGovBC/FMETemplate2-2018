'''
Created on Jan 9, 2017

@author: kjnether


About
=========
:synopsis:     A library containing functionality required to implement
               file change detection for source FMW files
:moduleauthor: Kevin Netherton
:date:         1-9-2017
:description:  Originally there was a change detection transformer available
               in the original fme template.  Then that code was ported to 
               ChangeDetectLib (version 1).  That code worked but was
               cumbersome to maintain, and hard to understand.  This version
               is a complete re-write / restructure of how the library works.
               
               The external iternface to the changedetect class should remain
               relatively consistent.  The code internal to this module will
               change significantly
               
   
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
'''

import DataBCFMWTemplate
import BCDCUtil.BCDCRestQuery  # @UnresolvedImport
import os.path
import datetime
import time
import logging
import pytz
import requests
import pprint

class Constants(object):
    
    # constants used by this library
    logFileParam_LastCheckedDateStr = 0
    logFileParam_LastCheckedUTCTimeStamp = 1
    logFileParam_FMW = 2
    logFileParam_SrcData = 3
    logFileParam_LastModifiedUTCTimeStamp = 4
    logFileParam_LastModifiedDateStr = 5
    logFileParam_LastModifiedDestDBEnvKey = 6
    logFileParam_WasModified = 7
    # number of parameters expected in a log file line
    # when split on the ,
    expectedLogLineParams = 8
    
    dateFormatStr = '%Y-%m-%d %H:%M:%S'

    LocalTimeZone = 'US/Pacific'

class ChangeDetect(object):
    '''
    This will be the public interface to this class. 
    
    Want to keep this and all classes in this library away from specific
    FME requirements.  Example FMEMacrovalues.  Specific values required
    from the macro value should be provided as individual parameters that
    are dealt with by the function that is using this module 
    '''
    def __init__(self, changeLogPath):
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.const = Constants()
                
        if not isinstance(changeLogPath, ChangeLogFilePath):
            msg = 'Called the ChangeDetect contructor with a {0} type object in the ' + \
                  'parameter changeLogPath.  This parameter must be a ChangeLogFilePath ' + \
                  'type object'
            raise ValueError, msg.format(type(changeLogPath))
        
        # This is a change log object (ChangeLogFilePath)
        self.chngLogFilePath = changeLogPath
        self.chngLog = ChangeLogFile(self.chngLogFilePath, self.const)
        #self.chngLogFile = ChangeLogFile(self.chngLogFilePath)
        self.chngLog.readChangeDetectLogFile()
        
        # objects to cache source data 
        self.sourceDataCollection = SourceDataCollection(self.const)
        self.changeLogCollection = self.chngLog.getChangeEventCollection()
        
    def hasChanged(self, srcDataPath, destDbEnvKey=None):
        retVal = False
        self.logger.debug("called has changed")
        # standardize the path:
        srcDataPathNormalized = Util.formatDataSet(srcDataPath)
        if self.sourceDataCollection.exists(srcDataPathNormalized, destDbEnvKey):
            retVal = self.sourceDataCollection.getChangeParam(srcDataPathNormalized, destDbEnvKey)
            self.logger.debug("using the cached version of source data, {0} {1}".format(srcDataPathNormalized, retVal))
        else:
            # havent' seen this source data set yet.
            
            # create a new sourcedata object,
            # use the sourcedata object to get the modification timestamp
            # check with the change log to see if the source data has modiifed
            # update the sourcedata object with the results of the modification
            #   test
            # add the sourdate object to the sourcedatacollections
            
            # before we do anything see if the srcData has an entry in the log
            # if it does not then its obviously 
            chgLogEvent = self.changeLogCollection.getMostRecentEvent(srcDataPathNormalized, destDbEnvKey)
            if chgLogEvent:
                # modification date of the file in srcDataUTCTimeStamp
                # has it changed since the last time the script was run?
                srcData = self.getSourceDataObj(srcDataPathNormalized, destDbEnvKey)
                srcDataUTCTimeStamp = srcData.getUTCTimeStamp()
                logEventUTCTimeStamp = chgLogEvent.getLastTimeRanAsUTCTimeStamp()
                # add the source data to the source data collection
                print 'srcDataUTCTimeStamp', srcDataUTCTimeStamp
                print 'logEventUTCTimeStamp', logEventUTCTimeStamp
                if srcDataUTCTimeStamp > logEventUTCTimeStamp:
                    # source data has been modified since last replication
                    retVal = True
                srcData.setChangeFlag(retVal)
                self.sourceDataCollection.addSourceDataSet(srcData)
            else:
                # if there is no change event in the changelog, then assume
                # that we should replicate data
                self.logger.debug("no change event for source {0}".format(srcDataPathNormalized))
                retVal = True
        self.logger.debug("change detected on {1} value {0}".format(retVal, srcDataPathNormalized))
        return retVal
    
    def getSourceDataObj(self, srcDataPathNormalized, destDbEnvKey):
        return SourceFileData(srcDataPathNormalized, destDbEnvKey, self.const)
                    
    def addFeatureChange(self, srcDataPath, destDbEnvKey):
        srcDataPathNormalized = Util.formatDataSet(srcDataPath)
        self.sourceDataCollection.incrementFeatureCount(srcDataPathNormalized, destDbEnvKey)
    
    def updateFileChangeLog(self, fmwName):
        # write records to the change log for each source feature type 
        # encountered in the replication
        # fmwName, changeDetectionEnabledParam, self.destDbEnvKey
        self.logger.debug("updating the change log")
        for srcData in self.sourceDataCollection.sourceDataList:
            self.logger.debug('srcData: {0}'.format(srcData))
            changeDate = srcData.getUTCTimeStamp()
            wasChanged = srcData.getWasChanged()
            newRecord = srcData.getChangeLogRecord(fmwName, wasChanged, changeDate)
            self.logger.debug("record 2 write {0}".format(newRecord))
            self.chngLog.writeLogRecord(newRecord)

    def setDateStrFormat(self, dateFormatStr):
        '''
        Sets the dateformat, this is the strftime string used 
        to convert datetime objects to strings and vise versa.
        
        By default it uses the one set in this libraries constants.
        This method allows you to override that so that it matches
        the format that might be defined by a calling application.
        
        This format is used when writing datetimes to the log.  Any 
        time the log writes a record it will write both the utctimestamp
        integer as well as the localtime date string (as defined by 
        the dateformat str)  
        
        '''
        self.const.dateFormatStr = dateFormatStr
            
class ChangeLogFilePath(object):
    '''
    Dealing with the change log file path seems to be complex 
    enought to justify its own path.  This object is where
    properties pertaining to the change log file path are
    stored as well as methods for verify the path, createing 
    directories etc..
    '''
    def __init__(self, changeLogRootDir, changeLogFileName, fmwFilePath, changeLogDirParameterName):
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.fullPathToChangeLog = None # gets populated by method calculateAndVerifyChangeLogFilePath
        self.changeLogRootDir = changeLogRootDir
        self.changeLogFileName = changeLogFileName
        self.fmwFilePath = fmwFilePath
        self.changeLogDirParameterName = changeLogDirParameterName
        self.calculateAndVerifyChangeLogFilePath()
        
    def getChangeLogFullPath(self):
        return self.fullPathToChangeLog
    
    def calculateAndVerifyChangeLogFilePath(self):
        # Check for the root directory for the path to the change logs
        if not os.path.exists(self.changeLogRootDir):
            msg = 'The root directory: {0} for file change logs does not exist' + \
                  'Either create it or change the parameter in the global section ' + \
                  'called {1} in the config file {2}'
            msg = msg.format(self.changeLogRootDir, self.changeLogDirParameterName, 
                             self.paramObj.confFile)
            self.logger.error(msg)
            raise ValueError, msg
        
        # Create the directory for this specific fmw
        fmwFileNameNoSuffix, suffix = os.path.splitext(self.fmwFilePath)
        changeLogFMWDir = os.path.join(self.changeLogRootDir, fmwFileNameNoSuffix)
        if not os.path.exists(changeLogFMWDir):
            msg = 'The FMW directory for change logs {0} does not exist.' + \
                  'Creating the directory now'
            msg = msg.format(changeLogFMWDir)
            self.logger.info(msg)
            os.mkdir(changeLogFMWDir)
        
        # Create the log file for this specific fmw
        self.fullPathToChangeLog = os.path.join(changeLogFMWDir, self.changeLogFileName)
        if not os.path.exists(self.fullPathToChangeLog):
            msg = 'There is no file change log currently created for the ' + \
                  'fmw: {0}, creating one now!  The full path to the file that ' + \
                  'is being created is {1}'
            msg = msg.format(self.fmwFilePath, self.fullPathToChangeLog)
            self.logger.info(msg)
            fh = open(self.fullPathToChangeLog, 'w')
            fh.close()
        self.logger.debug("change log file is: {0}".format(self.fullPathToChangeLog))

class ChangeLogFile(object):
    '''
    The interface to the change log file.  This method 
    can read the change log, returns a ChangeEventCollection
    
    '''
    def __init__(self, chngLogFilePath, const=None):
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.const = const
        if not self.const:
            self.const = Constants()
        
        self.chngLogFilePath = chngLogFilePath
        
        # populated by the readChangeDetectLogFile() method
        self.changeEventCollection = None

    def readChangeDetectLogFile(self):
        '''
        reads the change log into memory.
        '''
        self.changeEventCollection = ChangeEventCollection()
        logFile = self.chngLogFilePath.getChangeLogFullPath()
        if not os.path.exists(logFile):
            msg = 'Trying to read the change detection log file {0} ' +\
                  'however this path does not exist'
            self.logger.error(logFile)
            raise ValueError, msg.format(logFile)
        with open(logFile, 'r') as logFileFH:
            for line in logFileFH:
                line = line.strip()
                if line:
                    logFileEvent = ChangeEvent(line, self.const)
                    self.changeEventCollection.addChangeEvent(logFileEvent)
               
    def getChangeEventCollection(self):
        if not self.changeEventCollection:
            self.readChangeDetectLogFile()
        return self.changeEventCollection
    
    def writeLogRecord(self, logRecord):
        
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(logRecord)
        logFile = self.chngLogFilePath.getChangeLogFullPath()
        self.logger.debug("change log file is {0}".format(logFile))
        fh = open(logFile, 'a')
        logRecordStr = '{0}\n'.format(','.join(logRecord))
        fh.write(logRecordStr)
        fh.close()
    
class ChangeEventCollection(object):
    '''
    When the change log is read it creates one of these objects
    which will contain all the information contained in the 
    change log, with an api for analyzing it.
    '''
    def __init__(self):
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.eventList = []
        
    def addChangeEvent(self, event):
        self.eventList.append(event)
        
    def getMostRecentEvent(self, path, destDbEnv):
        '''
        returns the most recent event for the given path destdbenv
        combination.
        
        There may be records with this combination and a was modified
        parameter as false.  These records are ignored as was modified
        indicates whether a replication actually took place.
        '''
        retVal = None
        for event in self.eventList:
            if event.srcData == path and \
               destDbEnv.upper() == event.destDBEnvKeyValue.upper():
                if event.getWasModified():
                    if not retVal:
                        retVal = event
                    elif retVal.lastChecked < event.lastChecked:
                        retVal = event
        return retVal

class ChangeEvent(object):
    '''
    each line in the change log can be represented by one of 
    these objects
    '''
    def __init__(self, logLineString, const=None):
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.const = const
        if not self.const:
            self.const = Constants()

        self.logLineString = logLineString
        self.parseLogLine()
        
    def parseLogLine(self):
        '''
        takes the log line, which exists in the object property
        'changeLogLine' and parses it into the various properties
        required by this object
        '''
        # get rid of leading or trailing whitespace characters and
        # then split into a list.
        lineList = self.logLineString.strip().split(',')
        if len(lineList) <> self.const.expectedLogLineParams:
            msg = 'Expecting a log file line with {2} elements in it.  ' + \
                  'The line: ({0}) only has {1} elements in it'
            self.logger.error(msg.format(self.logLineString, len(lineList), self.const.expectedLogLineParams))
            raise ValueError, msg.format(self.logLineString, len(lineList), self.const.expectedLogLineParams)
        
        self.lastCheckedDateStr = lineList[self.const.logFileParam_LastCheckedDateStr]
        self.lastChecked = int(lineList[self.const.logFileParam_LastCheckedUTCTimeStamp])
        self.FMW = lineList[self.const.logFileParam_FMW]
        self.srcData = lineList[self.const.logFileParam_SrcData]
        self.srcData = Util.formatDataSet(self.srcData)
        self.lastModifiedUTC = int(lineList[self.const.logFileParam_LastModifiedUTCTimeStamp])
        self.lastModifiedDateStr = lineList[self.const.logFileParam_LastModifiedDateStr]
        self.destDBEnvKeyValue = lineList[self.const.logFileParam_LastModifiedDestDBEnvKey]
        self.wasModified = lineList[self.const.logFileParam_WasModified]
        if self.wasModified.lower() == 'false':
            self.wasModified = False
        else:
            self.wasModified = True
       
    def getWasModified(self):
        return  self.wasModified
            
    def getModificationAsUTCTimeStamp(self):
        self.logger.debug("utc time stamp raw {0}".format(self.lastModifiedUTC))
        timeStamp = datetime.datetime.utcfromtimestamp(self.lastModifiedUTC)
        timeStamp = pytz.utc.localize(timeStamp)
        timeStamp = timeStamp.replace( microsecond=0)
        return timeStamp
    
    def getLastTimeRanAsUTCTimeStamp(self):
        self.logger.debug("utc time stamp raw {0}".format(self.lastChecked))
        timeStamp = datetime.datetime.utcfromtimestamp(self.lastChecked)
        timeStamp = pytz.utc.localize(timeStamp)
        timeStamp = timeStamp.replace( microsecond=0)
        return timeStamp

class SourceDataCollection(object):
    '''
    The change transformer will funnel each and every feature that 
    is described in the FMW through the change detect process.
    When this happens there can be more than one source data set.
    This object keeps track of the collection of source data.
    '''
    def __init__(self, const):
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.const = const
        if not self.const:
            self.const = Constants()
        self.sourceDataList = []

    def addSourceDataSet(self, srcFileObject):
        # verify type
        if not isinstance(srcFileObject, SourceFileData):
            msg = 'trying to add a object of type {0} to a' +\
                  'SourceDataCollection object.  You can only ' +\
                  'add objects of type SourceFileData'
            raise ValueError, msg.format(type(srcFileObject))
        self.sourceDataList.append(srcFileObject)

    def setChangeFlag(self, srcFileObject,destDbEnv, changeFlag):
        srcDataObj = self.getSrcDataObj(srcFileObject, destDbEnv)
        srcDataObj.setChangeFlag(changeFlag)

    def exists(self, srcDataPath, destDbEnv):
        retVal = False
        srcData = self.getSrcDataObj(srcDataPath, destDbEnv)
        if srcData:
            retVal = True
        return retVal
    
    def getSrcDataObj(self, srcDataPath, destDbEnv):
        retVal = None
        for srcData in self.sourceDataList:
            if srcData.hasPath(srcDataPath, destDbEnv):
                retVal = srcData
                break
        return retVal
    
    def getChangeParam(self, srcDataPath, destDbEnv):
        # returns the boolean value used to indicate whether 
        # this particular data set has "srcDataPath" has been 
        # processed already, and if so whether it has been 
        # deemed to have been modified
        retVal = True
        srcData = self.getSrcDataObj(srcDataPath, destDbEnv)
        if srcData:
            retVal = srcData.changeParam
            self.logger.debug("found source data object for {0}, change param {1}".format(srcDataPath, retVal))
        return retVal
    
    def incrementFeatureCount(self, srcDataPath, destEnvDbKey):
        '''
        If the data does not exist in the source data collection then it 
        will be added and the feature count will be incremented.  
        
        If the data does exist then the source data collection is only 
        incremented
        '''
        srcFile = self.getSrcDataObj(srcDataPath, destEnvDbKey)
        if srcFile:
            self.logger.debug("found existing object for path {0}".format(srcDataPath))
            srcFile.incrementFeatureCount()
        else:
            self.logger.debug("creatting a new object for path {0}".format(srcDataPath))
            srcFile = SourceFileData(srcDataPath, destEnvDbKey, self.const)
            srcFile.incrementFeatureCount()
            self.addSourceDataSet(srcFile)        
        
class SourceFileData(object):
    '''
    Each source feature class in the fmw, will have one  of these objects
    created for it.  Will include methods to do things like get the modification
    date for this object, as well as keeping track of the number of features
    that are in this object etc.
    '''
    def __init__(self, srcPath, destDbEnv, const=None):
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.const  = const
        if not self.const:
            self.const = Constants()
        
        self.srcPath = srcPath
        self.destDbEnv = destDbEnv
        # default change param, used to identify if this data has been tagged
        # as having been modified.
        self.changeParam = True
        self.modificationUTCTimeStamp = None
        self.featureCount = 0
        
        # current time as utc
        #currentUTCTime = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        #currentUTCTimeStamp = int(time.mktime(currentUTCTime.timetuple()))
        #currentLocalDateTime = currentUTCTime.astimezone(pytz.timezone(self.const.LocalTimeZone))
        
        # TODO: not finished here!
        #fmwName = self.changeObj.fmeMacroValues[self.const.FMWMacroKey_FMWName]
        
        # Calculating the datetime for now.  This is the datetime for the when 
        # the data was checked.
        self.currentUTCDateTime = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        #currentLocalDateTime = self.currentUTCDateTime.astimezone(pytz.timezone(self.const.LocalTimeZone))
        #self.currentLocalDateTimeStr = currentLocalDateTime.strftime(self.const.FMELogDateFormatString)
        #self.currentUTCDateTimeStamp = int(time.mktime(currentUTCDateTime.timetuple()))
        
    def hasPath(self, testPath, destDbEnv):
        # used to identify if the supplied path is equal to the path
        # used for this boject
        retVal = False
        if testPath == self.srcPath:
            if self.destDbEnv.upper() == destDbEnv.upper():
                retVal = True
        return retVal
    
    def getUTCTimeStamp(self):
        '''
        returns the last modification date for the dataset being 
        described by this object
        '''
        if not self.modificationUTCTimeStamp:
            # convert to date
            #TODO: Should I ever be worried about the timestamp returning 
            #      as something other than pacific standard time.
            # getmtime should return UTC (aka gmt) time.
            # http://stackoverflow.com/questions/11405049/how-to-get-the-file-modification-date-in-utc-from-python
            modTimeStamp = os.path.getmtime(self.srcPath)
            #mtime = os.stat(inFilePath)[stat.ST_MTIME]
            # fromtimestamp - returns local time
            # utcfromtimestamp - returns datetime in utc time zone
            #timeStamp = datetime.datetime.utcfromtimestamp(modTimeStamp)
            timeStamp = datetime.datetime.utcfromtimestamp(modTimeStamp)
            timeStamp = pytz.utc.localize(timeStamp)
            timeStamp = timeStamp.replace( microsecond=0)
            self.modificationUTCTimeStamp = timeStamp
        return self.modificationUTCTimeStamp
    
    def incrementFeatureCount(self):
        self.featureCount += 1
        
    def getChangeLogRecord(self, fmw, changeDetectionEnabledParam, changeDate):
        # create a list with the correct number of elements
        outList = [None] * self.const.expectedLogLineParams
        # now start to populate the elements
        # - FMW Name
        outList[self.const.logFileParam_FMW] = os.path.basename(fmw)
        # - last modified Time stamp as an int
        outList[self.const.logFileParam_LastModifiedUTCTimeStamp] = str(Util.datetime2intTimeStamp(changeDate))
        # - last modified as a local date time string
        outList[self.const.logFileParam_LastModifiedDateStr] = Util.datetime2localDateTimeString(changeDate, self.const.dateFormatStr, self.const.LocalTimeZone)
        # - last time file was checked
        outList[self.const.logFileParam_LastCheckedUTCTimeStamp] = str(Util.datetime2intTimeStamp(self.currentUTCDateTime))
        # - last time file was checked as local date time string
        outList[self.const.logFileParam_LastCheckedDateStr] = Util.datetime2localDateTimeString(self.currentUTCDateTime, self.const.dateFormatStr, self.const.LocalTimeZone)
        # - source file - swapping back to windows paths
        outList[self.const.logFileParam_SrcData] = os.path.normcase(os.path.normpath(self.srcPath))
        # - last modified destination database env key
        outList[self.const.logFileParam_LastModifiedDestDBEnvKey] = self.destDbEnv
        # - was the change detection enabled, (ie was the change actually performed)
        outList[self.const.logFileParam_WasModified] = str(changeDetectionEnabledParam)
        return outList
    
    def getWasChanged(self):
        #retVal = False
        #if self.featureCount:
        #    retVal = True
        return self.changeParam
    
    def setChangeFlag(self, changeFlag):
        self.logger.debug("setting the change flag to {0}".format(changeFlag))
        self.changeParam = changeFlag
        
class Util:
    
    @staticmethod
    def formatDataSet(fmeDataSet):
        '''
        This is the parameter that is extracted from an fme objects
        feature object, using the feature.getattribute('fme_dataset')
        
        needs formatting so that it can be referred to consistently
        
        Was having problems when the paths were provided using 
        \\ charcters.  When trying to retrieve a value in a 
        dictionary and the dictioanry key had \\ in it.  Sometimes
        lookups failed because the \\ were being interpreted as\\\\
        
        All analysis of windows file paths is now being completed 
        using / slashes.  os.path functions all work consistently 
        with the forward slash syntax, example exists, isdir, ...
        '''
        # only normalize if the the dataset does not start with 
        # http:// or https://
        dataPath = fmeDataSet
        if dataPath[0:7].lower() <> r'http://' and \
         dataPath[0:8].lower() <> r'https://':
            #print 'normalizing the path', dataPath[0:7], dataPath[0:8], dataPath
            #self.logger.debug("orignal path: {0}".format(fmeDataSet))
            dataPath = os.path.normcase(dataPath)
            #self.logger.debug("case normalized path: {0}".format(fmeDataSet))
            dataPath = os.path.normpath(dataPath)
            #self.logger.debug("normalized path: {0}".format(fmeDataSet))
            # internally was having problems comparing paths when they used the 
            # windows delimiter.  To resolve this paths internal to the change
            # control will always be translated to use unix carriage returns
            # when record is written back to the log file the path delimiter 
            # get switched back
            dataPath = dataPath.replace('\\', '/')
            #self.logger.debug("path with forward slashes: {0}".format(dataPath))
        return dataPath

    @staticmethod
    def datetime2intTimeStamp( inDateTime):
        if not isinstance(inDateTime, datetime.datetime):
            msg = 'You sent an object of type {0} when expecting a datetime object'
            raise ValueError, inDateTime.format(type(inDateTime))
        
        inDateTimeNoTZ = inDateTime.replace(tzinfo=None)
        unixTime = (inDateTimeNoTZ - datetime.datetime(1970,1,1)).total_seconds()
        unixTimeInt = int(round(unixTime))
        #retVal = int(time.mktime(inDateTime.timetuple()))
        return unixTimeInt
    
    @staticmethod
    def datetime2localDateTimeString( inDateTime, datetimeStringFormat, localDateTime):
        if not isinstance(inDateTime, datetime.datetime):
            msg = 'You sent an object of type {0} when expecting a datetime object'
            raise ValueError, inDateTime.format(type(inDateTime))
        
        dateTimeAsLocal = inDateTime.astimezone(pytz.timezone(localDateTime))
        dateTimeAsLocalStr = dateTimeAsLocal.strftime(datetimeStringFormat)
        return dateTimeAsLocalStr

class ChangeDetectBCDC(ChangeDetect):
    
    def __init__(self, changeLogPath):
        ChangeDetect.__init__(self, changeLogPath)
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("creating ChangeDetectBCDC object")
        
        self.sourceDataCollection = SourceDataCollectionBCDC(self.const)
        
    def getSourceDataObj(self, srcDataPath, destDbEnvKey):
        return SourceBCDC(srcDataPath, destDbEnvKey, self.const)

    def addFeatureChange(self, srcDataPath, destDbEnvKey):
        self.logger.debug("adding feature change for {0}".format(srcDataPath))
        self.sourceDataCollection.incrementFeatureCount(srcDataPath, destDbEnvKey)

class SourceDataCollectionBCDC(SourceDataCollection):
    
    def __init__(self, const):
        SourceDataCollection.__init__(self, const)
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("created a SourceDataCollectionBCDC object")
        
    def addSourceDataSet(self, srcFileObject):
        # verify type
        if not isinstance(srcFileObject, SourceBCDC):
            msg = 'trying to add a object of type {0} to a ' +\
                  'SourceBCDC object.  You can only ' +\
                  'add objects of type SourceFileData'
            raise ValueError, msg.format(type(srcFileObject))
        self.logger.debug("added {0}".format(srcFileObject))
        self.sourceDataList.append(srcFileObject)
        
    def incrementFeatureCount(self, srcDataPath, destEnvDbKey):
        '''
        If the data does not exist in the source data collection then it 
        will be added and the feature count will be incremented.  
        
        If the data does exist then the source data collection is only 
        incremented
        '''
        
        srcFile = self.getSrcDataObj(srcDataPath, destEnvDbKey)
        self.logger.debug("incremented feature count ")
        if srcFile:
            self.logger.debug("found existing object for path {0}".format(srcDataPath))
            srcFile.incrementFeatureCount()
        else:
            self.logger.debug("creatting a new object for path {0}".format(srcDataPath))
            srcFile = SourceBCDC(srcDataPath, destEnvDbKey, self.const)
            srcFile.incrementFeatureCount()
            self.addSourceDataSet(srcFile)    

        
class SourceBCDC(SourceFileData):
    
    def __init__(self, srcPath, destDbEnv, const=None):
        SourceFileData.__init__(self, srcPath, destDbEnv, const)
        
        #modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        self.const  = const
        if not self.const:
            self.const = Constants()
            
    def getUTCTimeStamp(self ):
        if not self.modificationUTCTimeStamp:
            revisionUTCTimeStamp = None
            resp = requests.head(self.srcPath)
            # test to make sure the data is available
            if resp.status_code <> 200:
                msg = 'Unable to connect to the source dataset {0}'
                raise ValueError, msg.format(self.srcPath)
            else:
                self.modificationUTCTimeStamp = self.getBCDCResourceModificationDate(self.srcPath)
                
        return self.modificationUTCTimeStamp
    
    def getBCDCResourceModificationDate(self, resourceName):
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

    def getChangeLogRecord(self, fmw, changeDetectionEnabledParam, changeDate):
        logRecord = super(SourceBCDC, self).getChangeLogRecord(fmw, changeDetectionEnabledParam, changeDate)
        # now replace the src data with an unformatted version
        logRecord[self.const.logFileParam_SrcData] = self.srcPath
        return logRecord
        
