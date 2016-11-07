'''
Created on Sep 1, 2016

@author: kjnether
'''
import sys
import requests
import shutil
import os.path
import json
import urlparse
import datetime
import hashlib
import logging.config
import time
import pytz
import re
import platform
import DataBCFMWTemplate
import zipfile

class Constants(object):
    # various directory names used by the rest api.
    statusDir = 'status'
    orderDir = 'orders'
    extractDir = 'extract'
    extractFile = 'ParcelMapDirect_{0}.zip'
    restPath = '/pmd/api'
    # the is a default value, can be overridden in the constructor for a parcelmap api object
    orderStatusFileName = 'ParcelMapOrderStatus.json'
    orderStatusFileNameFullPath = None
    
    defaultFormat = 'File Geodatabase'
    defaultSRID = 'EPSG:3153'
    
    configDir = 'config'
    logConfigFile = 'logging_devel.config'
    
    logDir = 'logs'
    logFileName = (os.path.splitext(os.path.basename(__file__)))[0] + '.log'
    
    # keys for rest api
    restKey_jurisdiction = 'jurisdictionCode'
    restKey_orderid = 'orderID'
    restKey_SRID = 'SRID'
    restKey_expectedDate = 'estimatedCompletionTime'
    restKey_status = 'status'
    restKey_extract = 'extract'
    
    # the format we are expecting the datetime string to come in
    # example: "2016-09-03T00:43:57Z"
    expectedDateFormat = '%Y-%m-%dT%H:%M:%SZ'
    
    # This is the time in seconds that the script will wait
    # before it polls the server about the status of an 
    # order.  was 120, moving it to 
    pollInterval = 900
    # a request will return an expected completion time. The api 
    # will calculate how far in the future that value is in seconds
    # then propotion that number by this amount.  After that amount
    # of time has passed the api will start to poll every $pollInterval
    # seconds.
    # was 75 but have changed to 130 as the parcelmap ui 
    # seems to underestimate completion times
    pollStartTimeProportionPercentage = 200
    
    # Order statuses sometimes return an error
    # that looks like this:
    # ConnectionError: HTTPSConnectionPool(host='appstest1.ltsa.ca', port=443): Max retries exceeded with url: /pmd/api/orders/10630/status (Caused by NewConnectionError('<requests.packages.urllib3.connection.VerifiedHTTPSConnection object at 0x02D4BAD0>: Failed to establish a new connection: [Errno 11004] getaddrinfo failed',))
    # 
    # When querying for order status and this error is encountered
    # the script will pause for "ConnectionErrorRetryInterval" amount
    # of time in seconds
    # it will retry the query "ConnectionErrorMaxRetries 
    ConnectionErrorRetryInterval = 30
    ConnectionErrorMaxRetries = 5
    
    # Tries to download an order, if the order returns a 0 size file
    # then will try this many times before it fails
    maxParcelMapDownloadAttempts = 3
    
    # the suffix used for the checksum file (md5)
    fingerprintSuffix = '.md5'
    
    # when the status of an order returns and the order is processing 
    # it will return a string that looks like:
    #  - Processing
    #  - Processing (1/3)
    #  - Processing (10/12)
    # 
    # this regular expression will capture those messages
    isProcessingRegex = re.compile('processing\s*(\(\d+///d+\)){0,1}', re.IGNORECASE)
    
    testRequest1 = {
            'extract' : {
                 'jurisdictionCode' : 'J0084',
                'SRID' : defaultSRID,
                'format' : defaultFormat,
                'parcelFabricExtract' : 'Yes', 
                'realWorldChanges': 'No', 
                'fabricSpatialImprovements': 'No'
                }
            }
    
    defaultJurisDictionRequest = {
            'extract' : {
                 restKey_jurisdiction : '{0}',
                restKey_SRID : defaultSRID,
                'format' : defaultFormat,
                'parcelFabricExtract' : 'Yes', 
                'realWorldChanges': 'No', 
                'fabricSpatialImprovements': 'No'
                }
            }
    
    albersBB = [ [ [ 0, 0 ],
                   [ 3000000, 0],
                   [ 3000000,3000000 ],
                   [ 0, 3000000],
                   [ 0, 0 ] ] ]
    
    #geographicBB = [ [ [ -139.06, 48.30], 
    #                   [ ]]]
    
#     provincialJsonRequest = {
#               "extract": {
#                 "format": "File Geodatabase",
#                 "realWorldChanges": "No",
#                 "parcelFabricExtract": "Yes",
#                 restKey_jurisdiction : None,
#                 "fabricSpatialImprovements": "No",
#                 "geometry": {
#                   "type": "Polygon",
#                   "coordinates": albersBB
#                 },
#                 "SRID": "EPSG:3153"
#               }
#             }
    provincialJsonRequest = {
              "extract": {
                "format": defaultFormat,
                "realWorldChanges": "No",
                "parcelFabricExtract": "Yes",
                "jurisdictionCode": "ALL",
                "fabricSpatialImprovements": "No",
                "SRID": defaultSRID
              }
            }

    def __init__(self):
        pass
    
class RestBase():
    def __init__(self, url, userName, passWord):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("first log message")
        self.baseurl = url
        self.userName = userName
        self.passWord = passWord
        # TODO: Verifi that this works and if it does should not need to maintain the class properties of userName and passWord
        self.authObj = requests.auth.HTTPBasicAuth(userName, passWord)
        
    def postRequest(self, url, data):
        dataAsJsonStr = json.dumps(data, indent=2)
        self.logger.debug( 'json data as string: {0}'.format(dataAsJsonStr))
        headers = {'Content-Type': 'application/json'}
        r = requests.post(url, data=dataAsJsonStr, headers=headers, auth=self.authObj)
        # verify the status code
        if r.status_code < 200 or r.status_code > 299:
            msg = 'Post Request failed, status code is {0}, return text is {1}'
            #self.logger.debug()
            raise ValueError, msg.format(r.status_code, r.text)
        self.logger.info("order request returned status code {0}".format(r.status_code))
        return r.json()

    def getRequest(self, url, data=None):
        jsonRep = None
        if data:
            # TODO: add logic to include the data if its passed
            #       to this method, don't need this functionality yet
            pass
        r = requests.get(url, data=data, auth=self.authObj)
        if r.status_code in [429, 503]:
            statusCodeMesg = "When requesting the the data received http status " + \
                             "code {0}.  Pausing for {1} seconds and then will " + \
                             "re-issue the request"
            self.logger.error(statusCodeMesg.format(r.status_code, self.ConnectionErrorRetryInterval))
            time.sleep(self.ConnectionErrorRetryInterval)
            r = requests.get(url, data=data, auth=self.authObj)
        if r.status_code < 200 or r.status_code > 299:
            # problem with the request
            msg = "GET request returned a status code of {0}" + \
                  "expecting a 200 series code for success"
            raise requests.ConnectionError, msg.format(r.status_code)
        else:
            self.logger.debug("raw response for get request is: {0}".format( r))
            jsonRep = r.json()
            self.logger.debug("json response is {0}".format(jsonRep))
            self.logger.debug("requested url is: {0}".format( url))
        return jsonRep

    def getBinaryRequest(self, url, destFile, maxDownloadAttempts, data=None, attempts=0 ):
        self.logger.debug("requested destination file is: {0}".format( destFile ))
        r = requests.get(url, data=data, auth=self.authObj, stream=True)
        self.logger.info("response code from requst for order file: {0}".format(r.status_code ))
        if attempts > maxDownloadAttempts:
            msg = "Attempted to retrieve the data {0} times, all have failed!"
            msg = msg.format(attempts)
            raise IOError, msg
        if r.status_code == 429:
            statusCodeMesg = "When requesting the the data recieved http status " + \
                             "code {0}.  Pausing for {1} seconds and then will " + \
                             "re-issue the request"
            self.logger.error(statusCodeMesg.format(r.status_code, self.ConnectionErrorRetryInterval))
            time.sleep(self.ConnectionErrorRetryInterval)
            attempts += 1
            self.getBinaryRequest(url, destFile, maxDownloadAttempts, data=None, attempts=attempts )
            #r = requests.get(url, data=data, auth=self.authObj, stream=True,         
        if r.status_code == 200:
            self.logger.debug("attempting to write raw data to {0}".format(destFile))
            with open(destFile, 'wb') as f:
                #print r.content
                #r.raw.decode_content = True
                #print r.raw
                #shutil.copyfileobj(r.raw, f)
                for chunk in r:
                    f.write(chunk)
            fileSize = os.path.getsize(destFile)
            if int(fileSize) == 0:
                attempts += 1
                msg = "Order zip file: {0} was successfully downloaded but has a file size " + \
                      "of {1}.  Deleting this file and trying again, attempts {2}/{3}"
                msg = msg.format(destFile, fileSize, attempts, maxDownloadAttempts)
                self.logger.debug(msg)
                self.logger.debug("sleeping for {0}".format(self.ConnectionErrorRetryInterval))
                time.sleep(self.ConnectionErrorRetryInterval)
                self.getBinaryRequest(url, destFile, maxDownloadAttempts, data=None, attempts=attempts )
            msg = "finished writing to the temp zip dest file {0} size is {1}"
            self.logger.debug(msg.format(destFile, fileSize))
            
            
    def fixUrlPath(self, url):
        # the rest url is going to have directories added to 
        # it later so it is absoultely essential that it end with 
        # a / character for the subsequent urljoin calls to work.
        # making sure that this trailing / character exists here
        if url[len(url) - 1] <> '/':
            url = url + '/'
        return url

class parcelMapAPI(RestBase, Constants):
    
    def __init__(self, url, userName, passWord, destDir, orderStatusFileName=None):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        #self.configLogging()

        RestBase.__init__(self, url, userName, passWord)
        Constants.__init__(self)
        if orderStatusFileName:
            self.orderStatusFileName = orderStatusFileName
        
        self.user = userName
        self.passWord = passWord
        self.url = url
        
        # The directory where the parcelmap data will be downloaded to.
        # the name of the file is always calculated using the constant
        # extractFile which resolves to ParcelMapDirect_{0}.zip
        # where {0} is replaced with the order number
        self.destDir = destDir
        # will get calculated once the order has been placed as the name
        # is dependent on the order number
        self.destFile = None
        
        # as soon as the order is either recovered or placed this var
        # will get set
        self.orderId = None
        
    def configLogging(self):
        '''
        This method is only used during development, once in 
        production or attached to a fmw the logging init should
        be taken care of by the template logging configuration.
        
        '''
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        
        logFile = os.path.join(os.path.dirname(__file__), self.configDir, self.logConfigFile)
        logOutputsDir =  os.path.join(os.path.dirname(__file__), self.logDir)
        logOutputsFilePath = os.path.join(logOutputsDir, self.logFileName )
        if not os.path.exists(logOutputsDir):
            os.makedirs(logOutputsDir)
        print 'print logFile', logFile
        print 'print logOutputsDir', logOutputsDir
        print 'print logOutputsFilePath', logOutputsFilePath
        print 'print self.logFileName', self.logFileName
        logging.config.fileConfig(logFile, defaults={'logfilename': logOutputsFilePath})
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug("First log message")
        self.logger.debug("log config file {0}".format(logFile))
        self.logger.debug("log outputs file {0}".format(logOutputsFilePath))
    
    def requestParcels(self, requestBody):
        '''
        Recieves a python data structure that matches the expected 
        JSON body for a parcelmap order request.
        
        Method will return the json that is returned by the request.
        Up higher will only return the json if the status_code returned
        by the request in the list of success codes.
        '''
        # TODO: Response includes a link, need to figure out
        #       how to parse this out.
        #{"estimatedCompletionTime":"2016-09-03T00:43:57Z","extract":{"jurisdictionCode":"J0084","fromDate":null,"realWorldChanges":"No","fabricSpatialImprovements":"No","parcelFabricExtract":"Yes","geometry":null,"format":"File Geodatabase","SRID":"EPSG:3153"},"orderID":"10621"}

        # directory should be: 
        # $URL/pmd/api/orders HTTP/1.1
        url = self.fixUrlPath(self.url)
        url = urlparse.urljoin(url, self.restPath)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.orderDir)
        self.logger.debug("request parcels url using: {0}".format(url))
        #url = self.fixUrlPath(url)
        completed = False
        retries = 0
        while not completed:
            try:
                response = self.postRequest(url, requestBody)
                completed = True
            except requests.ConnectionError, e:
                msg = "Connection error occurred retrieving the order, error message is: {0}"
                self.logger.error(msg.format(str(e)))
                if retries >= self.ConnectionErrorMaxRetries:
                    msg = "Have tried to retrieve the data {0} times with no sucess," + \
                          " error message is: {1}"
                    self.logger.error(msg.format(url, str(e)))
                    raise
                else:
                    intervalMessage = "received a ConnectionError when trying  " + \
                                      "to connect to the order data.  Pausing {0} " + \
                                      "seconds before trying again."
                    self.logger.warning(intervalMessage.format(self.ConnectionErrorRetryInterval))
                    time.sleep(self.ConnectionErrorRetryInterval)
                    retryMessage = "number of retries is: {0}.  Will try up to a maximum of {1}"
                self.logger.info(retryMessage.format(retries, self.ConnectionErrorMaxRetries))
                retries += 1
        return response
        
    def requestOrderStatus(self, orderID):
        url = self.fixUrlPath(self.url)
        url = urlparse.urljoin(url, self.restPath)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.orderDir)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, str(orderID))
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.statusDir)
        self.logger.debug(" url using is {0}".format(url))
        # sometimes we get max requests exceeded errors, 
        # this loop is intended catch those and try again up 
        # to self.maxRetries interval
        completed = False
        retries = 0
        while not completed:
            try:
                response = self.getRequest(url)
                completed = True
                self.logger.debug("response from rest api for the order status is {0}".format( response))
            except requests.ConnectionError, e:
                msg = "Connection error occurred retrieving the order status, error message is: {0}"
                self.logger.error(msg.format(str(e)))
                if retries >= self.ConnectionErrorMaxRetries:
                    msg = "Have tried to retrieve the status of order number {0} {1} times with no success," + \
                          " error message is: {2}"
                    self.logger.error(msg.format(orderID, retries, str(e)))
                    raise
                else:
                    intervalMessage = "received a ConnectionError when trying  " + \
                                      "to retrieve to the order status.  Pausing {0} " + \
                                      "seconds before trying again. orderid: {1}"
                    self.logger.warning(intervalMessage.format(self.ConnectionErrorRetryInterval, orderID))
                    time.sleep(self.ConnectionErrorRetryInterval)
                    retryMessage = "number of retries is: {0}.  Will try up to a maximum of {1}"
                self.logger.info(retryMessage.format(retries, self.ConnectionErrorMaxRetries))
                retries += 1
        
        # response will look something like this:
        # {u'status': u'Completed', u'size': u'13 MB'}
        self.logger.debug("response from rest api for the order status is {0}".format( response))
        return response
    
    def requestOrderData(self, orderID, destFile):
        url = self.fixUrlPath(self.url)
        url = urlparse.urljoin(url, self.restPath)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.orderDir)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, str(orderID))
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.extractDir)
        url = self.fixUrlPath(url)
        mapFile = self.extractFile.format(orderID)
        url = urlparse.urljoin(url, mapFile)
        self.logger.info("url used for the order data: {0}".format(url))
        self.getBinaryRequest(url, destFile, self.maxParcelMapDownloadAttempts)
        
    def calculatePollingStartInterval(self, expectedDate):
        '''
        This method is going to recieve an expected time for an 
        order to be completed.  We will calculate how far in the
        future that date is, then calculate 75% of that time 
        duration and return it that value in seconds
        
        Subsequent processes will start polling the server at
        that point to determine if the job is complete or not.
        '''
        self.logger.debug("expectedDate is: {0} and has a type of {1}".format(expectedDate, type(expectedDate)))
        pacificTZ = pytz.timezone('Canada/Pacific')
        if type(expectedDate) is str or type(expectedDate) is unicode:
            expectedDateUTC = datetime.datetime.strptime(expectedDate, 
                                                  self.expectedDateFormat)
            expectedDate = pytz.utc.localize(expectedDateUTC, is_dst=None).astimezone(pacificTZ)
            self.logger.info("expected date in local time zone is: {0}, original time string {1}".format(expectedDate, expectedDateUTC))

        currentTime = datetime.datetime.now(tz=pacificTZ)
        self.logger.debug('local time now: {0}'.format(currentTime))
        if expectedDate < currentTime:
            # might as well make it wait 2 seconds
            proportionedInterval = 2
        else:
            intervalToExpectedTime = expectedDate - currentTime
            self.logger.debug("intervalToExpectedTime {0}".format(intervalToExpectedTime))
            proportionedInterval = (intervalToExpectedTime.seconds * 75) / 100
            self.logger.debug("wait interval is: {0}".format(proportionedInterval))
        return proportionedInterval
                
    def getStatusFile(self, dir=None):
        # ift he dir is provided it allows it to be overridden
        if not dir:
            dir = self.destDir
        if not self.orderStatusFileNameFullPath:
            self.orderStatusFileNameFullPath = os.path.join(dir, self.orderStatusFileName)
        return self.orderStatusFileNameFullPath
    
    def existsStatusFile(self):
        statusFile = self.getStatusFile()
        self.logger.debug("status file is {0}".format(statusFile))
        fileExists = os.path.exists(statusFile)
        self.logger.debug("file exists {0}".format(fileExists))
        return fileExists
    
    def deleteStatusFile(self):
        statusFile = self.getStatusFile()
        if os.path.exists(statusFile):
            os.remove(statusFile)
        
    def download(self, requestBody):
        '''
        This method will recieve a request body and will mange
        the rest of the process involved with downloading the
        data described in the request body.
        
        This method will:
            - detect 429 errors pause and resend the request
              when they occur
            - Issue the request and parse the return json 
              object to ensure that indicates the request was
              successfully filled.  returned json is also written 
              to the status file.
            - After request is made the returning object provides
              an estimates completion time.  The script will wait
              until that time has expired and will then attempt 
              to download the data
            - Before any downloads are made the scritp will query
              the parcelmap api to determine if the request is 
              acutally completed.  If not it will enter into a loop
              that will pause for 2 minutes then resend the request
            - Once the request is completed as determined by 
              a query regarding the status, the script will download
              the data.
        '''
        # get the path to the status file
        fullPathStatusFile = self.getStatusFile()
        
        # issue the request and get the response
        #sys.exit() # debug while I make sure this is getting bypassed
        response = self.requestParcels(requestBody)
        self.logger.info("request for parcel map data has been issued")
        self.logger.debug("returned from order request: {0}".format(response))
        
        parcelMapOrder = ParcelMapOrder(response)
        parcelMapOrder.validateOrder()
        self.orderId = parcelMapOrder.getOrderId()
        expectedDate = parcelMapOrder.getExpectedDate()
        self.logger.info("order id is: {0}".format(self.orderId))
        
        # save the parcelmap data to the status file
        parcelMapOrder.saveOrderToStatusFile(fullPathStatusFile)
        
        # order has been placed now monitor it, and when complete continue 
        # with downloading it.
        #self.monitorAndCompleteOrder(self.restKey_orderid, self.restKey_expectedDate)
        self.monitorAndCompleteOrder(self.orderId, expectedDate)
             
    def monitorAndCompleteOrder(self, orderID, expectedDateTime):
        '''
        
        Gets the orderid of the recently placed order, and the expected completion 
        time, and the destination file for where to dump the data once the order is 
        complete.  
        
        queries the rest api until the order with the order id is complete.  Then
        downloads.  Does not query until the expected Date time has expired.
        
        :param  orderID: The order id to monitor and download
        :type orderID: str
        :param  expectedDateTime: expected date time for order to complete.  In 
                                  json datetime format.
        :type expectedDateTime: str
        :param  destinationFile: Path to the destination file that will be created
                                 once the data is ready
        :type destinationFile: str(path)
        '''
        if self.orderId <> orderID:
            self.orderId = orderID
        # parse the date.
        #expectedDate = datetime.datetime.strptime(response[self.restKey_expectedDate], 
        #                                          self.expectedDateFormat)
        # calculate when to start polling for a response.
        pollStart = self.calculatePollingStartInterval(expectedDateTime)
        msg = "parcel map says order should be ready at {0}.  Will start " + \
              'polling parcel map for the order status in {1} seconds '
        self.logger.info(msg.format(expectedDateTime, pollStart))
        time.sleep(pollStart)
        
        # when the status of an order returns and the order is processing 
        # it will return a string that looks like:
        #  - Processing
        #  - Processing (1/3)
        #  - Processing (10/12)
        # 
        # this regular expression will capture those messages
        isProcessingRegex = re.compile('processing\s*(\(\d+///d+\)){0,1}', re.IGNORECASE)
        
        # starting to poll server for order status
        continueCheckingOnOrder = True
        while continueCheckingOnOrder:
            self.logger.info("initiating a status check on the order")
            statusResponse = self.requestOrderStatus(orderID)
            #if statusResponse[self.restKey_status].lower() == 'processing':
            if self.isProcessingRegex.match(statusResponse[self.restKey_status]):
                # keep on waiting
                self.logger.info("order still not ready, waiting for {0} seconds".format(self.pollInterval))
                time.sleep(self.pollInterval)
            elif statusResponse[self.restKey_status].lower() == 'completed':
                msg = 'order {0} is now complete, initiating download'
                self.logger.info(msg.format(orderID))
                continueCheckingOnOrder = False
            else:
                msg = 'returned an unexpected status code, entire' + \
                      'response is: {0}'
                raise ValueError, msg.format(statusResponse)
        # now download the order
        #     def requestOrderData(self, orderID, destFile):
        self.logger.info("order is now ready, retrieving it")
        msg = "pausing for {0} seconds to avoid 429 error..."
        self.logger.info(msg.format(self.ConnectionErrorRetryInterval))
        
        time.sleep(self.ConnectionErrorRetryInterval)
        destFileFullPath = self.getDestinationFilePath()
        self.requestOrderData(orderID, destFileFullPath)
        msg = "order has been downloaded and can be found at {0}"
        self.logger.info(msg.format(destFileFullPath))
    
    def getDestinationFilePath(self):
        retFilePath = None
        if not self.orderId:
            msg = 'Cannot calculate the destination file path becuase the ' +\
                  'order id has not been set yet.  The order id gets set ' + \
                  'either after a new order has been placed, or when an ' +\
                  'existing orders status file is detected and the script ' +\
                  'attempts to pick that order up'
            raise ValueError, msg
        justFile = self.extractFile.format(self.orderId)
        retFilePath = os.path.join(self.destDir, justFile)
        return retFilePath
    
    def downloadJurisdiction(self, jurisdictionId, destinationFile, SRID='EPSG:3153'):
        '''
        Receives a jurisdiction id, script then attempts to download that
        jurisdiction.
        
        # example of a successful request for an order
        {"estimatedCompletionTime":"2016-09-03T00:43:57Z",
        "extract":
            {"jurisdictionCode":"J0084",
             "fromDate":null,
             "realWorldChanges":"No",
             "fabricSpatialImprovements":"No",
             "parcelFabricExtract":"Yes",
             "geometry":null,
             "format":"File Geodatabase",
             "SRID":"EPSG:3153"
        },
        "orderID":"10621"}

        '''
        self.logger.info("Retrieving the parcel data for jurisdictionId: {0}".format(jurisdictionId))
        requestBody = self.defaultJurisDictionRequest
        requestBody[self.restKey_extract][self.restKey_SRID] = SRID
        requestBody[self.restKey_extract][self.restKey_jurisdiction] = jurisdictionId
        self.download(requestBody, destinationFile)
        self.logger.info("Data for jurisdiction: {0} has been downloaded to {1}".format(jurisdictionId, destinationFile))
        
    def downloadBC(self, SRID=None):
        if not SRID:
            SRID = self.defaultSRID
        # put logic in here to deal with:
        # - making the request
        # - retrieve the order id
        # - poll the status of the order every __ minutes
        # - retrieve the order and finish.
        # potentially integrate with FMW.
        self.logger.info("Retrieving the parcel data for the entire province")
        requestBody = self.provincialJsonRequest
        if SRID <> self.defaultSRID:
            requestBody[self.restKey_extract][self.restKey_SRID] = SRID
        self.download(requestBody)
        destFilePath = self.getDestinationFilePath()
        self.logger.info("Data for the entire province has been downloaded to {0}".format(destFilePath))
    
    def unZipFile(self, destFile, srcFGDB):
        '''
        gets the zip file and the full path to it.  The zip file path
        will contain the writeable destination directory.  
        
        Also receives the srcFGDB which is the name of the source 
        file geodatabase used by the FMW.  If this parameter includes
        a path it will be the read-only path.
        
        This method will reconstruct the srcFGDB so that the name 
        of the geodatabase is in the directory of the destFile.
        
        Finally it will unzip the destFile into the srcFGDB, if 
        the srcFGDB already exists it will be deleted.
                
        Delete the existing gdb, replace with the new gdb. 
        '''
        self.logger.debug("input zip file: {0}".format(destFile))
        self.logger.debug("final destination fgdb {0}".format(srcFGDB))
        # name of the final FGDB
        finalFGDBName = os.path.basename(srcFGDB)
        self.logger.debug('finalFGDBName: {0}'.format(finalFGDBName))
        
        # The destination directory specified for zip file
        destDir = os.path.dirname(destFile)
        self.logger.debug('destDir: {0}'.format(destDir))

        # The full path to where the new FGDB should be
        finalFGDBFullPath = os.path.normpath(os.path.join(destDir, finalFGDBName))
        self.logger.debug("Name of the final fgdb path to be updated (r/w) path {0}".format(finalFGDBFullPath))
        # extracting from zip file
        zip_ref = zipfile.ZipFile(destFile, 'r')
        # Get the file list from the zip file
        nameList = zip_ref.namelist()
        # from the filelist get the destination directory (name of 
        # fgdb in the zip file)
        zipDir = os.path.dirname(nameList[0])
        # The name of the fgdb that is going to be created by
        # unzipping the file.
        destFGDB = os.path.normpath(os.path.join(destDir, zipDir))
        self.logger.debug('destFGDB: {0}'.format(destFGDB))
        # todo: should get rid of the delete 
        self.logger.debug("temporary unzip home of the zip file {0}".format(destFGDB))
        # Try to delete the fgdb. 
        #if os.path.exists(destFGDB):
        #    try:
        #        shutil.rmtree(destFGDB)
        #    except:
        #        msg = 'Trying to remove the fgdb that currently exists in ' + \
        #              'the staging area ({0}) so it can be updated.  Unfortunately ' + \
        #              'ran into an error when this was attempted'
        #        self.logger.error(msg.format(destFGDB))
        #        raise
        if not os.path.exists(destFGDB):
            zip_ref.extractall(destDir)
        zip_ref.close()
        
        # finally rename the fgdb that was just extracted to have
        # the name of the destination fgdb
        if os.path.exists(finalFGDBFullPath):
            try:
                self.logger.debug("deleting {0}, then renaming this to that {1}".format(finalFGDBFullPath, destFGDB))
                shutil.rmtree(finalFGDBFullPath)
            except:
                msg = 'Unable to delete the existing FGDB ({0}), so it ' + \
                      'can be replaced by ({1})'
                self.logger.error(msg.format(finalFGDBFullPath,destFGDB ))
        
        shutil.move(destFGDB, finalFGDBFullPath)
             
class FingerPrinting(Constants):

    def __init__(self, destFile, fingerPrintFile):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        #self.logger.setLevel(logging.DEBUG)
        
        self.destFile = destFile
        self.fingerPrintFile = fingerPrintFile
        Constants.__init__(self)
        
    def getCacheFilePath(self):
        pathNoSuffix, zipSuffix = os.path.splitext(self.destFile)
        md5File = pathNoSuffix + self.fingerprintSuffix
        self.logger.debug("fingerprint file: {0}".format(md5File))
        return md5File
        
    def getLastFingerPrint(self):
        '''
        This method is going to retrieve from the cache file 
        the md5 checksum (fingerprint) for the last time the 
        file was downloaded.  (the zip file from which this 
        fingerprint was generated should no longer exist)
        
        The fingerprint file will have the same name as the 
        zip file except it will have a .md5 suffix.  The 
        directory will be the same as the destination date.
        '''
        #md5File = self.getCacheFilePath()
        md5File = self.fingerPrintFile
        self.logger.debug("md5 cache file is {0}".format(md5File))
        md5 = None
        if os.path.exists(md5File):
            self.logger.debug("input md5 exists")
            fh = open(md5File, 'r')
            md5 = fh.readline()
            md5 = md5.replace('\n', '')
            md5 = md5.strip()
            fh.close()
        return md5
        
    def calculateFingerPrint(self):
        md5 = None
        if os.path.exists(self.destFile):
            md5 = hashlib.md5(open(self.destFile, 'rb').read()).hexdigest()  # @UndefinedVariable
        else:
            msg = "Asked to create an MD5 checksum on a file ({0}) that does not exist"
            self.logger.warning(msg.format(self.destFile))
        return md5
    
    def cacheFingerPrint(self):
        #cacheFile = self.getCacheFilePath()
        cacheFile = self.fingerPrintFile
        self.logger.debug("md5 file {0}".format(cacheFile))
        if os.path.exists(cacheFile):
            os.remove(cacheFile)
        md5 = self.calculateFingerPrint()
        fh = open(cacheFile, 'w')
        fh.write(md5)
        msg = "File {0} should exist with the md5 checksum for the file {1}"
        self.logger.debug(msg.format(cacheFile, self.destFile))
        fh.close()
        
    def hasParcelFabricChanged(self):
        pastMd5 = self.getLastFingerPrint()
        currentMd5 = self.calculateFingerPrint()
        retVal = True
        self.logger.debug("md5's are:\n{0}\n{1}".format(pastMd5, currentMd5))
        # clean make sure there are no leading spaces
        if pastMd5 == currentMd5:
            retVal = False
        return retVal
        
class ParcelMapUtil():
    
    def __init__(self):
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        # TODO: figure out how to config logging for this fucker
        self.logger = logging.getLogger(modDotClass)
        
    def getPMPRestToken(self, fmeMacros):
        '''
        Custom transformers sometimes require this piece of informaiton.
        This function ties together the multiple steps required
        to retrieve this information.
        '''
        const = DataBCFMWTemplate.TemplateConstants()
        destKey = fmeMacros[const.FMWParams_DestKey]
        confFile = DataBCFMWTemplate.TemplateConfigFileReader(destKey)
        pmpToken = confFile.getPmpToken(platform.node())
        return pmpToken
    
    def getPMPDict(self, fmeMacros):
        const = DataBCFMWTemplate.TemplateConstants()
        destKey = fmeMacros[const.FMWParams_DestKey]
        confFile = DataBCFMWTemplate.TemplateConfigFileReader(destKey)
        pmpToken = confFile.getPmpToken(platform.node())
        pmpBaseDir = confFile.getPmpRestDir()
        pmpBaseUrl = confFile.getPmpBaseUrl()
        pmpDict = {'token': pmpToken, \
                   'baseurl': pmpBaseUrl, \
                   'restdir': pmpBaseDir}
        return pmpDict
    
    def getCustomTransformerPrefix(self, fmeMacros):
        '''
        When this transformer is used with another fmw the published parmaeters that
        get defined for this transformer will have a prefix inserted in front 
        of them. This method will determine what the prefix is and populate the 
        constants property for macroPrefix
        
        ParcelMapUpdater_TransName_XFORMER_NAME  :  ParcelMapUpdater_TransName

        searches for the _XFORMER_NAME and uses this to extract the name of the 
        custom transfomer
        '''
        prefix = ''
        regex = re.compile('.*_XFORMER_NAME$')
        for key in fmeMacros.keys():
            if regex.match(key):
                #self.logger.debug('transformer name prefix: {0} ', fmeMacros[key])
                prefix = fmeMacros[key]
        if not prefix:
            msg = 'Was unable to determine the transormer published parameter prefix'
            #self.logger.warning(msg)
        return prefix
    
    def readStatusFile(self, statusFile):
        '''
        When an order is placed with the parcel map api the returned 
        json data is cached in a file.  This method is designed to 
        read that file and return it as a ParcelMapOrder object
        
        :param  statusFile: The path to the status file with the parcel
                            map json data.
        :type statusFile: str(path)
        
        :returns: ParcelMapOrder object
        :rtype: ParcelMapOrder
        '''
        if not os.path.exists(statusFile):
            msg = 'The status file {0} does not exist' 
            self.logger.error(msg.format(statusFile))
            raise IOError, msg.format(statusFile)
        with open(statusFile, 'r') as data_file:    
            struct = json.load(data_file)
        parcelMapOrder = ParcelMapOrder(struct)
        return parcelMapOrder
    
class ParcelMapOrder(Constants):
    
    def __init__(self, requestData):
        Constants.__init__(self)
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        self.logger = logging.getLogger(modDotClass)
        self.requestData = requestData
        
    def getOrderId(self):
        '''
        Takes the request data that is returned when an order is placed for a product
        from parcelmap.  It then extracts and returns the orderid from this data 
        structure
        
        :returns: returns the order id extacted from the parcel map data.
        :rtype: str
        '''
        self.validateOrder()
        return self.requestData[self.restKey_orderid]
    
    def getExpectedDate(self):
        self.validateOrder()
        return self.requestData[self.restKey_expectedDate]
    
    def validateOrder(self):
        # make sure the response has an orderid
        if not self.requestData.has_key(self.restKey_orderid):
            msg = "Cannot find an orderid in the response from " + \
                  "the server.  Searching for the key {0} but could " + \
                  "not find it.  Full response is: {1}"
            self.logger.error(msg.format(self.restKey_orderid, self.requestData))
            raise ValueError, msg.format(self.restKey_orderid, self.requestData)
        # make sure the response has an expected date
        if not self.requestData.has_key(self.restKey_expectedDate):
            msg = 'The response does not have an expected date key: {0}' + \
                 'full response is: {1}'
            self.logger.error(msg.format(self.restKey_orderid, self.requestData))
            raise ValueError, msg.format(self.restKey_orderid, self.requestData)
    
    def saveOrderToStatusFile(self, fullPathStatusFile):
        with open(fullPathStatusFile, 'w') as fp:
            json.dump(self.requestData, fp)

        

