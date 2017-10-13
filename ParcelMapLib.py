'''
Created on Sep 1, 2016

@author: kjnether
'''
import datetime
import hashlib
import json
import logging.config
import os.path
import platform
import re
import shutil
import time
import urlparse
import zipfile

import DataBCFMWTemplate
import pytz
import requests


class Constants(object):
    '''
    simple class set up to define constants used by multiple aspects
    of the parcelmap library.
    '''
    # various directory names used by the rest api.

    statusDir = 'status'
    orderDir = 'orders'

    productDir = 'products'
    cannedDir = 'precanned'

    packagedProductType = 'ProvincewideParcelSnapshot'
    infoDir = 'info'
    extractDir = 'extract'
    extractFile = 'ParcelMapDirect_{0}.zip'
    packagedProductFile = 'ParcelMapBCSnapshot_{0}.zip'
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
    restKey_packagedProductDate = 'dateCreated'

    # the format we are expecting the datetime string to come in
    # example: "2016-09-03T00:43:57Z"
    expectedDateFormat = '%Y-%m-%dT%H:%M:%SZ'
    expectedDateFormatMillisec = '%Y-%m-%dT%H:%M:%S.%fZ'

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
    # ConnectionError: HTTPSConnectionPool(host='appstest1.ltsa.ca', port=443):
    # Max retries exceeded with url: /pmd/api/orders/10630/status
    # (Caused by NewConnectionError(
    #  '<requests.packages.urllib3.connection.VerifiedHTTPSConnection object at
    #    0x02D4BAD0>: Failed to establish a new connection: [Errno 11004]
    #    getaddrinfo failed',))
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
    isProcessingRegex = re.compile(r'processing\s*(\(\d+///d+\)){0,1}',
                                   re.IGNORECASE)

    testRequest1 = {'extract' : {
        'jurisdictionCode' : 'J0084',
        'SRID' : defaultSRID,
        'format' : defaultFormat,
        'parcelFabricExtract' : 'Yes',
        'realWorldChanges': 'No',
        'fabricSpatialImprovements': 'No'
    }}

    defaultJurisDictionRequest = {'extract' : {
        restKey_jurisdiction : '{0}',
        restKey_SRID : defaultSRID,
        'format' : defaultFormat,
        'parcelFabricExtract' : 'Yes',
        'realWorldChanges': 'No',
        'fabricSpatialImprovements': 'No'
    }}

    packagedProductRequest = {'pregeneratedSnapshot':
                              {'productType':'ProvincewideParcelSnapshot'}
                             }

    albersBB = [[[0, 0],
                 [3000000, 0],
                 [3000000, 3000000],
                 [0, 3000000],
                 [0, 0]
                ]
               ]

    # geographicBB = [ [ [ -139.06, 48.30],
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
            }}

    def __init__(self):
        pass

class RestBase(object):
    '''
    Manages interaction with the rest api.  Lower level generic.
    '''
    def __init__(self, url, userName, passWord):
        self.logger = logging.getLogger(__name__)
        self.logger.debug(u"first log message")
        self.baseurl = url
        self.userName = userName
        self.passWord = passWord
        # Getting authorization using basic auth (user/password)
        self.authObj = requests.auth.HTTPBasicAuth(userName, passWord)
        self.const = Constants()

    def postRequest(self, url, data):
        '''
        Issues a post request to the url provided, including the data parameter
        as JSON in the body of the post request
        :param url: the url to which the post request is to be sent
        :param data: the data structure to include in the body of the request.
                     this data structure will be converted to a json string
                     in this method.
        '''
        dataAsJsonStr = json.dumps(data, indent=2)
        self.logger.debug(u'json data as string: %s', dataAsJsonStr)
        headers = {u'Content-Type': u'application/json'}
        r = requests.post(url, data=dataAsJsonStr, headers=headers, auth=self.authObj)
        # verify the status code
        if r.status_code < 200 or r.status_code > 299:
            msg = u'Post Request failed, status code is {0}, return text is {1}'
            # self.logger.debug()
            raise ValueError, msg.format(r.status_code, r.text)
        self.logger.info(u"order request returned status code %s", r.status_code)
        return r.json()

    def getRequest(self, url, data=None):
        '''
        Recieves a url and a data structure (dict)  the datastructure is used as
        the body in a get request to the url.  The response body is converted to
        a python dictionary and returned.
        :param url: The url that points to the rest end point
        :param data: A data structure (dict) that will be included in the body
                     of the request to the url.
        :return: a data structure containing the data that was returned from
                 the rest api
        :rtype: dict
        '''
        jsonRep = None
        if data:
            # TODO: add logic to include the data if its passed
            #       to this method, don't need this functionality yet
            pass
        r = requests.get(url, data=data, auth=self.authObj)
        if r.status_code in [429, 503]:
            statusCodeMesg = u"When requesting the the data received http status " + \
                             u"code %s.  Pausing for %s seconds and then will " + \
                             u"re-issue the request"
            self.logger.error(statusCodeMesg,
                              r.status_code,
                              self.const.ConnectionErrorRetryInterval)
            time.sleep(self.const.ConnectionErrorRetryInterval)
            r = requests.get(url, data=data, auth=self.authObj)
        if r.status_code < 200 or r.status_code > 299:
            # problem with the request
            msg = u"GET request returned a status code of %s" + \
                  u"expecting a 200 series code for success"
            raise requests.ConnectionError, msg, r.status_code
        else:
            self.logger.debug(u"raw response for get request is: %s", r)
            jsonRep = r.json()
            self.logger.debug(u"json response is %s", jsonRep)
            self.logger.debug(u"requested url is: %s", url)
        return jsonRep

    def getBinaryRequest(self, url, destFile, maxDownloadAttempts, data=None, attempts=0):
        '''
        Attempts to download the zip file associated with the parcel map order.
        :param url: The url to the data that you want to download
        :param destFile: a file path that you want to use as the destination for
                         the data being downloaded.
        :param maxDownloadAttempts: If attempts to download the file fail, this
                                    parameter identifies the maximum number of
                                    times to retry the attempt before raising
                                    an exception
        :param data: the body of the GET request
        :param attempts: This is only used when the method retries.  This parameter
                         is used to keep track of what attempt the method is currently
                         on. (Don't use this parameter, INTERNAL)
        '''
        self.logger.debug(u"requested destination file is: %s", destFile)
        r = requests.get(url, data=data, auth=self.authObj, stream=True)
        self.logger.info(u"response code from request for order file: %s", r.status_code)
        if attempts > maxDownloadAttempts:
            msg = u"Attempted to retrieve the data {0} times, all have failed!"
            msg = msg.format(attempts)
            raise IOError, msg
        if r.status_code == 429:
            statusCodeMesg = u"When requesting the the data received http status " + \
                             u"code %s.  Pausing for %s seconds and then will " + \
                             u"re-issue the request"
            self.logger.error(statusCodeMesg, r.status_code, self.const.ConnectionErrorRetryInterval)
            time.sleep(self.const.ConnectionErrorRetryInterval)
            attempts += 1
            self.getBinaryRequest(url, destFile, maxDownloadAttempts, data=None, attempts=attempts)
            # r = requests.get(url, data=data, auth=self.authObj, stream=True,
        if r.status_code == 200:
            self.logger.debug(u"attempting to write raw data to %s", destFile)
            with open(destFile, 'wb') as f:
                # print r.content
                # r.raw.decode_content = True
                # print r.raw
                # shutil.copyfileobj(r.raw, f)
                for chunk in r:
                    f.write(chunk)
            fileSize = os.path.getsize(destFile)
            if int(fileSize) == 0:
                attempts += 1
                msg = u"Order zip file: %s was successfully downloaded but has " + \
                      u"a file size of %s.  Deleting this file and trying " + \
                      u"again, attempts %s/%s"
                # msg = msg.format(destFile, fileSize, attempts, maxDownloadAttempts)
                self.logger.debug(msg, destFile, fileSize, attempts, maxDownloadAttempts)
                self.logger.debug(u"sleeping for %s", self.const.ConnectionErrorRetryInterval)
                time.sleep(self.const.ConnectionErrorRetryInterval)
                self.getBinaryRequest(url, destFile, maxDownloadAttempts, data=None, attempts=attempts)
            msg = u"finished writing to the temp zip dest file %s size is %s"
            self.logger.debug(msg, destFile, fileSize)

    def fixUrlPath(self, url):
        '''
        This method adds a trailing path to the end of the url path
        if there is not already a trailing slash
        :param url: the url that you want to have a trailing slash added to.
        :return: the same url as the one provided but with a trailing slash
                 added to it.
        '''
        # the rest url is going to have directories added to
        # it later so it is absoultely essential that it end with
        # a / character for the subsequent urljoin calls to work.
        # making sure that this trailing / character exists here
        if url[len(url) - 1] <> '/':
            url = url + '/'
        self.logger.debug("fixed url path: %s", url)
        return url

class ParcelMapAPI(RestBase, Constants):
    '''
    class provides a python wrapper to the parcel map rest api.
    '''
    def __init__(self, url, userName, passWord, destDir, orderStatusFileName=None):
        # modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        # self.configLogging()

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
        # modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        modDotClass = '{0}'.format(__name__)

        logFile = os.path.join(os.path.dirname(__file__), self.configDir, self.logConfigFile)
        logOutputsDir = os.path.join(os.path.dirname(__file__), self.logDir)
        logOutputsFilePath = os.path.join(logOutputsDir, self.logFileName)
        if not os.path.exists(logOutputsDir):
            os.makedirs(logOutputsDir)
        # print 'print logFile', logFile
        # print 'print logOutputsDir', logOutputsDir
        # print 'print logOutputsFilePath', logOutputsFilePath
        # print 'print self.logFileName', self.logFileName
        logging.config.fileConfig(logFile, defaults={'logfilename': logOutputsFilePath})
        self.logger = logging.getLogger(modDotClass)
        self.logger.debug(u"log config file %s", logFile)
        self.logger.debug(u"log outputs file %s", logOutputsFilePath)

    def requestParcels(self, requestBody):
        '''
        Recieves a python data structure that matches the expected
        JSON body for a parcelmap order request.

        Method will return the json that is returned by the request.
        Up higher will only return the json if the status_code returned
        by the request in the list of success codes.
        '''
        # example of what is returned
        # {"estimatedCompletionTime":"2016-09-03T00:43:57Z","extract":{"jurisdictionCode":"J0084","fromDate":null,"realWorldChanges":"No","fabricSpatialImprovements":"No","parcelFabricExtract":"Yes","geometry":null,"format":"File Geodatabase","SRID":"EPSG:3153"},"orderID":"10621"}

        # directory should be:
        # $URL/pmd/api/orders HTTP/1.1
        url = self.fixUrlPath(self.url)
        url = urlparse.urljoin(url, self.restPath)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.orderDir)
        self.logger.debug(u"request parcels url using: %s", url)
        # url = self.fixUrlPath(url)
        completed = False
        retries = 0
        while not completed:
            try:
                response = self.postRequest(url, requestBody)
                completed = True
            except requests.ConnectionError, e:
                msg = u"Connection error occurred retrieving the order, error " + \
                      u"message is: %s"
                self.logger.error(msg, str(e))
                if retries >= self.ConnectionErrorMaxRetries:
                    msg = u"Have tried to retrieve the data %s times with no sucess," + \
                          u" error message is: %s"
                    self.logger.error(msg, url, str(e))
                    raise
                else:
                    intervalMessage = u"received a ConnectionError when trying  " + \
                                      u"to connect to the order data.  Pausing %s " + \
                                      u"seconds before trying again."
                    self.logger.warning(intervalMessage, self.ConnectionErrorRetryInterval)
                    time.sleep(self.ConnectionErrorRetryInterval)
                    retryMessage = u"number of retries is: %s.  Will try up to" + \
                                   u" a maximum of %s"
                self.logger.info(retryMessage, retries, self.ConnectionErrorMaxRetries)
                retries += 1
        return response

    def requestOrderStatus(self, orderID):
        '''
        Queries the rest api to determine the status of the orderid provided as
        an arguement
        :param orderID: the order id who's status you want to retrieve.
        :return: the json reponse from the rest api about the status of the order
                 and example of what gets returned:
                  {u'status': u'Completed', u'size': u'13 MB'}
        :rtype: dict
        '''
        url = self.fixUrlPath(self.url)
        url = urlparse.urljoin(url, self.restPath)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.orderDir)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, str(orderID))
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.statusDir)
        self.logger.debug(u" url using is %s", url)
        # sometimes we get max requests exceeded errors,
        # this loop is intended catch those and try again up
        # to self.maxRetries interval
        completed = False
        retries = 0
        while not completed:
            try:
                response = self.getRequest(url)
                completed = True
                self.logger.debug(u"response from rest api for the order status" + \
                                  u" is %s", response)
            except requests.ConnectionError, e:
                msg = u"Connection error occurred retrieving the order status, " + \
                      u"error message is: %s"
                self.logger.error(msg, str(e))
                if retries >= self.ConnectionErrorMaxRetries:
                    msg = u"Have tried to retrieve the status of order number %s %s " + \
                          u"times with no success error message is: %s"
                    self.logger.error(msg, orderID, retries, str(e))
                    raise
                else:
                    intervalMessage = u"received a ConnectionError when trying  " + \
                                      u"to retrieve to the order status.  Pausing %s " + \
                                      u"seconds before trying again. orderid: %s"
                    self.logger.warning(intervalMessage,
                                        self.ConnectionErrorRetryInterval,
                                        orderID)
                    time.sleep(self.ConnectionErrorRetryInterval)
                    retryMessage = u"number of retries is: %s.  Will try up to" + \
                                   u" a maximum of %s"
                self.logger.info(retryMessage, retries, self.ConnectionErrorMaxRetries)
                retries += 1

        # response will look something like this:
        # {u'status': u'Completed', u'size': u'13 MB'}
        self.logger.debug(u"response from rest api for the order status is %s", response)
        return response

    def getPackagedProductInfo(self):
        '''
        Hits the parcel map rest api to get information about the packaged
        product end points.
        returns the json data struct returned from the packaged product end
        point
        '''
        url = self.fixUrlPath(self.url)
        url = urlparse.urljoin(url, self.restPath)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.productDir)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.packagedProductType)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.infoDir)
        self.logger.debug(u"url to get packaged product info is %s", url)
        completed = False
        retries = 0
        while not completed:
            try:
                response = self.getRequest(url)
                completed = True
                self.logger.debug(u"response from rest api for the order " + \
                                  u"status is %s", response)
            except requests.ConnectionError, e:
                msg = u"Connection error occurred retrieving the order status," + \
                      u" error message is: %s"
                self.logger.error(msg, str(e))
                if retries >= self.ConnectionErrorMaxRetries:
                    msg = u"Have tried to retrieve the packaged product order " + \
                          u"info %s times with no success, error message " + \
                          u"is: %s"
                    self.logger.error(msg, retries, str(e))
                    raise
                else:
                    intervalMessage = u"received a ConnectionError when trying  " + \
                                      u"to retrieve to the packaged product " + \
                                      u"order info.  Pausing %s seconds  " + \
                                      u"before trying again."
                    self.logger.warning(intervalMessage, self.ConnectionErrorRetryInterval)
                    time.sleep(self.ConnectionErrorRetryInterval)
                    retryMessage = u"number of retries is: %s.  Will try up" + \
                                   u" to a maximum of %s"
                self.logger.info(retryMessage, retries, self.ConnectionErrorMaxRetries)
                retries += 1

        self.logger.debug(u"response from rest api for the order status is %s",
                          response)
        return response

    def requestOrderData(self, orderID, destFile, packagedProduct=False):
        '''
        :param orderID: the order id that was previously placed who's data you
                        want to download.
        :param destFile: the location that you want to download the requested
                         parcel map data to.
        :param packagedProduct: boolean value to indicate whether you are
                                requesting a packaged product
        '''
        url = self.fixUrlPath(self.url)
        url = urlparse.urljoin(url, self.restPath)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.orderDir)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, str(orderID))
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.extractDir)
        url = self.fixUrlPath(url)
        if packagedProduct:
            # unicode
            pkgProdInfo = self.getPackagedProductInfo()
            mapFile = self.packagedProductFile.format(pkgProdInfo[self.restKey_packagedProductDate])
        else:
            mapFile = self.extractFile.format(orderID)
        url = urlparse.urljoin(url, mapFile)
        self.logger.info(u"url used for the order data: %s", url)
        self.getBinaryRequest(url, destFile, self.maxParcelMapDownloadAttempts)

    def requestPackagedOrderData(self, orderId, destFile):
        '''
        using the orderId, an attempt is made to download the data for
        that order id
        :param orderId: The order id for the data we are attempting to
                        download
        :param destFile: The destination file, ie where the data will be
                         downloaded to.
        '''
        # https://apps.ltsa.ca/pmd/api/products/precanned/{orderId}.zip
        url = self.fixUrlPath(self.url)
        url = urlparse.urljoin(url, self.restPath)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.productDir)
        url = self.fixUrlPath(url)
        url = urlparse.urljoin(url, self.cannedDir)
        url = self.fixUrlPath(url)
        cannedDataFile = '{0}.zip'.format(orderId)
        url = urlparse.urljoin(url, cannedDataFile)
        self.logger.debug(u"canned data url: %s", url)

        #    pkgProdInfo = packagedProductDate = self.getPackagedProductInfo()
        #    mapFile = self.packagedProductFile.format(pkgProdInfo[self.restKey_packagedProductDate])
        # else:
        #    mapFile = self.extractFile.format(orderID)
        # url = urlparse.urljoin(url, mapFile)
        # self.logger.info("url used for the order data: {0}".format(url))
        self.getBinaryRequest(url, destFile, self.maxParcelMapDownloadAttempts)

    def calculatePollingStartInterval(self, expectedDate):
        '''
        This method is going to receive an expected time for an
        order to be completed.  We will calculate how far in the
        future that date is, then calculate 75% of that time
        duration and return it that value in seconds

        Subsequent processes will start polling the server at
        that point to determine if the job is complete or not.
        '''
        self.logger.debug(u"expectedDate is: %s and has a type of %s",
                          expectedDate, type(expectedDate))
        pacificTZ = pytz.timezone('Canada/Pacific')
        # if type(expectedDate) is str or type(expectedDate) is unicode:
        # if isinstance(expectedDate, str) or isinstance(expectedDate, unicode):
        if isinstance(expectedDate, (str, unicode)):
            # parcelmap api seems to erratically format the datestring, was
            # returning seconds as ints, but then changed to a float.  This
            # line will split the string on the '.' character in an attempt to
            # decipher what the hell the data string format is.
            if len(expectedDate.split('.')) == 2:
                expectedDateUTC = datetime.datetime.strptime(expectedDate,
                                                             self.expectedDateFormatMillisec)
            else:
                expectedDateUTC = datetime.datetime.strptime(expectedDate,
                                                             self.expectedDateFormat)
            expectedDate = pytz.utc.localize(expectedDateUTC, is_dst=None).astimezone(pacificTZ)
            self.logger.info(u"expected date in local time zone is: %s, " + \
                             u" original time string %s", expectedDate, expectedDateUTC)

        currentTime = datetime.datetime.now(tz=pacificTZ)
        self.logger.debug(u'local time now: %s', currentTime)
        if expectedDate < currentTime:
            # might as well make it wait 2 seconds
            proportionedInterval = 2
        else:
            intervalToExpectedTime = expectedDate - currentTime
            self.logger.debug(u"intervalToExpectedTime %s", intervalToExpectedTime)
            proportionedInterval = (intervalToExpectedTime.seconds * 75) / 100
            self.logger.debug(u"wait interval is: %s", proportionedInterval)
        return proportionedInterval

    def getStatusFile(self, statusFileDir=None):
        '''
        :param statusFileDir: You can override the default directory by populating
                              this optional arguement.
        :return: full path to the status file.
        Returns the path to the status file.  Status file contains
        is the file that should contain the results of the order
        request
        '''
        # ift he dir is provided it allows it to be overridden
        if not statusFileDir:
            statusFileDir = self.destDir
        if not self.orderStatusFileNameFullPath:
            self.orderStatusFileNameFullPath = os.path.join(statusFileDir, self.orderStatusFileName)
        return self.orderStatusFileNameFullPath

    def existsStatusFile(self):
        '''
        :return: identifies if the status file already exists.
        '''
        statusFile = self.getStatusFile()
        self.logger.debug(u"status file is %s", statusFile)
        fileExists = os.path.exists(statusFile)
        self.logger.debug(u"file exists %s", fileExists)
        return fileExists

    def deleteStatusFile(self):
        '''
        If the status file exists it is deleted / removed
        '''
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
        # sys.exit() # debug while I make sure this is getting bypassed
        response = self.requestParcels(requestBody)
        self.logger.info(u"request for parcel map data has been issued")
        self.logger.debug(u"returned from order request: %s", response)

        parcelMapOrder = ParcelMapOrder(response)
        parcelMapOrder.validateOrder()
        self.orderId = parcelMapOrder.getOrderId()
        expectedDate = parcelMapOrder.getExpectedDate()
        self.logger.info(u"order id is: %s", self.orderId)

        # save the parcelmap data to the status file
        parcelMapOrder.saveOrderToStatusFile(fullPathStatusFile)

        # order has been placed now monitor it, and when complete continue
        # with downloading it.
        # self.monitorAndCompleteOrder(self.restKey_orderid, self.restKey_expectedDate)
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
        # expectedDate = datetime.datetime.strptime(response[self.restKey_expectedDate],
        #                                          self.expectedDateFormat)
        # calculate when to start polling for a response.
        pollStart = self.calculatePollingStartInterval(expectedDateTime)
        msg = u"parcel map says order should be ready at %s.  Will start " + \
              u'polling parcel map for the order status in %s seconds '
        self.logger.info(msg, expectedDateTime, pollStart)
        time.sleep(pollStart)

        # when the status of an order returns and the order is processing
        # it will return a string that looks like:
        #  - Processing
        #  - Processing (1/3)
        #  - Processing (10/12)
        #
        # this regular expression will capture those messages
        # isProcessingRegex = re.compile(r'processing\s*(\(\d+///d+\)){0,1}', re.IGNORECASE)

        # starting to poll server for order status
        continueCheckingOnOrder = True
        while continueCheckingOnOrder:
            self.logger.info(u"initiating a status check on the order")
            statusResponse = self.requestOrderStatus(orderID)
            # if statusResponse[self.restKey_status].lower() == 'processing':
            if self.isProcessingRegex.match(statusResponse[self.restKey_status]):
                # keep on waiting
                self.logger.info(u"order still not ready, waiting for %s seconds",
                                 self.pollInterval)
                time.sleep(self.pollInterval)
            elif statusResponse[self.restKey_status].lower() == 'completed':
                msg = u'order %s is now complete, initiating download'
                self.logger.info(msg, orderID)
                continueCheckingOnOrder = False
            else:
                msg = u'returned an unexpected status code, entire' + \
                      u'response is: %s'
                raise ValueError, msg, statusResponse
        # now download the order
        #     def requestOrderData(self, orderID, destFile):
        self.logger.info(u"order is now ready, retrieving it")
        msg = u"pausing for %s seconds to avoid 429 error..."
        self.logger.info(msg, self.ConnectionErrorRetryInterval)

        time.sleep(self.ConnectionErrorRetryInterval)
        destFileFullPath = self.getDestinationFilePath()
        self.requestOrderData(orderID, destFileFullPath)
        msg = u"order has been downloaded and can be found at %s"
        self.logger.info(msg, destFileFullPath)

    def getDestinationFilePath(self):
        '''
        Gets the path that the destination parcelmap zip file should be
        downloaded to.
        '''
        retFilePath = None
        if not self.orderId:
            msg = u'Cannot calculate the destination file path because the ' + \
                  u'order id has not been set yet.  The order id gets set ' + \
                  u'either after a new order has been placed, or when an ' + \
                  u'existing orders status file is detected and the script ' + \
                  u'attempts to pick that order up'
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
        self.logger.info(u"Retrieving the parcel data for jurisdictionId: %s",
                         jurisdictionId)
        requestBody = self.defaultJurisDictionRequest
        requestBody[self.restKey_extract][self.restKey_SRID] = SRID
        requestBody[self.restKey_extract][self.restKey_jurisdiction] = jurisdictionId
        # self.download(requestBody, destinationFile)
        self.download(requestBody)
        self.logger.info(u"Data for jurisdiction: %s has been downloaded to %s",
                         jurisdictionId, destinationFile)

    def downloadBC(self, SRID=None):
        '''
        Puts in a request for a cut of the provinces data.  Monitors the
        status of the order and when its ready downloads the data.

        This can be a very long process, in excess of 24 hours.s
        '''
        if not SRID:
            SRID = self.defaultSRID
        # put logic in here to deal with:
        # - making the request
        # - retrieve the order id
        # - poll the status of the order every __ minutes
        # - retrieve the order and finish.
        # potentially integrate with FMW.
        self.logger.info(u"Retrieving the parcel data for the entire province")
        requestBody = self.provincialJsonRequest
        if SRID <> self.defaultSRID:
            requestBody[self.restKey_extract][self.restKey_SRID] = SRID
        self.download(requestBody)
        destFilePath = self.getDestinationFilePath()
        self.logger.info(u"Data for the entire province has been downloaded to %s",
                         destFilePath)

    def downloadCannedBC(self):
        '''
        Downloads the pre-packaged parcelmap data.
        '''
        self.logger.info(u"Retrieving the pre-packaged parcel data for the entire province")
        requestBody = self.packagedProductRequest

        # fullPathStatusFile = self.getStatusFile()

        # issue the request and get the response
        # sys.exit() # debug while I make sure this is getting bypassed
        response = self.requestParcels(requestBody)
        self.logger.info(u"request for pre-packaged parcel map data has been issued")
        self.logger.debug(u"returned from order request: %s", response)

        parcelMapOrder = ParcelMapOrder(response)
        parcelMapOrder.validateOrder()
        self.orderId = parcelMapOrder.getOrderId()
        # expectedDate = parcelMapOrder.getExpectedDate()
        self.logger.info(u"order id is: %s", self.orderId)

        # save the parcelmap data to the status file
        # No need to save the order as its just a canned file
        # parcelMapOrder.saveOrderToStatusFile(fullPathStatusFile)

        # order has been placed now monitor it, and when complete continue
        # with downloading it.
        # self.monitorAndCompleteOrder(self.restKey_orderid, self.restKey_expectedDate)
        # No need to monitor and complete as the data should
        # just be available
        # self.monitorAndCompleteOrder(self.orderId, expectedDate)
        time.sleep(self.ConnectionErrorRetryInterval)
        destFilePath = self.getDestinationFilePath()
        self.requestPackagedOrderData(self.orderId, destFilePath)
        self.logger.info("Data for the entire province has been downloaded to %s",
                         destFilePath)

    def unZipPackagedProduct(self, zipFile, destFGDB):
        '''
        zipFile - the zip file to unzip
        destFGDB - the destination fgdb path to create

        When downloading a packaged product you will get a zip
        file who's contents are:

        zipFile:
            ParcelMapBCSnapshot_<year>-<month>-<day>.gdb

        This method will extract the gdb to a temporary path with
        the name ParcelMapBCSnapshot_<year>-<month>-<day>.  The
        gdb will reside within that path.  After fully extracted
        the gdb will be moved from that temporary directory to
        the path recieved in the arg/variable: destFGDB


        '''
        self.logger.debug(u"unzip packaged product zip file: %s", zipFile)
        self.logger.debug(u"dest path for output gdb: %s", destFGDB)

        if os.path.exists(destFGDB):
            msg = u'the destination fgdb path: %s already exists so cant unzip there'
            self.logger.info(msg, destFGDB)
        else:
            # The destination directory specified for zip file
            destZipDir = os.path.dirname(zipFile)
            self.logger.debug(u'destZipDir: %s', destZipDir)

            # open from zip file, creating a zip object (zip_ref)
            zipRef = zipfile.ZipFile(zipFile, 'r')
            # Get the file list from the zip file
            nameList = zipRef.namelist()
            # example element from nameList: ParcelMapBCSnapshot_2017-04-16.gdb/a00000004.CatItemsByPhysicalName.atx
            # from the filelist extract the root directory of the contents of the file.
            # this will be the name of the directory geodatabase.  example of
            # a name is: ParcelMapBCSnapshot_2017-04-16.gdb
            FGDBInZipFile = os.path.dirname(nameList[0])
            tmpDir = os.path.join(destZipDir, (os.path.splitext(FGDBInZipFile))[0])
            tmpGDB = os.path.join(tmpDir, FGDBInZipFile)
            self.logger.debug(u"FGDBInZipFile: %s", FGDBInZipFile)
            self.logger.debug(u"tmpDir: %s", tmpDir)
            self.logger.info(u"extracting from the zip file: %s to %s", zipFile, tmpDir)
            if not os.path.exists(tmpDir):
                zipRef.extractall(tmpDir)

            zipRef.close()
            self.logger.info(u'renaming the directory %s to %s', tmpGDB, destFGDB)
            shutil.move(tmpGDB, destFGDB)
            if os.path.exists(tmpDir):
                shutil.rmtree(tmpDir)

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

        This is the method to use when you download the data from a
        extract request.  If you are downloading a packaged
        product use the unZipPackagedProduct() method
        '''
        self.logger.debug(u"input zip file: %s", destFile)
        self.logger.debug(u"final destination fgdb %s", srcFGDB)
        # name of the final FGDB
        finalFGDBName = os.path.basename(srcFGDB)
        self.logger.debug(u'finalFGDBName: %s', finalFGDBName)

        # The destination directory specified for zip file
        destDir = os.path.dirname(destFile)
        self.logger.debug(u'destDir: %s', destDir)

        # The full path to where the new FGDB should be
        finalFGDBFullPath = os.path.normpath(os.path.join(destDir, finalFGDBName))
        self.logger.debug(u"Name of the final fgdb path to be updated (r/w) path %s",
                          finalFGDBFullPath)
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
        self.logger.debug(u'destFGDB: %s', destFGDB)
        # todo: should get rid of the delete
        self.logger.debug(u"temporary unzip home of the zip file %s", destFGDB)
        # Try to delete the fgdb.
        # if os.path.exists(destFGDB):
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
                self.logger.debug(u"deleting %s, then renaming this to that %s",
                                  finalFGDBFullPath, destFGDB)
                shutil.rmtree(finalFGDBFullPath)
            except:
                msg = u'Unable to delete the existing FGDB (%s), so it ' + \
                      u'can be replaced by (%s)'
                self.logger.error(msg, finalFGDBFullPath, destFGDB)

        shutil.move(destFGDB, finalFGDBFullPath)

class FingerPrinting(Constants):
    '''
    Misc methods that help with working with the md5 checksum files
    and comparisons.
    '''

    def __init__(self, destFile, fingerPrintFile):
        self.logger = logging.getLogger(__name__)
        # self.logger.setLevel(logging.DEBUG)

        self.destFile = destFile
        self.fingerPrintFile = fingerPrintFile
        Constants.__init__(self)

    def getCacheFilePath(self):
        '''
        calculates the file path for where the md5 checksum file.
        :return: returns the location of the md5 checksum file
        '''
        pathNoSuffix = os.path.splitext(self.destFile)[0]
        md5File = pathNoSuffix + self.fingerprintSuffix
        self.logger.debug("fingerprint file: %s", md5File)
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

        :return: the md5 for the parcel map data the last time it was downloaded
        '''
        # md5File = self.getCacheFilePath()
        md5File = self.fingerPrintFile
        self.logger.debug("md5 cache file is %s", md5File)
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
        '''
        Calculates the fingerprint for the data that was downloaded from
        the parcel map api.
        :return: the md5 checksum value that was calculated for the parcelmap
                 data that was recently downloaded
        :rtype: str
        '''
        md5 = None
        if os.path.exists(self.destFile):
            md5 = hashlib.md5(open(self.destFile, 'rb').read()).hexdigest()  # @UndefinedVariable
        else:
            msg = "Asked to create an MD5 checksum on a file (%s) that does not exist"
            self.logger.warning(msg, self.destFile)
        return md5

    def cacheFingerPrint(self):
        '''
        Md5 hashes are used to determine if the data has changed since the last
        download.  This method calculates and caches the md5 checksum for the
        parcelmap data that was last downloaded.
        '''
        # cacheFile = self.getCacheFilePath()
        cacheFile = self.fingerPrintFile
        self.logger.debug("md5 file %s", cacheFile)
        if os.path.exists(cacheFile):
            os.remove(cacheFile)
        md5 = self.calculateFingerPrint()
        fh = open(cacheFile, 'w')
        fh.write(md5)
        msg = "File %s should exist with the md5 checksum for the file %s"
        self.logger.debug(msg, cacheFile, self.destFile)
        fh.close()

    def hasParcelFabricChanged(self):
        '''
        retrieves the cached md5 checksum string and then calculates the
        md5 checksum for the data that was retrieved from parcelmap.  Use
        the two different values to determine whether the data has changed
        since the last time it was retrieved.
        '''
        pastMd5 = self.getLastFingerPrint()
        currentMd5 = self.calculateFingerPrint()
        retVal = True
        self.logger.debug("md5's are:\n%s\n%s", pastMd5, currentMd5)
        # clean make sure there are no leading spaces
        if pastMd5 == currentMd5:
            retVal = False
        return retVal

class ParcelMapUtil(object):
    '''
    Miscellaneous utility methods
    '''

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.const = DataBCFMWTemplate.TemplateConstants()

    def getPMPRestToken(self, fmeMacros):
        '''
        Custom transformers sometimes require this piece of informaiton.
        This function ties together the multiple steps required
        to retrieve this information.
        '''
        destKey = fmeMacros[self.const.FMWParams_DestKey]
        confFile = DataBCFMWTemplate.TemplateConfigFileReader(destKey)
        pmpToken = confFile.getPmpToken(platform.node())
        return pmpToken

    def getPMPDict(self, fmeMacros):
        '''
        Returns a dictionary with the parameters required by the constructor
        of the PMP.PMPRestConnect.PMP class
        :param fmeMacros: a dictionary containing the fme macros
        :return: PMP connection dictionary
        :rtype: dict
        '''
        destKey = fmeMacros[self.const.FMWParams_DestKey]
        confFile = DataBCFMWTemplate.TemplateConfigFileReader(destKey)
        pmpToken = confFile.getPmpToken(platform.node())
        pmpBaseDir = confFile.getPmpRestDir()
        pmpBaseUrl = confFile.getPmpBaseUrl()
        pmpDict = {'token': pmpToken, \
                   'baseurl': pmpBaseUrl, \
                   'restdir': pmpBaseDir}
        self.logger.debug("PMP url: %s, baseurl: %s", pmpBaseUrl, pmpBaseDir)
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
                self.logger.debug('transformer name prefix: %s', fmeMacros[key])
                prefix = fmeMacros[key]
        if not prefix:
            msg = 'Was unable to determine the transformer published parameter prefix'
            self.logger.warning(msg)
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
            self.logger.error(msg, statusFile)
            raise IOError, msg.format(statusFile)
        with open(statusFile, 'r') as data_file:
            struct = json.load(data_file)
        parcelMapOrder = ParcelMapOrder(struct)
        return parcelMapOrder

class ParcelMapOrder(Constants):
    '''
    This is an api that sits on top of the parcel map response
    data from a requested order request.
    '''

    def __init__(self, requestData):
        Constants.__init__(self)
        self.logger = logging.getLogger(__name__)
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
        '''
        returns the expected date for the parcel map order.
        '''
        self.validateOrder()
        return self.requestData[self.restKey_expectedDate]

    def validateOrder(self):
        '''
        Makes sure that the response from the parcel map api for the order
        includes a orderid and an expected date.

        Expected date is necessary as it is used to initiate a poller that
        monitors the status of the order.
        '''
        # make sure the response has an orderid
        if not self.requestData.has_key(self.restKey_orderid):
            msg = "Cannot find an orderid in the response from " + \
                  "the server.  Searching for the key {0} but could " + \
                  "not find it.  Full response is: {1}"
            msg = msg.format(self.restKey_orderid, self.requestData)
            self.logger.error(msg)
            raise ValueError, msg
        # make sure the response has an expected date
        if not self.requestData.has_key(self.restKey_expectedDate):
            msg = 'The response does not have an expected date key: {0}' + \
                 'full response is: {1}'
            msg = msg.format(self.restKey_orderid, self.requestData)
            self.logger.error(msg)
            raise ValueError, msg

    def saveOrderToStatusFile(self, fullPathStatusFile):
        '''
        Writes the order status json object to the status file.
        :param fullPathStatusFile: path to the status file that is to be
                                   created
        '''
        with open(fullPathStatusFile, 'w') as fp:
            json.dump(self.requestData, fp)
