'''
Created on Oct 11, 2017

@author: kjnether

This is an attempt to create an emailer, that can be easily encorporated into
The fme framework.

It gets called by the shutdown.
- looks for a published parameter named EMAILER which is a multiline text
  entry (described here)
- The formulates an email with the jobs log attached.

'''

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os.path
import re
import smtplib

#import DataBCFMWTemplate


class EmailFrameworkBridge(object):
    '''
    This method is the outer interface that is used by the FME framework.
    It recieves the fme object.  Using the fme object it reads the published
    parameters and searches for published parameters that identify the fmw
    has been configured to send custom notifications out with.

    Optional published parameters are configured for the framework, that are used
    to control email notifications.  They are:

      - NOTIFIY_SUCCESS: A line delimited list of emails to send if the job
                         was successful
      - NOTIFY:          A line delimited list of email addresses to send
                         notifications to regardless of whether the jobs
                         succeeds or fails.
      - NOTIFY_FAILURE: A line delimited list of email addresses to send out if
                        the job fails.
    '''

    def __init__(self, fmeObj, const, params, config):
        self.fmeObj = fmeObj
        self.logger = logging.getLogger(__name__)
        self.logger.debug("constructing emailer object")
        
        self.const = const #TemplateConstants
        self.params = params #calcparamsbase
        self.config = config # templateconfigfile
                
        #self.const = DataBCFMWTemplate.TemplateConstants()
        #self.params = DataBCFMWTemplate.TemplateConfigFileReader(
        #    self.fmeObj.macroValues[self.const.FMWParams_DestKey])
        self.email = None

        # print the macroValues
        # self.printMacros()
        self.notifyAll = None
        self.notifyFail = None
        self.notifySuccess = None
        
        self.getNotificationEmails()

    def printMacros(self):
        '''
        This is a debugging method that logs the macro values
        '''
        macroKeys = self.fmeObj.macroValues.keys()
        macroKeys.sort()
        msg = "{0} = {1}  ({2})"
        for macroKey in macroKeys:
            value = str(self.fmeObj.macroValues[macroKey])
            # value2 = self.fmeObj.resolveFMEMacros(macroKey)
            msgFormatted = msg.format(macroKey, value, value)
            self.logger.debug(msgFormatted)

    def reformatEmailAddresses(self, emailString):
        '''
        fme seems to reformat email addresses that are included in multiline text
        parameters. For example the entries:
             bill@the.cat.com
             opus@the.cat.com

        turn into:
        bill<at>the.cat.com<lf>opus<at>the.cat.com

        This script will convert them into a python 'list' of email addresses
        where <at> turns into @ and the <lf> turns into an address delimiter

        '''
        # DEBUG-Emailer.printMacros-74: NOTIFY_ALL = kevin.netherton<at>gov.bc.ca
        #       <lf>fran.tarkington<at>gmail.com  (kevin.netherton<at>gov.bc.ca<lf>
        #       fran.tarkington<at>gmail.com)
        emailList = emailString.split('<lf>')
        for lstCntr in range(0, len(emailList)):
            emailList[lstCntr] = emailList[lstCntr].replace('<at>', '@').strip()
        self.logger.debug("reformatted list: %s", emailList)
        return emailList

    def getNotificationEmails(self):
        '''
        Reads the published parameters asssociated with the fme job and determines
        if there are any email notification parameters defined and if so populates
        the address properties with the contents of the various email parameters.
        '''
        self.logger.debug("getting the notification email addresses")
        for pubParam in self.fmeObj.macroValues:
            if pubParam == self.const.FMWParams_Notify_All:
                self.notifyAll = self.fmeObj.macroValues[pubParam]
            if pubParam == self.const.FMWParams_Notify_Fail:
                self.notifyFail = self.fmeObj.macroValues[pubParam]
            if pubParam == self.const.FMWParams_Notify_Success:
                self.notifySuccess = self.fmeObj.macroValues[pubParam]

    def areNotificationsDefined(self):
        '''
        Returns a true or false indicating whether a notification has been
        configured for a job.
        :return: boolean telling us whether the job is configured for email
                 notification or not.
        '''
        params2Check = [self.notifyAll, self.notifyFail, self.notifySuccess]
        returnValue = False

        for notificationList in params2Check:
            if notificationList:
                returnValue = True
        self.logger.debug("notifications defined?: %s", returnValue)
        return returnValue

    def sendNotifications(self):
        '''
        checks to see if notifications are defined in the published parameters
        if notification exist then extracts and parses the values from those
        published parameters and puts them into a list of email addresses.

        Grabs the information out of the fmw and the fme object that need to be
        included in the email message, (subject, attachemetn, status, source
        parameter, destination parameters etc)

        Finally it puts all that information together into an email message and
        sends it to the addresses identifies in the various notification parameters.
        '''
        if self.areNotificationsDefined():
            self.logger.debug("There are parameters in the FMW for notifications")

            fromAddress = self.config.getEmailFromAddress()
            # toAddress are a list
            toAddress = self.getEmailAddresses()
            if toAddress:
                subject = self.getEmailSubject()
                body = self.getLogFileBody()
                fmwLogFileName = self.getFMWFileAsLogFile()
                self.logger.debug("toAddress: %s", toAddress)
                #    emailTo, emailFrom, emailSubject, emailBody=None):
                emailMessage = Email(emailTo=toAddress, emailFrom=[fromAddress],
                                     emailSubject=subject, emailBody=body,
                                     fmwFileName=fmwLogFileName)

                logFile = self.fmeObj.logFileName
                emailMessage.addAttachement(logFile)

                emailServer = EmailServer(self.config)

                sender = SendEmail(emailServer, emailMessage)
                sender.setup()
                sender.send()
            else:
                self.logger.debug("notification parameters are defined but they " + \
                                  "are all blank, ie no addresses defined in any " + \
                                  "of the parameters")
        else:
            self.logger.debug("notification are not configured")

    def getFMWFileAsLogFile(self):
        '''
        Retrieves the name of the fmw that is being run, stips off the .fmw suffix
        and adds the .log suffix, then returns it.  This is just a text parameter
        and is used later on when naming the log file that is being attached to
        the notification.  For example where the fmw being run is called:
          - myfmwName.fmw

        The method will return:
          - myfmwName.log
        '''
        self.logger.debug("getting the fmw log file label")
        fmwPath = self.fmeObj.macroValues[self.const.FMWMacroKey_FMWName]
        fmwName = os.path.basename(fmwPath)
        fmwNoSuffix = os.path.splitext(fmwName)[0]
        return '{0}.log'.format(fmwNoSuffix)

    def getLogFileBody(self):
        '''
        Returns what should be included in the body of the email message
        '''
        self.logger.debug("getting the notification body")
        # body = 'This is the body for the message, will come ' + \
        #       'back and propertly format this'

        body = """\
                 <html>
                 <head></head>
                 <body>Notification email for the FME Server Replication Script:{0}<br>
                       Source parameters:<br><br>
                       {1}<br>--------------------<br><br>
                       Destination params are:<br> {2}<br><br>
                       The job number is: {3}<br>
                       The job status/success is: {4}<br><br><br>
                       The full log for the replication is attached to this
                       email.
                 </body>
                 </html>
              """
        status = self.fmeObj.status
        if isinstance(status, str):
            status = unicode(status)
        scriptName = self.fmeObj.macroValues[self.const.FMWMacroKey_FMWName]
        self.logger.debug("script name: %s", scriptName)
        source = self.getSource()
        dest = self.getDestination()
        jobNum = self.getJobNum()
        self.logger.debug("the body format is: %s", body)
        body = body.format(scriptName, source, dest, jobNum, status)
        return body

    def getJobNum(self):
        '''
        Retrieves the job number if the job is run on FME Server.  If not it
        just gets a null.
        '''
        # check for pub param: FME_JOB_ID
        # otherwise try to get from LOG_FILENAME
        jobNum = None
        if self.const.FMEMacroKey_JobId in self.fmeObj.macroValues:
            # job id exists, get the job id from this parameter
            jobNum = self.fmeObj.macroValues[self.const.FMEMacroKey_JobId]
            self.logger.debug("job number from the parameter (%s) is %s",
                              self.const.FMEMacroKey_JobId, jobNum)
        else:
            if self.const.FMEMacroKey_LogFileName in self.fmeObj.macroValues:
                logFile = self.fmeObj.macroValues[self.const.FMEMacroKey_LogFileName]
                self.logger.debug("log file from the macro values parameter (%s), is %s",
                                  self.const.FMEMacroKey_LogFileName, logFile)
            else:
                logFile = self.fmeObj.logFileName
                self.logger.debug("log file from the fme property  logFileName, is %s",
                                  logFile)
            justLogFileName = os.path.basename(logFile)
            logFileNosuffix = os.path.splitext(justLogFileName)[0]
            # if its an fme server job then the log file name will start with
            # job_<jobid>
            fmeServerLogFileRegex = re.compile(r'^job_\d+$')
            if fmeServerLogFileRegex.match(logFileNosuffix):
                jobNum = logFileNosuffix.replace('job_', '')
                self.logger.debug("job number extracted from the log name is: %s",
                                  jobNum)
        logFile = self.fmeObj.logFileName
        # if the log file starts with
        self.printMacros()
        return jobNum

    def getSource(self):
        '''
        Gets the source for the current job, for now just rips through
        all the published parameters and returns anything that starts
        with SRC_ with the exception of SRC_PASSWORD
        '''
        self.logger.debug("getting the sources for the script")
        returnParamList = []
        paramTmpltStr = '{0} = {1}\n'
        regexObj = re.compile('SRC_.*')
        omitRegexObj = re.compile(".*_PASSWORD$")
        self.logger.debug("regex's are now compiled")
        paramKeys = self.fmeObj.macroValues.keys()
        paramKeys.sort()
        for param in paramKeys:
            if regexObj.match(param):
                # source param=
                if not omitRegexObj.match(param):
                    value = self.fmeObj.macroValues[param]
                    self.logger.debug("%s = %s", param, value)
                    paramStr = paramTmpltStr.format(param, value)
                    returnParamList.append(paramStr)
        return returnParamList

    def getDestination(self):
        '''
        reads the published parameters for the script and assembles a string that
        describes the destination for this script.
        '''
        # start by searching for DEST_DB_ENV_KEY.  If it exists
        # then just use that as the destination.
        self.logger.debug("getting the destinations string")
        retStr = 'None'

        destTmplt = 'schema: {0} host: {1} service name: {2}'

        if self.const.FMWParams_DestKey in self.fmeObj.macroValues:
            #params = DataBCFMWTemplate.CalcParams(self.fmeObj.macroValues)
            host = self.config.getDestinationHost()
            servName = self.config.getDestinationServiceName()
            destSchema = self.params.getDestinationSchema()
            retStr = destTmplt.format(destSchema, host, servName)
        self.logger.debug("dest string: %s", retStr)
        return retStr
    
    def getDestDbEnvKey(self):
        '''
        Attempts to retrieve the destination database environment key.  If no 
        key can be found will return a null value.
        
        :return: Destination Database environment key
        :rtype: str
        '''
        self.logger.debug("getting the destinations string")
        destDbEnvKey = None
        if self.const.FMWParams_DestKey in self.fmeObj.macroValues:
            destDbEnvKey = self.fmeObj.macroValues[self.const.FMWParams_DestKey]
        return destDbEnvKey
        
    def getEmailSubject(self):
        '''
        Assembles an email subject line based on the name of the fmw that is
        being run, and whether the run was a success or not.
        '''
        self.logger.debug("constructing the subject line")
        fmwName = self.fmeObj.macroValues[self.const.FMWMacroKey_FMWName]
        dbEnv = self.getDestDbEnvKey()
        status = self.getStatus()
        if status:
            statusText = 'Success'
        else:
            statusText = 'Failure'
        subjectText = '{0} - {1} - {2}'
        subjectText = subjectText.format(fmwName, statusText, dbEnv)
        return subjectText

    def getStatus(self):
        '''
        Gets the status string from the fme object.  Returns true or false
        to indicate success.
        :return: boolean indicating sucess
        :rtype: bool
        '''
        status = None
        if isinstance(self.fmeObj.status, bool):
            status = self.fmeObj.status
        elif isinstance(self.fmeObj.status, str):
            status = False
            if self.fmeObj.status.upper().strip() == 'TRUE':
                status = True
        return status

    def getEmailAddresses(self):
        '''
        Email addresses can be defined
        '''
        # get the job status, and configure email messages according to the
        # contents
        self.logger.debug("getting the email addresses now.")
        status = self.getStatus()
        emailAddresses = []
        if self.notifyAll:
            notifyAll = self.reformatEmailAddresses(self.notifyAll)
            self.logger.debug("notifyall: %s", notifyAll)
            emailAddresses.extend(notifyAll)
        if status:
            if self.notifySuccess:
                notifySuccess = self.reformatEmailAddresses(self.notifySuccess)
                self.logger.debug("notifySuccess: %s", notifySuccess)
                emailAddresses.extend(notifySuccess)
        else:
            if self.notifyFail:
                notifyFail = self.reformatEmailAddresses(self.notifyFail)
                self.logger.debug("notifyFail: %s", notifyFail)
                emailAddresses.extend(notifyFail)
        return emailAddresses

class EmailServer(object):
    '''
    Contains the parameters that are required to actually send
    an email, things like SMTP server, ports etc.

    :ivar param: This is a DataBCFMWTemplate.TemplateConfigFileReader object.
                 this class will extrac the information it needs from the
                 object
    :ivar smtpServer: the smtp server name
    :ivar smtpPort: the smtp port
    '''
    def __init__(self, config):
        self.config = config
        self.smtpServer = None
        self.smtpPort = None
        self.parseParams()

    def parseParams(self):
        '''
        retrieves the email smtp server and port from the fme framework config
        file.
        '''
        self.smtpServer = self.config.getEmailSMTPServer()
        self.smtpPort = self.config.getEmailSMTPPort()

    def getSMTPPort(self):
        '''
        Returns the SMTP port
        '''
        return self.smtpPort

    def getSMTPServer(self):
        '''
        Returns the SMTP server name
        '''
        return self.smtpServer

class Email(object):
    '''
    This is a class that contains the structure of an email
    message including:
      - To
      - From
      - Subject
      - Body

    :ivar emailTo: a list of email addresses that the message should be sent to.
    :ivar emailFrom: a list of email addresses that the message should appear to
                     be sent from
    :ivar emailSubject: email subject line
    :ivar fmwFileName: the path to the fmw file that was run.
    :ivar emailBody: the body of the message of the email
    :ivar attachementFilePath: if the email is to include an attachement the path
                               to the file that is to be attached is defined here.
    '''
    def __init__(self, emailTo, emailFrom, emailSubject, fmwFileName, emailBody=None):
        self.emailTo = emailTo
        self.emailFrom = emailFrom
        self.emailSubject = emailSubject
        self.emailBody = emailBody
        self.fmwFileName = fmwFileName
        self.attachementFilePath = None
        if not isinstance(self.emailTo, list):
            msg = 'The emailTo parameter should be a list.  It is currently a' + \
                  ' {0}.  Values: {1}'
            msg = msg.format(type(self.emailTo), self.emailTo)
            raise ValueError, msg
        if not isinstance(self.emailFrom, list):
            msg = 'The emailFrom parameter should be a list.  It is currently a' + \
                  ' {0}.  Values: {1}'
            msg = msg.format(type(self.emailFrom), self.emailFrom)
            raise ValueError, msg

    def getFMWNameLogFile(self):
        '''
        :return: a log file with the same name as the fmw file that is being run
                 but with a .log suffix.
        '''
        return self.fmwFileName

    def addAttachement(self, attachmentFilePath):
        '''
        Defines the location of the file that is to be attached to the email.
        '''
        self.attachementFilePath = attachmentFilePath

    def getAttachementFilePath(self):
        '''
        :return: a string to valid path where the file that is to be attached
                 is located
        '''
        return self.attachementFilePath

class SendEmail(object):
    '''
    This class contains the methods that can be used to send the actual email,

    :ivar logger: needs no explaination
    :ivar emailServer: a EmailServer object
    :ivar emailObj: a Email object
    :ivar msg: the MIMEMultipart message object that is going to be populated
               by this script
    :ivar setupComplete: a flag used to identify if the email message setup has
                         been completed or not
    '''

    def __init__(self, emailSrvr, emailObj):
        self.logger = logging.getLogger(__name__)
        self.emailServer = emailSrvr
        self.emailObj = emailObj
        self.msg = MIMEMultipart()
        self.setupComplete = False

        # verify the types of some args
        if not isinstance(emailSrvr, EmailServer):
            msg = 'The property emailSrvr in the class constructor received an ' + \
                  'object of type {0}.  This property must of type: EmailServer'
            raise ValueError, msg.format(type(emailSrvr))
        if not isinstance(emailObj, Email):
            msg = 'The property emailObj in the class constructor received an ' + \
                  'object of type {0}.  This property must of type: Email'
            raise ValueError, msg.format(type(emailObj))

    def setup(self):
        '''
        Constructs the email message, by adding the to / from / subject / body
        and attachements to it.
        '''
        self.msg["Subject"] = self.emailObj.emailSubject
        self.msg["From"] = ','.join(self.emailObj.emailFrom)
        # msg["To"] = "datamaps@gov.bc.ca,DataBCDA@gov.bc.ca,dataetl@gov.bc.ca,
        #              datadba@gov.bc.ca"
        self.msg["To"] = ','.join(self.emailObj.emailTo)
        self.logger.debug("Email to addresses include: %s",
                          ','.join(self.emailObj.emailTo))

        # Create the body of the message (a plain-text and an HTML version).
        body = MIMEText(self.emailObj.emailBody, 'html')

        # Record the MIME types of both parts - text/plain and text/html.
        # body = MIMEText(html, 'html')
        # self.msg.attach(MIMEText(body))
        self.msg.attach(body)
        self.setupComplete = True

    def send(self):
        '''
        takes the contents of the Email object defined in the constructor and
        extracts the necessary properties from it to assemble an email, then
        initiates communciation with the smtp server defined in the property
        emailServer, and finally sends the email.
        '''
        if not self.setupComplete:
            self.setup()
        emailServerName = self.emailServer.getSMTPServer()
        emailServerPort = self.emailServer.getSMTPPort()
        self.logger.debug("server: %s port: %s", emailServerName, emailServerPort)

        # add attachment here
        attacheFilePath = self.emailObj.getAttachementFilePath()
        if attacheFilePath:
            fmwLogName = self.emailObj.getFMWNameLogFile()
            self.addAttachement(attacheFilePath, fmwLogName)
        self.logger.debug("attempting to send message now")
        smtp = smtplib.SMTP(emailServerName, emailServerPort)
        self.logger.debug("smtp server object created, sending email now")
        smtp.sendmail(self.msg["From"], self.emailObj.emailTo, self.msg.as_string())
        self.logger.debug("notification should now be sent.")
        smtp.quit()

    def addAttachement(self, attachementFile, fmwNameLog):
        '''
        :param attachementFile: a string that describes a valid path to the file
                                that is going to be attached to the email.
        :param fmwNameLog: When the file is attached to the email this is the
                           name that will be assigned to that file as an
                           email attachement.
        This method takes the path defined in attachementFile and does the
        actual attachment of that file to the email that is to be sent.
        '''
        self.logger.debug("attachementFile: %s", attachementFile)
        with open(attachementFile, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=fmwNameLog
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % fmwNameLog
        self.msg.attach(part)
