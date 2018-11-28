'''
Created on Mar 22, 2018

@author: kjnether
'''
import logging
import os.path
import re
import shutil
import subprocess
import sys
import tempfile
import datetime
import platform

import DataBCFMWTemplate


def attemptFMEObjectsImport():
    global fmeobjects
    logger = logging.getLogger(__name__)
    try:
        logger.debug("Attempt 1: importing the fmeobjects module")
        import fmeobjects  # @UnresolvedImport
        logger.debug("Attempt 1: successfully imported the fmeobjects module")
    except:

        # once move to production can extract the fme path from the
        # macro value FME_HOME and pass those parameters to this script
        pathList = os.environ['PATH'].split(';')
        sys.path.insert(0, r'E:\sw_nt\FME2017\fmeobjects\python27')
        os.environ['PATH'] = ';'.join(pathList)
        logger.debug("Attempt 2: importing the fmeobjects module")
        import fmeobjects  # @UnresolvedImport
        logger.debug("Attempt 2: successfully imported the fmeobjects module")


class Reader(object):
    '''
    Place to put ffs file reader functionality

    Very simple at the moment but can anticipate as we move towards autmated
    error reporting we will be in a position to add more functionality
    that reports on specific features etc.

    '''

    def __init__(self, ffsFile, fmeInstallPath, retry=False):
        '''

        :param ffsFile: the ffs file who's features we want to count
        :type ffsFile: str
        :param fmeInstallPath: the path to the fme install that is to be used
                               to get the fmeobjects requirement to query
                               the ffs file.
        :type fmeInstallPath:  str
        :param retry: identifies if this run is a result a automated
                      re-run.
        :type retry: bool
        '''
        self.retry = retry
        self.ffsFile = ffsFile
        self.stdoutIDStr = 'ffsFeatures:'
        self.stdoutTemplate = self.stdoutIDStr + ' {0}'
        self.logger = logging.getLogger(__name__)
        self.fmeInstallPath = fmeInstallPath
        # double check that the file exists
        if not os.path.exists(self.ffsFile):
            msg = 'you specified an FFS file: {0} that does not exist'
            msg = msg.format(self.ffsFile)
            self.logger.error(msg)
        # attemptFMEObjectsImport()

    def getFMEObjects(self):
        '''
        This method is used when the ffs file feature count is to take place
        in the same process as the requesting process.  When this happens
        it is likely that a different version of python is being used than
        the version supplied with FME.  As a result we need to add the paths
        for where python should find fmeobjects.

        This is further complicated by having multiple installs (32 and 64bit)
        This method will do the following:
          - is the current python 32 or 64 bit
          - get the 32 / 64 bit fme directory
          - add the 64 bit fme directory to the path
          - attempt to import fme objects.
        '''
        bit = platform.architecture()[0]
        params = DataBCFMWTemplate.TemplateConfigFileReader('DLV')
        use32Bit = False
        # possible values: '32bit' / '64bit'
        if bit == '32bit':
            use32Bit = True
        fmeInstallPathTmplt = params.getFMERootDirTmplt(bit32=use32Bit)
        # fmeInstallPathTmplt contains a format param {0} where the release
        # year gets inserted.  Going to start with whatever year it is and count
        # backwards 5 years at a time until an install path is found
        now = datetime.datetime.now()
        year = now.year
        yearRange = 5
        for fmeYear in range(year, year - yearRange, -1):
            fmePath = fmeInstallPathTmplt.format(fmeYear)
            if os.path.exists(fmePath):
                break
        if not os.path.exists(fmePath):
            msg = 'unable to find a fme install path, years tried {0} to ' + \
                  '{1} using the install path template string {2}'
            msg = msg.format(year - yearRange, year, fmeInstallPathTmplt)
            raise ValueError, msg
            
        fmeObjPaths = os.path.join(fmePath, 'fmeobjects', 'python27')
        self.logger.debug("fmeObjPaths: %s", fmeObjPaths)
        sys.path.insert(0, fmeObjPaths)
        global fmeobjects
        import fmeobjects

    def getFeatureCountSameProcess(self):
        '''
        bascially normal operation, just calls this method, it creates an
        ffs file reader counts the features and returns them
        :return: The number of features found in the FFS file.
        :rtype: int
        '''
        self.getFMEObjects()
        tempDir = os.path.dirname(self.ffsFile)
        # ffsFile = tempfile.NamedTemporaryFile(dir=tempDir, delete=False)
        ffsFile = tempfile.mktemp(suffix='.ffs', dir=tempDir, prefix='tmp_')
        self.logger.debug("tempfile name: %s", ffsFile)
        shutil.copyfile(self.ffsFile, ffsFile)
        self.logger.debug("copied %s to %s", self.ffsFile, ffsFile)
        # attemptFMEObjectsImport(self.fmeInstallPath)
        try:
            self.logger.debug("starting to open and read the ffs file: %s", ffsFile)
            reader = fmeobjects.FMEUniversalReader('FFS', False, [])  # @UndefinedVariable
            self.logger.debug("created the reader... now try reading")
            # reader.open(self.ffsFile, [])
            reader.open(ffsFile, [])

            # Read all the features on the dataset.
            feature = reader.read()
            # atribNames = feature.getAllAttributeNames()
            # self.logger.debug( 'atribNames: %s', atribNames)
            featureCnt = 0
            while feature != None:
                featureCnt += 1
                # Just log each feature.
                # print 'feature: %s', feature
                feature = reader.read()
            self.logger.info('total features: %s', featureCnt)
            # Close the reader before leaving.
            reader.close()
        finally:
            if os.path.exists(ffsFile):
                self.logger.debug("cleaning up the temp file: %s", ffsFile)
                os.remove(ffsFile)
        return featureCnt

    def getPythonInstallRootDirectory(self, param):
        '''
        starts by querying the registry to find the python install path.
        if it can't read the registry then it attempts to get hard coded
        paths from the template config file.  If can't get it there then
        raises and error.
        '''
        pythonRootDir = None
        try:
            # this will get the arcpy paths, and add them to the sys.path
            # parameter which should then allow for use of arcpy using the
            # fme python default interpreter

            # starting by trying to retrieve from the registry
            pyPaths = DataBCFMWTemplate.InstallPaths.PythonInstallPaths()
            pythonRootDir = pyPaths.getInstallDir()
            self.logger.debug("pythonpath: %s", pythonRootDir)
        except WindowsError, e:
            #
            self.logger.exception(e)
            msg = "was unable to pull the arc install from the registry.  trying " + \
                  'to guess what the install location is before failing.'
            self.logger.warning(msg)
            pythonRootDir = param.getPythonRootDir()
        except Exception, e:
            self.logger.exception(e)
            self.logger.error('re-raising the exception ')
            raise
        return pythonRootDir

    def getThisFileAsPy(self):
        '''
        gets __file__ if the suffix is .pyc then switches to .py and
        returns
        '''
        thisFileList = os.path.splitext(__file__)
        thisFileList = list(thisFileList)
        self.logger.debug("thisFileList1: %s", thisFileList)
        if thisFileList[1] == '.pyc':
            thisFileList[1] = '.py'
        thisFileString = ''.join(thisFileList)
        self.logger.debug("thisFileString: %s", thisFileString)
        return thisFileString

    def getFMEInstallPath(self, param, fmeVersion=None):
        '''
        If fmeVersion is specified will append this number to the end of the
        install path template retreived from the config file.

        If no fmeVersion is provided then the method will start with 2015 and
        increment by 1 until it finds a install path that exists.  If one
        is found it is returned.  If it is not returned an error will be
        raised.

        :param param: a reference to TemplateConfigFileReader object used
                      to retrieve information from the conf file.
        :param fmeVersion: (optional) The FME Version (4 digit year) to
                           append to the fme template install path that
                           gets retrieved from the config file.
        '''
        # verify the fmeVersion if provided was a 4 digit number
        if fmeVersion:
            fmeVersionStr = '{0}'.format(fmeVersion)
            if len(fmeVersionStr) != 4 or not fmeVersionStr.isdigit():
                msg = 'fmeVersion provided: {0} is an invalid value.  Must' + \
                      'be a 4 digit number'
                msg = msg.format(fmeVersion)
                raise ValueError(msg)

        fmeInstallPathTmplt = param.getFMERootDirTmplt()

        fmeInstallDir = None
        if fmeVersion:
            fmeInstallDir = fmeInstallPathTmplt.format(fmeVersion)
        else:
            now = datetime.datetime.now()
            for testFMEVersion in range(2015, now.year + 1):
                curPath = fmeInstallPathTmplt.format(testFMEVersion)
                if os.path.exists(curPath):
                    fmeInstallDir = curPath
                    break
            if not os.path.exists(fmeInstallDir):
                msg = 'unable to find an install path to FME.  Path ' + \
                      'template that was tested: %s'
                self.logger.error(msg, fmeInstallPathTmplt)
                raise IOError(msg)
        fmeInstallDir = os.path.realpath(os.path.normpath(fmeInstallDir))
        return fmeInstallDir

    def getExecEnv(self, param, fmeVersion=None):
        '''
        assembles environment variables necessary to run this script on a
        separate process.

        :param fmeVersion: if this parameter is specified it must be the 4
                           digit year of the FME release that should be
                           used for the fmeobjects imports.  If not
                           specified then it will use whatever is
                           configured in the current environment variables.
                           If it is specified it will do a search for paths
                           with FME<4 digit year> and change them to the
                           specified <fmeVersion> ie FME<fmeVersion>

        '''
        libPath = os.path.join(os.path.dirname(__file__), 'lib')
        libPath = os.path.realpath(os.path.normpath(libPath))

        fmeRootDir = self.getFMEInstallPath(param, fmeVersion)

        fmePythonDir = os.path.join(fmeRootDir, 'fmepython27')
        fmePythonDir = os.path.realpath(os.path.normpath(fmePythonDir))

        fmeObjDir = os.path.join(fmeRootDir, 'fmeobjects', 'python27')
        fmeObjDir = os.path.realpath(os.path.normpath(fmeObjDir))

        self.logger.debug("fmeRootDir: {0}".format(fmeRootDir))
        self.logger.debug("fmePythonDir: {0}".format(fmePythonDir))
        self.logger.debug("fmeObjDir: {0}".format(fmeObjDir))

        # versionRegex = re.compile('^[a-zA-Z]{1}:.\\FME\d{4}.')
        self.logger.debug("syspath %s", sys.path)
        my_env = os.environ.copy()

        pythonPath = []
        pythonPath.append(libPath)
        pythonPath.append(fmeRootDir)
        pythonPath.append(fmePythonDir)
        pythonPath.append(fmeObjDir)

        # make sure all of sys.path is a regular string
        sysPathStrList = []
        sysPathStrList.append(libPath)

        # iterate through existing path list and swap the fme versions.
        for pth in sys.path:
            fixedPath = os.path.realpath(pth)
            fixedPath = os.path.normpath(fixedPath)
            if fmeVersion:
                searchString = r'\\FME\d{4}'
                fixedPath = re.sub(searchString, '\\FME{0}'.format(
                    fmeVersion), pth)
                fixedPath = os.path.normpath(fixedPath)
            if fixedPath not in sysPathStrList:
                sysPathStrList.append(str(fixedPath))
                self.logger.debug("adding path: {0}".format(fixedPath))
            # print '   ', fixedPath

        # insert the FME Paths to the start of the path string:
        paths2Add = [fmeRootDir, fmePythonDir, fmeObjDir]
        for path2Add in paths2Add:
            path2Add = os.path.realpath(path2Add)
            path2Add = os.path.normpath(path2Add)
            if path2Add not in sysPathStrList:
                sysPathStrList.insert(0, path2Add)
                self.logger.debug("adding path: {0}".format(path2Add))
        syspathString = ';'.join(sysPathStrList)
        self.logger.debug("syspath string %s, %s", syspathString,
                          type(syspathString))
        my_env['PATH'] = syspathString
        my_env['PYTHONPATH'] = ';'.join(pythonPath)

        self.logger.debug("myenv: %s", my_env)
        return my_env

    def getFeatureCountSeparateProcess(self, fmeVersion=None, fmeInstallPath=None):
        '''
        This method will spawn a separate subprocess calling this module
        using its command line interface.

        This functionality exists as the fme.exe crashes in the shutdown
        when using the ffs file reader (fmeobjects) is attempted.

        To work around this limitation this method will create a subprocess
        and execute the ffs file reader on that subprocess, capturing and
        parsing the output from the ffs file
        '''
        self.logger.debug("fmeVersion: %s", fmeVersion)
        param = DataBCFMWTemplate.TemplateConfigFileReader('DLV')
        pythonRootDir = self.getPythonInstallRootDirectory(param)
        pythonExe = os.path.join(pythonRootDir, 'python')
        self.logger.debug("executing this script in separate subprocess")
        # making sure we are calling the .py file and not the pyc file
        thisFileString = self.getThisFileAsPy()

        execEnv = self.getExecEnv(param, fmeVersion=fmeVersion)
        # self.getFMEInstallPath()
        fmeExecutable = os.path.join(self.fmeInstallPath, 'fme.exe')

        # commandList = [pythonExe, thisFileString, self.ffsFile]
        commandList = [fmeExecutable, 'python', thisFileString, self.ffsFile, self.fmeInstallPath]
        self.logger.debug("command being executed: %s", ' '.join(commandList))
        out = subprocess.check_output(commandList, env=execEnv)
        self.logger.debug("out is: %s", out)
        regexFfsFeatures = re.compile(r'{0}\s*\d+'.format(self.stdoutIDStr), re.IGNORECASE)
        for outLine in out.split('\n'):
            self.logger.debug('outline: %s', outLine)
            if regexFfsFeatures.match(outLine):
                features = outLine.replace(self.stdoutIDStr, '').strip()

        self.logger.debug("features: %s", features)
        if not isinstance(features, (int, long, float, complex)):
            features = int(features)

        return features

    def getFeatureCount(self, separateProcess=False):
        '''
        Uses FME Objects to read through the supplied FFS file and
        count the number of features in the file.

        This only starts with what is possible.  Can actually get all
        the attributes etc from the file, geometry etc.

        :var separateProcess: If this flag is set to true then the feature counting
                              will take place on a separate process than the process
                              that this method was called with.
        '''
        if separateProcess:
            feats = self.getFeatureCountSameProcess()
        else:
            feats = self.getFeatureCountSeparateProcess()
        return feats


if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage = 'FFSReader <ffs file to read> <fme install path>'
        raise ValueError, usage

    # TODO: Modify this to use the logging config file to retrieve the
    #       logger when called as its own script
    # logger = logging.getLogger(__name__)
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.DEBUG)

    ffsFile = sys.argv[1]
    installPath = sys.argv[2]
    rdr = Reader(ffsFile, installPath)
    feats = rdr.getFeatureCountSameProcess()
    # feats = rdr.getFeatureCount()
    outStr = rdr.stdoutTemplate.format(feats)
    print outStr

