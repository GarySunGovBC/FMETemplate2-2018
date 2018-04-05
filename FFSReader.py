'''
Created on Mar 22, 2018

@author: kjnether
'''
import os.path
import logging
import sys
import tempfile
import shutil
import subprocess
import re
import DataBCFMWTemplate
logger = logging.getLogger(__name__)

def attemptFMEObjectsImport():
    global fmeobjects
    try:
        logger.debug("Attempt 1: importing the fmeobjects module")
        import fmeobjects  # @UnresolvedImport
        logger.debug("Attempt 1: successfully imported the fmeobjects module")
    except:
        # once move to production can extract the fme path from the 
        # macro value FME_HOME and pass those parameters to this script
        #
        
        pathList = os.environ['PATH'].split(';')
        sys.path.insert(0, r'E:\sw_nt\FME2015\fmeobjects\python27')
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

    def __init__(self, ffsFile, retry=False):
        self.retry = retry
        self.ffsFile = ffsFile
        self.stdoutIDStr = 'ffsFeatures:'
        self.stdoutTemplate = self.stdoutIDStr + ' {0}'
        self.logger = logging.getLogger(__name__)
        # double check that the file exists
        if not os.path.exists(self.ffsFile):
            msg = 'you specified an FFS file: {0} that does not exist'
            msg = msg.format(self.ffsFile)
            self.logger.error(msg)
        #attemptFMEObjectsImport()
        
    def getFeatureCountSameProcess(self):
        '''
        bascially normal operation, just calls this method, it creates an 
        ffs file reader counts the features and returns them
        :return: The number of features found in the FFS file.
        :rtype: int
        '''
        tempDir = os.path.dirname(self.ffsFile)
        #ffsFile = tempfile.NamedTemporaryFile(dir=tempDir, delete=False)
        ffsFile = tempfile.mktemp(suffix='.ffs', dir=tempDir, prefix='tmp_')
        self.logger.debug("tempfile name: %s", ffsFile)
        shutil.copyfile(self.ffsFile, ffsFile)
        self.logger.debug("copied %s to %s", self.ffsFile, ffsFile)
        attemptFMEObjectsImport()
        try:
            self.logger.debug("starting to open and read the ffs file: %s", ffsFile)
            reader = fmeobjects.FMEUniversalReader('FFS', False, [])
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
    
    def getFeatureCountSeparateProcess(self):
        '''
        This method will spawn a separate subprocess calling this module using its
        command line interface.
        
        This functionality exists as the fme.exe crashes in the shutdown when 
        using the ffs file reader (fmeobjects) is attempted.
        
        To work around this limitation this method will create a subprocess and 
        execute the ffs file reader on that subprocess, capturing and parsing the 
        output from the ffs file
        
        '''
        # parameters needed to do this:
        #   ffs file path
        #   python exe path
        #   path to this module __file__
        #
        #TODO: replace this with a method call to calculate the path
        #pythonexe = 'E:\sw_nt\python27\ArcGIS10.2'
        # -----------------------------------------------------------------
        try:
            # this will get the arcpy paths, and add them to the sys.path
            # parameter which should then allow for use of arcpy using the
            # fme python default interpreter
            pyPaths = DataBCFMWTemplate.InstallPaths.PythonInstallPaths()
            pythonRootDir = pyPaths.getInstallDir()
            self.logger.debug("pythonpath: %s", pythonRootDir)
        except WindowsError, e:
            #
            self.logger.exception(e)
            msg = "was unable to pull the arc install from the registry.  trying " + \
                  'to guess what the install location is before failing.'
            self.logger.warning(msg)
            param = DataBCFMWTemplate.TemplateConfigFileReader('DLV')
            pythonRootDir = param.getPythonRootDir()
        except:
            raise
        my_env = os.environ.copy()
        pythonExe = os.path.join(pythonRootDir, 'python')
        self.logger.debug("executing this script in separate subprocess")
        commandList = [pythonExe, __file__, self.ffsFile]
        self.logger.debug("command being executed: %s", ' '.join(commandList))
        out = subprocess.check_output(commandList, env=my_env)
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
#         tempDir = os.path.dirname(self.ffsFile)
#         #ffsFile = tempfile.NamedTemporaryFile(dir=tempDir, delete=False)
#         ffsFile = tempfile.mktemp(suffix='.ffs', dir=tempDir, prefix='tmp_')
#         self.logger.debug("tempfile name: %s", ffsFile)
#         shutil.copyfile(self.ffsFile, ffsFile)
#         self.logger.debug("copied %s to %s", self.ffsFile, ffsFile)
#         try:
#             attemptFMEObjectsImport()
#             self.logger.debug("starting to open and read the ffs file: %s", ffsFile)
#             reader = fmeobjects.FMEUniversalReader('FFS', False, [])
#             self.logger.debug("created the reader... now try reading")
#             # reader.open(self.ffsFile, [])
#             reader.open(ffsFile, [])
# 
#             # Read all the features on the dataset.
#             feature = reader.read()
#             # atribNames = feature.getAllAttributeNames()
#             # self.logger.debug( 'atribNames: %s', atribNames)
#             featureCnt = 0
#             while feature != None:
#                 featureCnt += 1
#                 # Just log each feature.
#                 # print 'feature: %s', feature
#                 feature = reader.read()
#             self.logger.info('total features: %s', featureCnt)
#             # Close the reader before leaving.
#             reader.close()
#         finally:
#             if os.path.exists(ffsFile):
#                 self.logger.debug("cleaning up the temp file: %s", ffsFile)
#                 os.remove(ffsFile)
#         return featureCnt
    
if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage = 'FFSReader <ffs file to read>'
        raise ValueError, usage
    
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    
    ffsFile = sys.argv[1]
    rdr = Reader(ffsFile)
    feats = rdr.getFeatureCountSeparateProcess()
    #feats = rdr.getFeatureCount()
    outStr = rdr.stdoutTemplate.format(feats)
    print outStr
    
