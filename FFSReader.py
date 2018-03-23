'''
Created on Mar 22, 2018

@author: kjnether
'''
import os.path
import logging
import sys
try:
    import fmeobjects  # @UnresolvedImport
except:
    pathList = os.environ['PATH'].split(';')
    sys.path.insert(0, r'E:\sw_nt\FME2015\fmeobjects\python27')
    os.environ['PATH'] = ';'.join(pathList)
    import fmeobjects  # @UnresolvedImport


class Reader(object):
    '''
    Place to put ffs file reader functionality
    
    Very simple at the moment but can anticipate as we move towards autmated
    error reporting we will be in a position to add more functionality 
    that reports on specific features etc. 
    '''

    def __init__(self, ffsFile):
        self.ffsFile = ffsFile
        self.logger = logging.getLogger(__name__)

    def getFeatureCount(self):
        '''
        Uses FME Objects to read through the supplied FFS file and
        count the number of features in the file.

        This only starts with what is possible.  Can actually get all
        the attributes etc from the file, geometry etc.
        '''
        reader = fmeobjects.FMEUniversalReader('FFS', False, [])
        reader.open(self.ffsFile, [])

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
        return featureCnt
