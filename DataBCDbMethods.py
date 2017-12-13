'''
Created on Dec 11, 2017

@author: kjnether
'''

import logging
import re

import DB.DbLib


class DataBCDbMethods(object):

    '''
    :ivar fmeObj: This is the fme object that gets created in a workspace and
                  is described in the help under FME_END_PYTHON.  properties
                  include: cpuTime, cpuUserTime, elapsedRunTime, etc...
    :ivar const: a constants object with a bunch of variables used by various
                 aspects of the framework. TemplateConstants()
    :ivar params: CalcParamsBase() Type, provides an interfact to the published
                  parameters in the fmw.
    :ivar config:  TemplateConfigFileReader() type, an interface to properties
                  stored in the framework config file.
    '''

    def __init__(self, fmeObj, const, params, config):
        self.logger = logging.getLogger(__name__)
        self.fme = fmeObj
        self.const = const
        self.config = config
        self.params = params

    def analyzeDestinationFeatures(self):
        '''
        This is the method that will be called in the shutdown, to analyze any
        destination datasets.
        '''

        destinations = self.getDestinations()
        for destination in destinations:
            destinationTable = destination.getDestinationFeature()
            destinationSchema = destination.getDestinationSchema()
            destinationSchemaPosition = destination.getDestinationSchemaPosition()
            destinationPswd = self.params.getDestinationPassword(destinationSchemaPosition)
            
            if not destinationPswd:
                destKey = self.params.getDestDatabaseEnvKey()
                msg = 'unable to retrieve a password for the schema: {0}' + \
                      'and the destination key word: {1}'
                msg = msg.format(destinationSchema, destKey)
                self.logger.error(msg)
                raise ValueError, msg
            
            host = self.params.getDestinationHost()
            serviceName = self.params.getDestinationServiceName()
            port = self.params.getDestinationOraclePort()
            msg = 'host: {0} sn: {1} port: {2}'
            msg = msg.format(host, serviceName, port)
            self.logger.debug(msg)
            
            db = DB.DbLib.DbMethods()
            self.logger.debug("created  a db obj")

            #  user, pswd, instance, server, port=1521):
            db.connectNoDSN(destinationSchema,
                            destinationPswd,
                            serviceName,
                            host,
                            port)
            # schema, objType, objName, connObj=None):  #pylint: disable=too-many-branches

            if db.objExists(destinationSchema, 'TABLE', destinationTable):
                self.logger.debug("created a connection")
                args = [destinationSchema, destinationTable]
                self.logger.debug("args going to analyze are: %s", args)
                cur = db.executeProcedure('dbms_stats.gather_table_stats', args)
                db.commit()
                cur.close()
                # exec dbms_stats.gather_table_stats('schema', 'table')
                # exec dbms_stats.gather_table_stats('WHSE_SAT','BILLTHECAT');
                msg = 'Analyze is now complete on: {0}.{1}'.format(
                    destinationSchema, destinationTable)
                self.logger.info(msg)
            else:
                msg = 'Unable to analyze the dataset: %s.%s as the destination ' + \
                      'is a View'
                msg = msg.format(destinationSchema, destinationTable)
                self.logger.warning(msg)

    def getDestinations(self):
        '''
        iterates over all the published parameters that are defined in the fmw
        looking for destination parameters.  These are returned in a datastructure
        that describes:

        - destination table
        - destination schema
        - parameter that contains the destination table
        - parameter that contains the destination schema
        - destination schema number
        - destination feature number

        '''
        matchExpr = '^{0}.*$'
        destFeatures = []
        fmeMacros = self.fme.macroValues
        matchExpr = matchExpr.format(self.const.FMWParams_DestFeatPrefix)
        for macroKey in fmeMacros.keys():
            if re.match(matchExpr, macroKey, re.IGNORECASE):
                destFeatMacroKey = macroKey
                destFeat = fmeMacros[destFeatMacroKey]
                # get number?  ie, DEST_FEATURE_1 would get 1
                destFeaturePostion = macroKey[len(self.const.FMWParams_DestFeatPrefix):]
                if not destFeaturePostion.isdigit():
                    msg = 'Trying to extract the number from the published ' + \
                          'parameter {0}.  Extracted the number {1} but it ' + \
                          'is not a number'
                    msg = msg.format(macroKey, destFeaturePostion)
                    raise ValueError, msg
                # now get the schema for that number
                destFeaturePostion = int(destFeaturePostion)
                defaultSchema = self.params.getDestinationSchema()
                msg = "destination position is: {0}".format(destFeaturePostion)
                self.logger.debug(msg)
                if self.params.existsDestinationSchema(destFeaturePostion):
                    destSchema = self.params.getDestinationSchema(destFeaturePostion)
                    schemaMacroProperty = self.params.getMacroKeyForPosition(
                        self.const.FMWParams_DestSchema, destFeaturePostion)
                    schemaPosition = destFeaturePostion
                    # destFeat = '{0}.{1}'.format(positionalSchema, destFeat)
                else:
                    if not defaultSchema:
                        msg = 'There does not appear to be a published parameter ' + \
                              'That defines the destination schema in this fmw.'
                        raise ValueError, msg
                    destSchema = defaultSchema
                    schemaPosition = None
                    schemaMacroProperty = self.params.getMacroKeyForPosition(
                        self.const.FMWParams_DestSchema, schemaPosition)

                destFeatObj = DestFeatures(destFeat,
                                           destFeatMacroKey,
                                           destFeaturePostion,
                                           destSchema,
                                           schemaMacroProperty,
                                           schemaPosition)
                destFeatures.append(destFeatObj)
        return destFeatures


class DestFeatures(object):
    '''
    Simple class providing an easy way to store and retrieve the
    destination feature information extracted from the FMW

    :ivar destFeature: The destination feature/table
    :ivar destFeatureMacroKey: the published parameter in the fmw that contains
                               this destination feature
    :ivar destFeaturePosition: The position used to distinguish between multiple
                               destinations.  Example for DEST_FEATURE_1 this
                               parameter would be equal to 1
    :ivar destSchema: The schema that feature is expected to be within.
    :ivar destSchemaMacroKey: The published parameter that contains the destination
                              schema.
    :ivar destSchemaPosition: The position of the destination schema.
    '''

    def __init__(self, destFeature, destFeatureMacroKey, destFeaturePosition,
                 destSchema, destSchemaMacroKey, destSchemaPosition):
        self.destFeature = destFeature
        self.destFeatureMacroKey = destFeatureMacroKey
        self.destFeaturePosition = destFeaturePosition
        self.destSchema = destSchema
        self.destSchemaMacroKey = destSchemaMacroKey
        self.destSchemaPosition = destSchemaPosition

    def getDestinationFeature(self):
        '''
        :return: the destination table/feature
        '''
        return self.destFeature

    def getDestinationFeatureMacroKey(self):
        '''
        :return: the published parameter from the fmw that contains destination
                 table / feature.
        '''
        return self.destFeatureMacroKey

    def getDestinationFeaturePosition(self):
        '''
        :return: the destination feature position, when an fmw writes to multiple
                 tables they destination features are numbered.  This returns the
                 number of the destination feature.
        '''
        return self.destFeaturePosition

    def getDestinationSchema(self):
        '''
        :return: the schema that contains the destination feature / table.
        '''
        return self.destSchema

    def getDestinationSchemaMacroKey(self):
        '''
        :return: the published parameter that contains the destination schema
        '''
        return self.destSchemaMacroKey

    def getDestinationSchemaPosition(self):
        '''
        :return: the schema position, for when there are multiple destination
                 schemas, example DEST_SCHEMA_4, this parameter would be equal to
                 4
        '''
        return self.destSchemaPosition

    def getAsList(self):
        '''
        :return: a list with the following values in it, in this order:
          - destination feature
          - destination feature published parameter name
          - destination position
          - destination schema
          - destination schema published parameter name
          - destination schema position
        :rtype: list

        Mostly used for testing but available for use!
        '''
        retVal = [self.destFeature,
                  self.destFeatureMacroKey,
                  self.destFeaturePosition,
                  self.destSchema,
                  self.destSchemaMacroKey,
                  self.destSchemaPosition]
        return retVal
