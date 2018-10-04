'''
This class performs the schema mapping required by Kirk

Fieldmap information currently comes from two different locations:
  - counter transformers (require the count field to be renamed)
  - field maps (defined in the kirk api)

Counter:
  You cannot dynamically define the output attribute for counter
  transformers.  Kirk cludges this by alway naming the counter
  transformer attribute as: KIRK_COUNTERATTRIBUTE.

  This class will then identify if that attribute exists, and it it
  does it gets renamed to the field defined in the kirk api for the
  the output counter attribute.

FieldMaps:
   Fieldmaps are defined for individual jobs in Kirk.
   this class will detect if these types of field maps exist.  If they do
   they are retrieved from the kirk api, and implemented.

The output attributes for the transformer that implements this class will
reflect any attribute renaming that is defined in Kirk.

Created on Oct 3, 2018

@author: kjnether
'''
import fmeobjects
import logging
import DBCFMEConstants
import KIRKParams


class FeatureProcessor(object):
    '''
    This class completes any attribute renaming that needs to be
    implemented, attribute renaming parameters all come from Kirk api.
    '''

    def __init__(self, fme):
        self.fme = fme
        self.tmpltConstants = DBCFMEConstants.TemplateConstants()
        self.logger = logging.getLogger(__name__)
        self.attribNames = None
        self.isFirstFeature = True
        self.kirkFldMap = None
        self.kirkCounterTmpAttrib = \
            self.tmpltConstants.FMWParams_KirkCounterTmpAttribute
        self.logger.info("kirk attribute renamer is initialized")

    def input(self, feature):
        '''
        This method is automatically called for every
        feature that goes througth the pythoncaller transformer.

        The method remaps the attributes based on the counter published
        parameter values, and whether a field map is defined in kirk
        '''
        self.populateAttribNames(feature)
        # remapping counter attribute if it exists.
        if self.attributeExists(self.kirkCounterTmpAttrib):
            feature = self.remapCounterAttribute(feature)

        # remapping kirk field names
        kirkMappedFieldsCountAttribute = \
            self.tmpltConstants.FMWParams_KirkFldMapCnt
        if (kirkMappedFieldsCountAttribute in self.fme.macroValues) and \
                self.fme.macroValues[kirkMappedFieldsCountAttribute]:
            feature = self.mapFields(feature)
        self.isFirstFeature = False
        self.pyoutput(feature)

    def mapFields(self, feature):
        '''
        implements the kirk field map on the input feature

        :param feature: the feature that should have the field map applied
                        to
        :type feature: fmeobjects.FMEFeature
        :return: the same feature after the fieldmap was applied to it.
        '''
        fldMapDict = self.getKirkFieldMapDict()
        for srcField in fldMapDict:
            destField = fldMapDict[srcField]
            value = feature.getAttribute(srcField)
            feature.setAttribute(destField, value)
            # print 'new attribute name:', newName
            feature.removeAttribute(srcField)
            if self.isFirstFeature:
                msg = 'srcfld: {0} destfld: {1}'.format(srcField, destField)
                self.logger.debug(msg)
        return feature

    def getKirkFieldMapDict(self):
        '''
        If the field map doesn't already exist in memory retrieves it and
        caches in self.kirkFldMap
        :return: the field map as a dictionary where the key is the source
                 field and the value is the destination
        :return: dict
        '''
        if self.kirkFldMap is None:
            self.logger.debug("retrieving the field map from the kirk api")
            kirkParams = KIRKParams.KIRKParams(self.fme.macroValues)
            fldMap = kirkParams.getFieldMap()
            self.kirkFldMap = fldMap.getFieldMapAsDict()
        return self.kirkFldMap

    def attributeExists(self, attribute2Test):
        '''
        tests to see if the published parameter described in the input
        parameter attribute2Test exists or not
        :param attribute2Test: the name of the attribute who's existence is
                                to be ascertained
        :type attribute2Test: bool
        '''
        retVal = False
        if attribute2Test in self.attribNames:
            retVal = True
        return retVal

    def populateAttribNames(self, feature):
        '''
        reads the input feature and extracts the attribute names that are
        definied for it
        :param feature: the input feature
        :type feature: fmeobjects.FMEFeature
        '''
        if self.attribNames is None:
            self.attribNames = feature.getAllAttributeNames()

    def remapCounterAttribute(self, feature):
        '''
        Counter transformers do not support the ability to dynamically define
        the output attribute.  To overcome this issue kirk counters always
        output the attribute KIRK_TMP_COUNTER_ATTRIBUTE with the counter
        value.

        The pythoncaller transformer that this class controls then calls this
        method that will determine if the tmp attribute exists.  If it does
        it gets renamed to the value contained in the attribute:
        KIRK_COUNTERATTRIBUTE

        :param feature: the input feature to apply the renaming to
        :type feature: fmeobjects.FMEFeature
        '''
        # the published parameter with the name of the temporary attribute
        pubparamWithCounterName = \
            self.tmpltConstants.FMWParams_KirkCounterAttribute
        # now get the actual attribute name
        outputCounterAttributeName = \
            self.fme.macroValues[pubparamWithCounterName]
        # the value associated with the current tmp attribute that is
        # statically defined in the counter.  (based on framework standard
        # nameing, expecting it to be called: KIRK_TMP_COUNTER_ATTRIBUTE
        value = feature.getAttribute(self.kirkCounterTmpAttrib)
        feature.setAttribute(outputCounterAttributeName, value)
        feature.removeAttribute(self.kirkCounterTmpAttrib)
        if self.isFirstFeature:
            msg = "pub param name: {0} and actual new column name: {1}"
            msg = msg.format(pubparamWithCounterName,
                             outputCounterAttributeName)
            self.logger.debug(msg)

            msg = 'remapping the temporary field to {0} to {1}'
            msg = msg.format(self.kirkCounterTmpAttrib,
                             outputCounterAttributeName)
            self.logger.info(msg)
        return feature

    def close(self):
        pass
