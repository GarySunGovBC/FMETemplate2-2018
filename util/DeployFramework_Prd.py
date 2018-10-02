'''
Created on Nov 6, 2017

@author: kjnether

Mostly inheriting everything from the DeployFramework, and overriding parameters
to allow for deployment to a different server

monkey patching mostly !

Should consider re-write to allow for better extensibility
'''
import DeployFramework
import FMEUtil.PyFMEServerV2 as PyFMEServer
import logging
import os.path
import posixpath
import shutil
import ConfigParser

OriginalDeployment = DeployFramework.Deployment

class Params(DeployFramework.Params):
    fmeServerUrl = 'http://prodetlreplication.dmz'
    fmeServerAuthKey = 'apikeyhere'
    #fmeServerRepository = "TemplateV2_TEST"
# to use this use similar approach as was used for PythonDeployment, etc
# class Deploy(DeployFramework.Deploy):
#
#     def __init__(self):
#         DeployFramework.Deploy.__init__(self)

class PythonDeployment(DeployFramework.PythonDeployment):
    '''
    The python deployment class.  Used to deploy the python modules that make
    up the fme framework
    '''
    def __init__(self):
        DeployFramework.Deployment = Deployment
        DeployFramework.PythonDeployment.__init__(self)

class ConfigsDeployment(DeployFramework.ConfigsDeployment):
    def __init__(self):
        self.configFileToUpdate = 'templateDefaults.config'
        DeployFramework.Deployment = Deployment
        DeployFramework.ConfigsDeployment.__init__(self)

    def deployServerSpecific(self):
        '''
        Going to redeploy the config file, but before it does that it
        will patch the contents of it so that the fmeserver section
        of the config file is updated for the machine that we are
        deploying to:

        affected sections:

        [fmeserver]
        host=http://servername
        rootdir=/fmerest/v2/
        token=apikey
        '''
        params = Params()

        # define the tmpFile which is the modified version of the
        # config file for the server that this script is set up for.
        tmpDir = os.path.join(os.path.dirname(__file__), 'tmp')
        tmpFile = os.path.join(os.path.dirname(__file__), 'tmp', self.configFileToUpdate)
        if os.path.exists(tmpDir):
            # os.remove(tmpFile)
            shutil.rmtree(tmpDir)
        os.mkdir(tmpDir)
        self.logger.debug("temp dir now exists: %s", tmpDir)

        # Create the actual temp config file by editing the current
        # config file but rewrite the fmeserver params
        srcDir = os.path.join(self.params.templateSourceDir, self.srcDir)
        srcFile = os.path.join(srcDir, self.configFileToUpdate)
        self.logger.debug("source file: %s", srcFile)
        conf = ConfigParser.ConfigParser()
        conf.read(srcFile)
        conf.remove_section('fmeserver')
        # values = conf.options('fmeserver')
        conf.add_section('fmeserver')
        conf.set('fmeserver', 'host', params.fmeServerUrl)
        conf.set('fmeserver', 'rootdir', 'fmerest/v2/')
        conf.set('fmeserver', 'token', params.fmeServerAuthKey)
        fh = open(tmpFile, 'w')
        conf.write(fh)
        fh.close()
        self.logger.debug("temp the config file is now updated: %s", tmpFile)


        # Write the new file up to fme server'
        self.logger.debug("uploading %s to fme server", tmpFile)
        self.logger.debug("attempting to write to fme server")
        fmeServerDir = posixpath.join('config', self.configFileToUpdate)
        self.logger.debug("fmeserverDir: %s", fmeServerDir)
        
        destDir = posixpath.join(self.templateDestDir, 'config')
        destFile = posixpath.join(destDir, self.configFileToUpdate)
        destFile = posixpath.normpath(destFile)
        self.logger.debug("destFile: %s", destFile)
        # src = os.path.join(self.params.templateSourceDir, self.configFileToUpdate)
        deployment = Deployment(self.params.fmeServerResourcesEngineDirType,
                                tmpFile,
                                destDirStr=destFile)
        deployment.deploy()
        self.logger.debug("The file should now be updated.")

        # finally cleanup and delete the temp file

        # easiest way is to create a temp junk directory
        # save the modified file there
        # then copy it up, that way can name it the same thing
        # self.fmeServ = PyFMEServer.FMEServer(params.fmeServerUrl, params.fmeServerAuthKey)
        # self.resource = self.fmeServ.getResources()
        # self.resource.copyFile(params.fmeServerResourcesEngineDirType,
        #                       self.destDirList,
        #                       self.srcFile,
        #                       overwrite=True)

        # finally upload it and then delete it.


class FMECustomizationDeployments(DeployFramework.FMECustomizationDeployments):
    def __init__(self):
        DeployFramework.Deployment = Deployment
        DeployFramework.FMECustomizationDeployments.__init__(self)

class BinaryDeployments(DeployFramework.BinaryDeployments):
    def __init__(self):
        DeployFramework.Deployment = Deployment
        DeployFramework.BinaryDeployments.__init__(self)

class Deployment(OriginalDeployment):

    def __init__(self, *args, **kw):
        DeployFramework.PyFMEServer = PyFMEServer
        DeployFramework.Params = Params
        # DeployFramework.Deployment.__init__(self, dirType, srcFile, destDirStr, destDirList)
        super(DeployFramework.Deployment, self).__init__(*args, **kw)

if __name__ == '__main__':
    DeployFramework.ConfigLogging()

    deployPy = PythonDeployment()
    deployPy.copyPythonFiles(overwrite=True)
    deployPy.copyPythonDependencies(overwrite=True)

    deployConfig = ConfigsDeployment()
    deployConfig.copyConfig(overwrite=True)
    deployConfig.deployServerSpecific()

    deployBin = BinaryDeployments()
    deployBin.copyBinaries(overwrite=True)

    deployFmeCust = FMECustomizationDeployments()
    deployFmeCust.copyCustomTransformers(overwrite=True)
