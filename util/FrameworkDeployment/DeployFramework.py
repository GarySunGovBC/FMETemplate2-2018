'''
This script will do the deployments based on the deployment configuration
defined in the file:
  ../../config/FMEServerDeployment.json

'''
import site
import os.path
import warnings
import logging.config
# curDir = os.path.dirname(__file__)
# curDir = os.path.abspath(curDir)
# print 'curDir', curDir
# site.addsitedir(curDir)
# # libdir
# libDir = os.path.join(curDir,
#                       '..',
#                       '..',
#                       'lib')
# libDir = os.path.normpath(libDir)
# print 'libDir', libDir
# site.addsitedir(libDir)
# site.addsitedir(os.path.dirname(libDir))

import util.FrameworkDeployment.DeployFrameworkLib as DeployFrameworkLib


# pylint: disable=no-self-use, invalid-name

filterMessage = 'Unverified HTTPS request is being made. Adding ' + \
                'certificate verification is strongly advised. ' + \
                'See: https://urllib3.readthedocs.io/en/latest/a' + \
                'dvanced-usage.html#ssl-warnings'
warnings.filterwarnings("ignore", message=filterMessage)

if __name__ == '__main__':
    # ------------------------------------------
    # ---------- Configuring Logging ----------
    # ------------------------------------------
    logConfFile = 'DeploymentLogging.config'
    logConfFilePath = os.path.join(os.path.dirname(__file__), logConfFile)

    outputLogFile = os.path.join(os.path.dirname(__file__), 'deploy.log')
    logging.config.fileConfig(logConfFilePath, defaults={
        'logfilename': str(outputLogFile)})

    logging.debug("testing first log message")

    # ---------- reading deployment config file  ----------
    deployConfig = os.path.join(os.path.dirname(__file__),
                                '..', '..', 'config',
                                'FMEServerDeployment.json')

    #          START ACTUAL DEPLOYMENTS
    # ----------------------------------------------------
    # Custom Transformers, etc
    #  Copies the fme customizations
    # ----------------------------------------------------
    deployFmeCust = DeployFrameworkLib.FMECustomizationDeployments(deployConfig=deployConfig)
    deployFmeCust.copyCustomTransformers(overwrite=True)

    # ----------------------------------------------------
    # Configs
    #  logging config and other config file
    # ----------------------------------------------------
    deployConf = DeployFrameworkLib.ConfigsDeployment()
    deployConf.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # binary Deployment
    #   leave commented out as we don't use the binary stuff anymore
    # ----------------------------------------------------
    binDeploy = DeployFrameworkLib.BinaryDeployments()
    binDeploy.copyFiles()

    # ----------------------------------------------------
    # Python (framework)
    # copies the python libs that are in the framework root directory
    # and defined in the section frameworkPython
    # ----------------------------------------------------
    deployPy = DeployFrameworkLib.PythonFMEFramework()
    deployPy.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # Python Dependencies
    # copies the pythonDependencies section.  This is a section
    # that can be commented out as it is also deployed when you
    # deploy
    # ----------------------------------------------------
    pyDepDeploy = DeployFrameworkLib.PythonDependencies()
    pyDepDeploy.copyFiles(overwrite=False)
    # This deployment can stay commented out, provides the option
    # of deploying only the DBCpyLib modules used by the framework
    dbcPyLibDly = DeployFrameworkLib.DBCPyLibDependencies()
    dbcPyLibDly.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # Secrets deploy
    # ----------------------------------------------------
    deploySecrets = DeployFrameworkLib.SecretsDeployment()
    deploySecrets.copyFiles(overwrite=False)
