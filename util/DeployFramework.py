'''
This script will do the deployments based on the deployment configuration
defined in the file:
  ../../config/FMEServerDeployment.json

'''
import os.path
import warnings
import logging.config
import sys
from FrameworkDeployment import usercustomize  # @UnusedImport
from FrameworkDeployment import DeployFrameworkLib
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
    configDir = os.path.join(os.path.dirname(__file__), '..',
                                   'config')
    logConfFilePath = os.path.join(configDir, logConfFile)

    outputLogFile = os.path.join(os.path.dirname(__file__), 'deploy.log')
    logging.config.fileConfig(logConfFilePath, defaults={
        'logfilename': str(outputLogFile)})

    logging.debug("testing first log message")

    # ---------- reading deployment config file  ----------
    deployConfig = os.path.join(configDir,
                                'FMEServerDeployment.json')

    # ------------------------------------------
    #   retrieving the args
    # ------------------------------------------
    env = None
    if len(sys.argv) > 1:
        env = sys.argv[1]

    #          START ACTUAL DEPLOYMENTS
    # ----------------------------------------------------
    # Custom Transformers, etc
    #  Copies the fme customizations
    # ----------------------------------------------------
    deployFmeCust = DeployFrameworkLib.FMECustomizationDeployments(
        deployConfig=deployConfig, env=env)
    deployFmeCust.copyCustomTransformers(overwrite=True)

    # ----------------------------------------------------
    # Configs
    #  logging config and other config file
    # ----------------------------------------------------
    deployConf = DeployFrameworkLib.ConfigsDeployment(
        deployConfig=deployConfig, env=env)
    deployConf.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # binary Deployment
    #   leave commented out as we don't use the binary stuff anymore
    # ----------------------------------------------------
    binDeploy = DeployFrameworkLib.BinaryDeployments(
        deployConfig=deployConfig, env=env)
    binDeploy.copyFiles()

    # ----------------------------------------------------
    # Python (framework)
    # copies the python libs that are in the framework root directory
    # and defined in the section frameworkPython
    # ----------------------------------------------------
    deployPy = DeployFrameworkLib.PythonFMEFramework(
        deployConfig=deployConfig, env=env)
    deployPy.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # Python Dependencies
    # copies the pythonDependencies section.  This is a section
    # that can be commented out as it is also deployed when you
    # deploy
    # ----------------------------------------------------
    pyDepDeploy = DeployFrameworkLib.PythonDependencies(
        deployConfig=deployConfig, env=env)
    pyDepDeploy.copyFiles(overwrite=False)
    # This deployment can stay commented out, provides the option
    # of deploying only the DBCpyLib modules used by the framework
    dbcPyLibDly = DeployFrameworkLib.DBCPyLibDependencies(
        deployConfig=deployConfig, env=env)
    dbcPyLibDly.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # Secrets deploy
    # ----------------------------------------------------
    deploySecrets = DeployFrameworkLib.SecretsDeployment(
        deployConfig=deployConfig, env=env)
    deploySecrets.copyFiles(overwrite=False)
