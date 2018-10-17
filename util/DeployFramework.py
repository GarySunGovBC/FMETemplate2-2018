'''
This script will do the deployments based on the deployment configuration
defined in the file:
  ../../config/FMEServerDeployment.json

'''
import argparse
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
    import argparse
    validSections = ['all', 'config', 'dbcdeps', 'bins', 'py', 'pydep',
                     'secrets', 'fmecust']
    parser = argparse.ArgumentParser()
    envHelp = 'Key word that describes the destination fme server to deploy to'
    parser.add_argument('--env', help=envHelp, choices=['fme_tst', 'fme_prd'])
    destHelp = 'What exactly do you want to deploy, options include: {0}'
    destHelp = destHelp.format(destHelp)
    parser.add_argument('--dest', help=destHelp, choices=validSections)
    args = parser.parse_args()
    if args.dest:
        msg = "using the destination key sent as an arg: %s", args.dest
        logging.debug(msg)
        section = args.dest
    else:
        section = 'all'

    if args.env:
        msg = "the destination keyword is: %s", args.env
        logging.debug(msg)
        env = args.env

    # evaluate args:
    # acceptable section values

    #          START ACTUAL DEPLOYMENTS
    # ----------------------------------------------------
    # Custom Transformers, etc
    #  Copies the fme customizations
    # ----------------------------------------------------
    if section == 'fmecust' or section == 'all':
        deployFmeCust = DeployFrameworkLib.FMECustomizationDeployments(
            deployConfig=deployConfig, env=env)
        deployFmeCust.copyCustomTransformers(overwrite=True)

    # ----------------------------------------------------
    # Configs
    #  logging config and other config file
    # ----------------------------------------------------
    if section == 'config' or section == 'all':
        deployConf = DeployFrameworkLib.ConfigsDeployment(
            deployConfig=deployConfig, env=env)
        deployConf.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # binary Deployment
    #   leave commented out as we don't use the binary stuff anymore
    # ----------------------------------------------------
    if section == 'bins' or section == 'all':
        binDeploy = DeployFrameworkLib.BinaryDeployments(
            deployConfig=deployConfig, env=env)
        binDeploy.copyFiles()

    # ----------------------------------------------------
    # Python (framework)
    # copies the python libs that are in the framework root directory
    # and defined in the section frameworkPython
    # ----------------------------------------------------
    if section == 'py' or section == 'all':
        deployPy = DeployFrameworkLib.PythonFMEFramework(
            deployConfig=deployConfig, env=env)
        deployPy.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # Python Dependencies
    # copies the pythonDependencies section.  This is a section
    # that can be commented out as it is also deployed when you
    # deploy
    # ----------------------------------------------------
    if section == 'pydep' or section == 'all':
        pyDepDeploy = DeployFrameworkLib.PythonDependencies(
            deployConfig=deployConfig, env=env)
        pyDepDeploy.copyFiles(overwrite=False)

    # This deployment does a subset of the previous one 'pydep'  it will
    # deploy the dependencies that make up the dbcpylib.  Not called
    # when all is specified as everything that is deployed here gets
    # deployed also by the 'pydep' section
    # of deploying only the DBCpyLib modules used by the framework
    if section == 'dbcpydep':
        dbcPyLibDly = DeployFrameworkLib.DBCPyLibDependencies(
            deployConfig=deployConfig, env=env)
        dbcPyLibDly.copyFiles(overwrite=False)

    # ----------------------------------------------------
    # Secrets deploy
    # ----------------------------------------------------
    if section == 'secrets' or section == 'all':
        deploySecrets = DeployFrameworkLib.SecretsDeployment(
            deployConfig=deployConfig, env=env)
        deploySecrets.copyFiles(overwrite=False)
