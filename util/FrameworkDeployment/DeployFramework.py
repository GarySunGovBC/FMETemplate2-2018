'''
This script will do the deployments based on the deployment configuration
defined in the file:
  ../../config/FMEServerDeployment.json

'''

import os.path
import logging.config
import DeployFrameworkLib
import warnings

warnings.filterwarnings("ignore", message='Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings')

if __name__ == '__main__':
    # ---------- configuring logging ----------
    logConfFile = 'DeploymentLogging.config'
    logConfFilePath = os.path.join(os.path.dirname(__file__), logConfFile)

    outputLogFile = os.path.join(os.path.dirname(__file__), 'deploy.log')
    logging.config.fileConfig(logConfFilePath, defaults={
        'logfilename': str(outputLogFile)})

    logging.debug("testing first log message")
    # configure deployment

    # ---------- deployment config file ----------
    deployConfig = os.path.join(os.path.dirname(__file__),
                                '..', '..', 'config',
                                'FMEServerDeployment.json')

    # deployPy = DeployFrameworkLib.PythonDeployment(deployConfig=deployConfig)
    # deployPy.copyPythonFiles(overwrite=False)
    # deployPy.copyPythonDependencies(overwrite=False)
    # deployPy.copyDataBCModules(overwrite=False)
    
    # leave commented out as we don't use the binary stuff anymore
    #deployBin = DeployFrameworkLib.BinaryDeployments(deployConfig=deployConfig)
    #deployBin.copyBinaries(overwrite=False)
    
    #deployFmeCust = DeployFrameworkLib.FMECustomizationDeployments(deployConfig=deployConfig)
    #deployFmeCust.copyCustomTransformers(overwrite=True)

    deployConf = DeployFrameworkLib.ConfigsDeployment()
    deployConf.copyConfig(overwrite=False)
