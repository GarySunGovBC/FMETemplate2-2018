'''
Uses putty to build an SSH tunnel.

PID is cached in a temporary file.
Startup looks for the PID, as does the 
shutdown.

If the PID file exists at startup it gets 
read, get the PID from it.  Check to see if 
it exists.  If it does it is KILLED. 




@author: kjnether
'''
import os
import subprocess
import psutil
import logging
import time
import DataBCFMWTemplate

class SSHTunnel(object):
    
    def __init__(self, localPort, destHost, destPort, keyFile, username, pidfile, puttyPath ):
        
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger()
        self.logger.debug("SSH Module initiated")
        
        #ch = logging.StreamHandler()
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        #ch.setFormatter(formatter)
        #self.logger.addHandler(ch)
        
        # amount of time to sleep after initiating the creation of the 
        # ssh tunnel
        self.pauseTime = 10

        self.localPort = localPort
        self.destHost = destHost
        self.destPort = destPort
        self.keyFile = keyFile
        self.username = username
        self.pidFile = pidfile
        self.tunnelProcessName = puttyPath
        
        msg = "localport:{0}, desthost:{1}, destport:{2}, keyfile:{3}, username: {4}, pidfile:{5}"
        msg = msg.format(self.localPort, self.destHost, self.destPort, self.keyFile, self.username, self.pidFile)
        self.logger.info(msg)
        
        if not os.path.exists(self.tunnelProcessName):
            puttyDir, puttyFile = os.path.split(self.tunnelProcessName)
            templateDir = os.path.dirname(puttyDir)
            msg = 'expecting a putty.exe file to exist in the FME Framework ' + \
                  'root directory {0} called: {1}.  "putty.exe" file should be ' + \
                  'called {2}.  This file does not exist, please grab a copy ' + \
                  'from the internet and place in this directory (http://www.putty.org/) ' +\
                  'and then retry this run.'
            msg = msg.format(templateDir, puttyDir, puttyFile)
            self.logger.error(msg)
            raise ValueError, msg
        
        loginId = '{0}@{1}'.format(username, destHost)
        self.command = puttyPath
    
    def createTunnel(self):
        # first look for pid file
        self.killPid(MustExist=False)
        lFlag = '{0}:{1}:{2}'.format(self.localPort, self.destHost, self.destPort)
        loginId = '{0}@{1}'.format(self.username, self.destHost)
        tunnelCommand = [self.command, '-ssh', '-i', self.keyFile, '-L', lFlag, '-N', loginId]
        self.logger.debug("tunnelcommand: {0}".format(tunnelCommand))
        p = subprocess.Popen(tunnelCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        msg = "process id for the ssh tunnel: {0}"
        self.logger.debug(msg.format(p.pid))
        fh = open(self.pidFile, 'w')
        fh.write('{0}\n'.format(p.pid))
        fh.close()
        if not os.path.exists(self.pidFile):
            msg = 'The pid file {0} was not successfully created' + \
                  'The PID file MUST be created otherwise there will ' + \
                  'be no way to delete the tunnel'
            self.logger.error(msg.format(self.pidFile))
            raise IOError, msg
        self.logger.info("pausing for {0} seconds to allow the tunnel to finish its creation".format(self.pauseTime))
        time.sleep(self.pauseTime)
    
    def killPid(self, pid=None, MustExist=True):
        if not pid:
            if os.path.exists(self.pidFile):
                self.logger.info("retrieving the pid from the pid cache file: {0}".format(self.pidFile))
                fh = open(self.pidFile, 'r')
                pidLine = fh.readline()
                fh.close()
                pid = pidLine.strip()
        if not pid:
            msg = "The process cache file: {0} does not exist, and no process number was provided.  No processes have or will be killed"
            self.logger.error(msg.format(self.pidFile))
            if MustExist:
                raise IOError, msg.format(self.pidFile)
        if pid:
            completed = False
            self.logger.debug("checking to make sure the pid {0} is {1} process".format(pid, self.tunnelProcessName))
            tunnelProcessName = os.path.basename(self.tunnelProcessName)
            for p in psutil.process_iter():
                processName = p.name()
                if p.pid == int(pid):
                    self.logger.debug('found a process with a pid: {0}'.format(pid))
                    if processName.lower().strip() == tunnelProcessName:
                        self.logger.info("killing the {0} process (pid={1})".format(processName, p.pid))            
                        p.kill()
                        completed = True
                    else:
                        msg = "the process id is not attached to an {0} process"
                        msg = msg.format(self.tunnelProcessName)
                        self.logger.info(msg)
            if not completed:
                self.logger.debug('The pid was not found')
        if os.path.exists(self.pidFile):
            self.logger.info("removing the pid cache file {0}".format(self.pidFile))
            os.remove(self.pidFile)

class SSHTunnelHelper(object):
    '''
    this class is the bridge between the FMW published parameters
    and the building of the tunnel.  Very simple api
    '''
    
    def __init__(self, fme):
        self.fme = fme
        modDotClass = '{0}'.format(__name__)
        self.logger = logging.getLogger(modDotClass)
        
        self.logger.debug("macro values {0}".format(fme.macroValues))

        
        self.const= DataBCFMWTemplate.TemplateConstants()
        dbEnvKey = self.getDestDBEnvKey()
        self.logger.debug("dest db env key is: {0}".format(dbEnvKey))
        self.conf = DataBCFMWTemplate.TemplateConfigFileReader(dbEnvKey)
        fmwDir = self.getFMWDir()
        fmwName = self.getFMWFile()
        
        pidDirectory = self.conf.calcPIDCacheDirectory(fmwDir, fmwName)
        pidFile = os.path.normpath(os.path.join(pidDirectory, self.const.PIDFileName))
        
        self.logger.debug("pid cache file is: {0}".format(pidFile))
        
        # Retrieve the SSH Params
        localPort = self.getSSHLocalPort()
        destHost = self.getSSHDestinationHost()
        destPort = self.getSSHDestinationPort()
        keyFile = self.getSSHKeyFile()
        username = self.getSSHUserName()
        
        puttyCommand = self.conf.calcPuttyExecPath()
        # localPort, destHost, destPort, keyFile, username, pidfile ):

        self.ssh = SSHTunnel(localPort, destHost, destPort, keyFile, username, pidFile, puttyCommand)
        
    def createTunnel(self):
        self.ssh.createTunnel()
        
    def killTunnel(self):
        self.ssh.killPid()
        
    def getDestDBEnvKey(self):
        destdbEnvKey = self.getMacroParam(self.const.FMWParams_DestKey)
        return destdbEnvKey
                
    def getFMWDir(self):
        fmwDir = self.getMacroParam(self.const.FMWMacroKey_FMWDirectory)
        return fmwDir
    
    def getFMWFile(self):
        fmwFile = self.getMacroParam(self.const.FMWMacroKey_FMWName)
        return fmwFile
    
    def getSSHLocalPort(self):
        localPort = self.getMacroParam(self.const.FMWParams_SSHTunnel_LocalPort)
        return localPort
    
    def getSSHDestinationHost(self):
        destHost = self.getMacroParam(self.const.FMWParams_SSHTunnel_DestHost)
        return destHost
    
    def getSSHDestinationPort(self):
        destPort = self.getMacroParam(self.const.FMWParams_SSHTunnel_DestPort)
        return destPort
    
    def getSSHKeyFile(self):
        # this is the name of the key file to use to establish the ssh 
        # tunnel
        keyFile = self.getMacroParam(self.const.FMWParams_SSHTunnel_KeyFile)
        
        # now get the directory for the key file
        rootDir = self.conf.getTemplateRootDirectory()
        configDir = self.conf.getConfigDirName()
        keyFileFullPath = os.path.join(rootDir, configDir, keyFile)
        if not os.path.exists(keyFileFullPath):
            msg = 'the ssh key file specified: {0} does not exist in ' + \
                  'the expected directory {1}. Get the key that you want ' + \
                  'to use and re-locate it to this directory.'
            msg = msg.format(keyFile, os.path.dirname(keyFileFullPath))
            self.logger.error(msg)
            raise KeyError, msg
        return keyFileFullPath
    
    def getSSHUserName(self):
        userName = self.getMacroParam(self.const.FMWParams_SSHTunnel_HostUsername)
        return userName
    
    def getMacroParam(self, key):
        if key not in self.fme.macroValues:
            msg = 'Cannot create a SSH tunnel until you create and populate the parameter {0}'
            self.logger.error(msg.format(key))
            raise KeyError, msg.format(key)
        return self.fme.macroValues[key]
        
        
            
            
            
        
        
        
        
        
        
    
    
    
