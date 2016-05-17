'''
Created on May 9, 2016


idea is this script can be run to take any changes that have been 
implemented in the template code and deploy it to server.

@author: kjnether
'''

import os.path
import posixpath
import FMEUtil.PyFMEServerV2

class Params(object):
    
    FmeServerUrl = 'http://fenite.dmz'
    FmeServerAuthKey = '93978b32c457984ac4bb1912ed27e7fcade6e3ec'
    FmeServerRepository = 'GuyLaFleur'
    SourceDirectory = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\templateImplementation\BCGW_REP_SCHEDULED\acdf_ownership_codes_staging_csv_bcgw'
    pythonDirs = ["Plugins", 'python', 'python27']
    
class Deploy(object):
    
    def __init__(self):
        pass
    
class FMWDeployment(object):
    '''
    Methods to copy FMW's to FME server.
    '''
    def __init__(self):
        self.params = Params()
        self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
    
    def CopyFMW(self, fmwFile, repoName):
        fmwPath, fmw = os.path.split(fmwFile)
        if not os.path.exists(fmwFile):
            msg = 'The fmw file {0} that you are trying to upload to FME Server does not exist'
            msg = msg.format(fmwFile)
            raise ValueError, msg
        repo = self.fmeServ.getRepository()
        wrkspcs = repo.getWorkspaces(repoName)
        if wrkspcs.exists(fmw):
            repo.updateRepository(repoName, fmwFile)
            wrkspcs.registerWithJobSubmitter(fmw)
        else:
            repo.copy2Repository(repoName, fmwFile)
            wrkspcs.registerWithJobSubmitter(fmw)

class PythonDeployment(object):
    
    def __init__(self):
        self.params = Params()
        self.engineResDir = 'FME_SHAREDRESOURCE_ENGINE'
        self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
        self.resource = self.fmeServ.getResources()
        self.templatePythonFiles = ['DataBCFMWTemplate.py','ChangeDetectLib.py','FMELogger.py']
        self.templateSourceDir = r'\\data.bcgov\work\scripts\python\DataBCFmeTemplate2'
        #self.templateDestDir = r'/Plugins/Python/python27'
        self.templateDestDir = r'/Plugins/python/python27'

        self.depsDir = r'\\data.bcgov\work\scripts\python\DataBCFmeTemplate2\lib_ext'
        
        self.dataBCModules = ['DB', 'Misc', 'PMP']
        self.dataBCModulesDir = r'\\data.bcgov\work\scripts\python\DataBCPyLib'
        
    def DeleteRootDir(self):
        print 'deleting the plugins dir...'
        self.resource.deleteDirectory(self.engineResDir, ['Plugins'])
        print 'done'
        
    def CopyPythonFiles(self):
        self.CheckForDirectories()
        for pyFile in self.templatePythonFiles:
            src = os.path.join(self.templateSourceDir, pyFile)
            dest = posixpath.join(self.templateDestDir, pyFile)
            if not self.resource.exists(self.engineResDir, dest.split('/')):
                
                self.resource.copyFile(self.engineResDir, 
                                       self.templateDestDir.split('/'), 
                                       src, 
                                       overwrite=True
                                       )
                
    def CopyPythonDependencies(self):
        ignoreList = ['.gitignore', 'requirements.txt']
        for dirName, subdirList, fileList in os.walk(self.depsDir):
            
            for curFile in fileList:
                if curFile not in ignoreList:
                    srcFile = os.path.join(dirName, curFile)
                    relpath = os.path.relpath(dirName, self.depsDir)
                    relpath = relpath.replace('\\', '/')
                    destPath = posixpath.join(self.templateDestDir, relpath)
                    dirList = destPath.split('/')
                    print 'copying the file, ', srcFile
                    self.resource.copyFile(self.engineResDir, 
                                       dirList, 
                                       srcFile, 
                                       overwrite=True, 
                                       createDirectories=True)
    
    def CopyConfig(self):
        ignoreList = ['dbCreds.json']
        configDirName = 'config'
        srcDir = os.path.join(self.templateSourceDir, configDirName)
        destDir = posixpath.join(self.templateDestDir, configDirName)
        dirList = destDir.split('/')
        for dirName, subdirList, fileList in os.walk(srcDir):
            for curFile in fileList:
                if curFile not in ignoreList:
                    srcFile = os.path.join(dirName, curFile)
                    print 'srcFile', srcFile, destDir
                    self.resource.copyFile(self.engineResDir, 
                                               dirList, 
                                               srcFile,
                                               overwrite=True, 
                                               createDirectories=True)
    
    def CopyCustomTransformers(self):
        customizationDir = 'fmeCustomizations'
        transformersDir = 'Transformers'
        srcPath = os.path.join(self.templateSourceDir, customizationDir, transformersDir)
        for dirName, subdirList, fileList in os.walk(srcPath):
            for file2Copy in fileList:
                srcFile = os.path.join(dirName, file2Copy)
                relPath = os.path.relpath(srcFile, os.path.join(self.templateSourceDir, customizationDir))
                relPath = relPath.replace("\\", '/')
                dir = posixpath.dirname(relPath)
                print 'relPath', relPath
                print 'copying {0} to {1} on fme server'.format(srcFile, dir)
                dirList = dir.split('/')
                self.resource.copyFile(self.engineResDir, 
                                               dirList, 
                                               srcFile,
                                               overwrite=True, 
                                               createDirectories=True)
                                

    
    def CopyDataBCModules(self):
        for DataBCMod in self.dataBCModules:
            modPath = os.path.join(self.dataBCModulesDir, DataBCMod)
            for dirName, subdirList, fileList in os.walk(modPath):
                del subdirList
                for curFile in fileList:
                    junk, suffix = os.path.splitext(curFile)
                    del junk
                    if suffix <> '.pyc':
                        srcFile = os.path.join(dirName, curFile)
                        relPath = os.path.relpath(modPath, self.dataBCModulesDir)
                        relPath = relPath.replace('\\', '/')
                        destPath = posixpath.join(self.templateDestDir, relPath,  curFile)
                        
                        dirList = destPath.split('/')
                        if not self.resource.exists(self.engineResDir, dirList):
                            print 'srcFile:', srcFile
                            print 'destPath:', destPath
                            del dirList[-1]
                            self.resource.copyFile(self.engineResDir, 
                                           dirList, 
                                           srcFile,
                                           overwrite=True, 
                                           createDirectories=True)
        
    def CheckForDirectories(self):
        dirType = self.engineResDir
        dir2CreateList = self.templateDestDir.split('/')
        dirs = []
        if not self.resource.exists(dirType, dir2CreateList):
            for curDir in dir2CreateList:
                tmp = dirs[:]
                tmp.append(curDir)
                if not self.resource.exists(dirType, tmp):
                    self.resource.createDirectory(dirType, dirs, curDir)
                    dirs.append(curDir)



if __name__ == '__main__':
    fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\none2none.fmw'
    #fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\acdf_ownership_codes_staging_csv_bcgw.fmw'
    #fmwFile = r'Z:\Workspace\kjnether\proj\FMETemplateRevision\data\testFMWs\og_water_management_basins_sp_ogis_sde_bcgw.fmw'
        
    repoName = 'GuyLaFleur'
    fmwDeploy = FMWDeployment()
    fmwDeploy.CopyFMW( fmwFile, repoName)
    
    
#     deploy = PythonDeployment()
#     deploy.CopyCustomTransformers()
#     #deploy.DeleteRootDir() 
#     deploy.CopyPythonFiles()
#     deploy.CopyPythonDependencies()
#     deploy.CopyDataBCModules()
#     deploy.CopyConfig()
            
        
    