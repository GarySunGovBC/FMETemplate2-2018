
import FMEUtil.PyFMEServerV2
import pprint
import sys
import datetime
import Writers.XLWriter
import os.path

class Params(object):
    FmeServerUrl = 'http://fenite.dmz'
    FmeServerAuthKey = '93978b32c457984ac4bb1912ed27e7fcade6e3ec'
    FmeServerRepository = 'GuyLaFleur'


class RunFMWs(object):
    
    def __init__(self):
        self.params = Params()
        self.fmeServ = FMEUtil.PyFMEServerV2.FMEServer(self.params.FmeServerUrl, self.params.FmeServerAuthKey)
        self.fmeServRepo = self.fmeServ.getRepository()
        self.fmeServJobs = self.fmeServ.getJobs()
        
        dt = datetime.datetime.now()
        dtStr = dt.strftime('%Y-%m-%d_%H-%M-%S')
        workbookFile = 'runReport_{0}.xls'.format(dtStr)
        workbookDir = r'Z:\scripts\python\DataBCFmeTemplate2\util'
        workbookPath = os.path.join(workbookDir, workbookFile)
        sheetName = 'Run Reprt.'
        
        self.writer = Writers.XLWriter.XlWrite(workbookPath, sheetName)
        
        header = ['FMW',
                  'response',
                  'JobID', 
                  'NumFeaturesOutput', 
                  'Status', 
                  'StatusMsg', 
                  'timeStarted', 
                  'timeFinished',
                  'timeRequested' ]
        self.writer.addHeader(header)

    def runAllFMWSequentially(self):
        wrkspcs = self.fmeServRepo.getWorkspaces(self.params.FmeServerRepository)
        wrkspcsNames = wrkspcs.getWorkspaceNames()
        for wrkSpcName in wrkspcsNames:
            params = wrkspcs.getPublishedParams(wrkSpcName)
            params2Send = {}
            
            for param in params:
                # make sure that running against delivery.
                # should be set up like this already.
                if param['name'].upper() == 'DEST_DB_ENV_KEY':
                    print 'value was:', param
                    param['defaultValue'] = ['DLV']
                params2Send[str(param['name'])] = param['defaultValue']
                
            #payload = {}
            #payload['publishedParameters'] = params2Send
            
            #payload['TMDirectives']= {"priority": 100}
            
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(params2Send)
            print 'running the job ', wrkSpcName, '...'
            try:
                result = self.fmeServJobs.submitJobSync(self.params.FmeServerRepository, 
                                               wrkSpcName, 
                                               params2Send)
                
            except:
                print result
                result = {}
                result['id'] = 0
                result['numFeaturesOutput'] = 0
                result['status'] = 0
                result['statusMessage'] = 0
                result['timeStarted'] = 0
                result['timeFinished'] = 0
                result['timeRequested'] = 0
            print 'response is: '
            pp.pprint(result)
            rowData = [wrkSpcName,
                       '', 
                       result['id'], 
                       result['numFeaturesOutput'], 
                       result['status'], 
                       result['statusMessage'], 
                       result['timeStarted'], 
                       result['timeFinished'], 
                       result['timeRequested'] ]
            
            self.writer.addRow(rowData, toggleStyle=True)
            
            self.writer.save()

if __name__ == '__main__':
    runner = RunFMWs()
    runner.runAllFMWSequentially()
    
    