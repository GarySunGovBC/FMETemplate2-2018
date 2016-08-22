"""

About
=========
:synopsis:     Quick and dirty database abstraction layer
:moduleauthor: Kevin Netherton
:date:         5-21-2014
:description:  Yes could have used something like sqlalchemy.  This is 
               just a simple poller app so don't see the need for a 
               full on database abstraction layer and the extra time 
               involved with re-writing queries to use sqlalchemy.
               
               
   
:comment:  This module is a simplified version of some code I've 
           been using for a number of years. The full blown version 
           can be found at: \\spatialfiles\work\ilmb\vic\geobc\bier\corp_lib\dbutil


      

.. note:: 

      This is an example of a note


Dependencies:
-------------------
Relies on the excellent cx_Oracle module.  
http://cx-oracle.sourceforge.net/

Also relies on the AppConfigs module and that modules dependant 
config file to get the database connection parameters file and 
other database specific parameters.

      


API DOC:
===============     
"""
import inspect
import logging
import os
import sys
import cx_Oracle

import Misc.AppConfigs
    
class DbMethods():
    '''
    Just database methods.  No code in here that is specific to entering 
    info into the esri stats database. 
        
    :ivar confObj: the AppConfigs object, used to retrieve parameters
                   that are stored in the app. config file.
    :ivar connObj: The cx_Oracle connection object
    :ivar dbParams: Contains the database connection parameters.
    '''
    
    connObj = None # the db connection object
    # a hash that will be populated with database connection 
    # parameters by the method __getDbParams
    dbParams = {} 
                  
    def __init__(self, configFile=''):
        self.initLogging()
        self.logging.debug("using this version of dblib")
        if configFile:
            self.connect(configFile)
            
    def connectParams(self, user=None, pswd=None, instance=None):
        if user:
            self.dbParams['username'] = user
        if pswd:
            self.dbParams['password'] = pswd
        if instance:
            self.dbParams['instance'] = instance
        try:
            self.connObj = cx_Oracle.connect(str(self.dbParams['username']), 
                                             str(self.dbParams['password']),
                                             str(self.dbParams['instance']))
        except:
            msg = "problem encountered when trying to connect " + \
                  "to the database instance ({0}) using the "+ \
                  'id ({1}), password {2}'
            passwd = '*' * len(self.dbParams['password'])
            msg = msg.format(self.dbParams['instance'], self.dbParams['username'],  passwd)
            self.logging.debug(msg)
            raise
        
    def connectNoDSN(self, user, pswd, instance, server, port=1521):
        '''
        Allows for the creation of a database connection when the 
        client does not have a valid TNS file.  Allows you to connect
        using port and server name.
        
        :param  user: schema that you are using to connnect to the database
        :type user: str
        :param  pswd: password that goes with the schema
        :type pswd: str
        :param  instance: The database instance (service_name) that is being connected to.
        :type instance: str
        :param  server: The server that the instance resides on 
        :type server: str
        :param  port: the port that the database listener is attached to.
        :type port: int
        '''
        try:
            #dsn = cx_Oracle.makedsn(server, port, service_name=instance)
            dsn = cx_Oracle.makedsn(str(server), str(port), service_name=str(instance))
            #dsn = cx_Oracle.makedsn(str(server), str(port), str(instance)).replace('SID','SERVICE_NAME')
        except Exception, e:
            msg = 'Got an error trying to create the dsn using the ' + \
                  'service_name keyword.  Trying with no keywords'
            self.logging.debug(msg)
            self.logging.debug(str(e))
            msg = 'input params are, server: {0}, port: {1}, inst {2}'
            msg = msg.format(server, port, instance)
            dsn = cx_Oracle.makedsn(str(server), str(port), str(instance)).replace('SID','SERVICE_NAME')
            self.logging.debug('dsn returned is: {0}'.format(dsn))
        self.connectParams(user, pswd, dsn)
        
    def connect(self, configFile):
        self.confObj = Misc.AppConfigs.Config(configFile)
        self.__getDbParams()
        self.connectParams()
        
    def initLogging(self):
        '''
        This code is here just cause I like to have my log messages contain
        Module.Class.Function for each message.  If you don't care too much about what 
        you log messages look like in the log file, you can bypass this code and just 
        issue a logging.getLogger() method
        '''
        #curFile = inspect.getfile(inspect.currentframe())  
        #if curFile == '<string>':
        #    curFile = sys.argv[0]
        #logName = os.path.splitext(os.path.basename(curFile))[0] + '.' + self.__class__.__name__
        # and this line creates a log message
        #self.logging = logging.getLogger(logName)
        modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        print 'logger name', modDotClass
        self.logging = logging.getLogger(modDotClass)
        self.logging.debug("Logging set up in the module: " + str(os.path.basename(__file__)))
        
    def __getDbParams(self):
        '''
        Retrieves the database parameters from the database config file.
        '''
        # get directory that this module is in, backup one, then go 
        # into config
        
        #dbParamsFile = self.confObj.getDbParamatersFile()
        #confDir = self.confObj.getConfigDirectory()
        dbParamFileWithPath = self.confObj.getFullPathToDbParamsFile()
        print 'dbParamFileWithPath', dbParamFileWithPath
        #dbParamFileWithPath  = os.path.join(os.path.dirname(os.path.dirname(__file__)), confDir, dbParamsFile)
        #dbParamsFile = os.path.join(os.path.dirname(__file__), self.dbParamFile)
        fh = open(dbParamFileWithPath, 'r')
        self.dbParams['username'] = fh.readline().replace("\n", "")
        self.dbParams['password'] = fh.readline().replace("\n", "")
        self.dbParams['instance'] = fh.readline().replace("\n", "")
        msg = "using the db accoung: " + str(self.dbParams['username']) + \
              " and the instance: " + str(self.dbParams['instance'])
        self.logging.debug(msg)
        fh.close()
        
    def commit(self):
        '''
        commits the current connection
        '''
        self.connObj.commit()
        
    def rollback(self):
        '''
        Rolls back any transactions that have not previously been committed
        '''
        self.connObj.rollback()
        
    def executeOracleSqlNoReturn(self, sqlToExecute, values=None):
        '''
        Executes a sql statement then closes the cursor and cleans it up
        does not return anything.  The sql statement is also committed.
        
        This is the way to send DML or DDL statements to the database.
        
        :param  sqlToExecute: sql statement to be executed
        :type sqlToExecute: string
        '''
        curObj = self.connObj.cursor()
        if values:
            curObj.execute(sqlToExecute, values)
        else:
            curObj.execute(sqlToExecute)
        curObj.close()
        del curObj
        
    def executeOracleSql(self, sqlToExecute, values=None):
        '''
        Executes sql and returns the cursor object which will contain the 
        result set.
        
        :param  sqlToExecute: sql statement to execute
        :type sqlToExecute: string
        
        :returns: database cursor object that contains the results of the sql 
                  statement
        :rtype: database connection object.
        '''
        curObj = self.connObj.cursor()
        if values:
            curObj.execute(sqlToExecute, values)
        else:
            curObj.execute(sqlToExecute)
        return curObj
    
    def getCursor(self):
        '''
        Returns a database cursor object
        
        :returns: cx_Oracle cursor
        :rtype: cx_Oracle cursor
        '''
        curObj = self.connObj.cursor()
        return curObj

    def objExists(self, schema, objType, objName, connObj=None):
        '''
        Receives a schema name, database object type, database object
        name and a connection object.  Returns a boolean value indicating 
        whether there is an object with that name and type in the 
        database
        
        :param  schema: schema name
        :type schema: string
        :param  objType: object type, example 'TABLE', 'VIEW', 'INDEX' etc.
        :type objType: string
        :param  objName: name of the database object.
        :type objName: string
        :param  connObj: database connection object that implements the 
                         python database api 2.0.
        :type connObj: database connection object
        
        :returns: Describe the return value
        :rtype: boolean
        '''
        curObj = None
        if connObj == None:
            connObj = self.connObj
            
        schema = schema.upper()
        objType = objType.upper()
        objName = objName.upper()
        
        if objType.upper() == 'TABLES' or objType.upper() == 'TABLE':
            if schema <> None:
                sql = 'select * from all_tables where table_name = \'' + objName.upper() + '\' and ' + \
                      'owner = \'' + schema.upper() + '\''
                curObj = self.executeOracleSql(sql)
            else:
                sql = 'select * from all_tables where table_name = \'' + objName.upper() + '\''
                curObj = self.executeOracleSql(sql)
                
        elif objType.upper() == 'INDEX' or objType.upper() == 'INDEXES':
            whereClause = ' where index_name = \'' + objName.upper() + '\''
            if schema:
                whereClause = whereClause + ' and  owner = \'' + schema.upper() + '\''
            sql = 'select index_name from all_indexes ' + whereClause
            curObj = connObj.cursor()
            curObj.execute(sql)
        elif objType.upper() == 'VIEW' or objType.upper() == 'VIEWS':
            whereClause = ' where view_name = \'' + objName.upper() + '\''
            if schema:
                whereClause = whereClause + ' and  owner = \'' + schema.upper() + '\''
            sql = 'select view_name from all_views ' + whereClause
            curObj = connObj.cursor()
            curObj.execute(sql)
        elif objType.upper() == 'MATERIALIZEDVIEW' or objType.upper() == 'MV' or \
             objType.upper() == 'MVIEW' or objType.upper() == 'MATERIALIZED VIEW':
            # all_mviews
            whereClause = ' where MVIEW_NAME = \'' + objName.upper() + '\''
            if schema:
                whereClause = whereClause + ' and  owner = \'' + schema.upper() + '\''
            sql = 'select MVIEW_NAME from all_mviews ' + whereClause
            curObj = connObj.cursor()
            curObj.execute(sql)
        else:
            raise 'FunctionalityNotDefinedError', 'Functionality for the object type ' + objType + \
                  ' is not yet defined!'
        row = curObj.fetchone()
        retval = ''
        if row:
            retval = True
        else:
            retval = False
            
        curObj.close()
        del curObj
        return retval
    
    def getDbLinks(self, schema):
        pass
        
    def getFromDb(self, dataList, dbTable, dbCodeCol, dbValuesCol):
        '''
        This is generic method for extracting information from the 
        database.  Creating a generic method so that I don't have to 
        create a separate method to extract product, user, and org
        information from the database.  Data needs to be extracted 
        from the db to enter it, as the extraction process provides 
        us the code values for various data values.  These codes 
        are necessary for the normalization of the information in 
        the database
        
        :param  dataList: a list of the values that are to be entered
                          into the database.
        :type dataList: list
        :param  dbTable: The name of the lookup table that must contain 
                         entries for all the values in the previous list
                         dataList
        :type dbTable: string
        :param  dbCodeCol: This is the column in the previous table that 
                           contains the database codes.  This is the column
                           that establishes the relationship between the 
                           lookup table and the actual data table.
        :type dbCodeCol: string
        :param  dbValues: The column in the database that contains the 
                          names / descriptions for the values that need to 
                          be enteered.  This is where the values described in 
                          the parameter dataList exist.
        :type dbValues: string
        '''
        
        in_clause = ', '.join([':id%d' % x for x in xrange(len(dataList))]) 
        #dbFormattedList = self.quoteStrings(dataList)
        sql = 'SELECT ' + \
              dbCodeCol + ' codes, ' + \
              dbValuesCol + ' vals ' + \
              ' FROM ' + \
               dbTable + \
               ' WHERE ' + \
               ' ' + dbValuesCol + '  in ( %s )' % in_clause
               
        self.logging.debug("sql: " + str(sql))
    
        if type(dataList) is not list:
            dataList = list(dataList)
            #print 'list dataList:', dataList
        
        cur = self.executeOracleSql(sql, dataList)
        retList = []
        for line in cur:
            inList = [line[0], line[1]]
            retList.append(inList)
        return retList
                
    def closeDbConnection(self):
        self.connObj.close()

        
        
        