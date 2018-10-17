'''
This module contains various classes that
if inserted into the correct directory will
allow for overriding of default functionality
provided in the DataBCFMWTemplate

To get your FMW to use this script place it
either in the directory described in the
config files global section in the
parameter customizescriptdir

or in the current directory.  If the override
exists in both directories the fmw directory
will take precedence.

@author: kjnether
'''
import DataBCFMWTemplate
import DBCFMEConstants
import logging
import SSHTunnelling
import os.path

class Start(DataBCFMWTemplate.DefaultStart):

    def __init__(self, fme):
        # inheriting from the default startup
        DataBCFMWTemplate.DefaultStart.__init__(self, fme)

        self.const = DBCFMEConstants.TemplateConstants()

        # going to use the same logger as the DataBCFMWTemplate
        # modDotClass = '{0}'.format('CustomStartupShutdown')
        self.logger = logging.getLogger(self.const.LoggingExtendedLoggerName)

        # line above inherits the functionality of the default startup.
        # this way if you don't implement a required method it will
        # default to the default version.
        self.logger.debug("using the custom startup for the module {0}".format(__file__))

        self.ssh = SSHTunnelling.SSHTunnelHelper(fme)
        self.logger.debug("ssh tunnel object should be created")

    # override default startup
    def startup(self):
        '''
        This is where the custom startup code would go,
        '''
        # this method is currently overriding the default class
        self.logger.info("running the default startup routine")
        #         return super(Foo, self).baz(arg)
        # super(DataBCFMWTemplate.DefaultStart, self).startup()
        super(Start, self).startup()
        self.logger.debug("ssh tunnel is going to be created now")
        self.ssh.createTunnel()

        # now running the specific biot_element startup
        self.logger.debug("Runing a custom startup for {0}".format(__file__))
        self.logger.debug(" FINISHED custom startup")

class Shutdown(DataBCFMWTemplate.DefaultShutdown):

    def __init__(self, fme):
        DataBCFMWTemplate.DefaultShutdown.__init__(self, fme)

        self.fme = fme
        # modDotClass = '{0}.{1}'.format(__name__,self.__class__.__name__)
        # use the parents startup
        modDotClass = '{0}'.format('CustomStartupShutdown')
        self.logger = logging.getLogger(modDotClass)

        # any custom shutdown initiation code goes here
        self.logger.debug("custom shutdown instantiated")
        # need to get parameters from the script to create the tunnel
        self.ssh = SSHTunnelling.SSHTunnelHelper(fme)

    def shutdown(self):
        # custom shutdown methods go here.
        # you probably want to make sure that you are calling the DWM
        # writer.  This could be easily enabled by inheriting from
        # the default shutdown and then call the super classes
        # shutdown.  You can then append your functionality onto the end.
        self.logger.info("running the default startup routine")
        # super.shutdown()
        super(Shutdown, self).shutdown()

        self.logger.info("running custom shutdown routine")
        self.ssh.killTunnel()

