#!/usr/bin/env python
import os
import sys
import ConfigParser
import logging
import Pyro4
from os import listdir
from os.path import isfile, join
import threading
import getpass

class QuiltConfig:
    """Responsible for access to quilt configuration"""

    # The name of the environment variable that defines the location
    # of the quilt config directory.  This will be the filesystem path to a
    # directory containing a quilt.cfg file.  ALso it may have a smd.d
    # subdirectory where sourcemaster configurations may be
    QUILT_CFG_DIR_VAR='QUILT_CFG_DIR'

    # ConfigParser for quilt.cfg
    _config = None

    def GetCfgDir(self):
        """Get the configuration dir which is to contain config files"""
        # default configuration location to /etc/quilt
        cfgdir = '/etc/quilt'
        
        # if QUILT_CFG_DIR_VAR set, set location off of that value
        if self.QUILT_CFG_DIR_VAR in os.environ:
            cfgdir = os.environ[self.QUILT_CFG_DIR_VAR]        

        return cfgdir

    def __init__(self):
        """Construct the object, read in the main quilt.cfg file"""
       
        # if config file exists at location, read it in
        quiltcfg = os.path.join(self.GetCfgDir(), 'quilt.cfg')

        if not os.path.exists(quiltcfg):
            logging.warning("quilt config not found at: " + quiltcfg)
        else:
            self._config = ConfigParser.ConfigParser()
            self._config.read(quiltcfg)

    def GetValue(        # [out] string
        self,
        sectionName,     # [in] string, name of section
        valueName,       # [in] string, name of value
        default):        # [in] value of default
        """
        Access a configuration value.  Configuraiton values can be
        specified with a section and a name.  The configuration value is
        returned.  If the specified value is not present the passed in
        default is returned.
        """
       
        if (    self._config == None or
                not self._config.has_section(sectionName) or
                not self._config.has_option(sectionName,valueName)):
            return default

        return self._config.get(sectionName,valueName)

    def GetSourceManagers(self):
        """get list of all defined source managers"""
    
        # iterate through the source managers defined in the configuration
        smdcfgdir = self.GetValue('source_managers', 'config_dir',
            os.path.join(self.GetCfgDir(), 'smd.d'))

        smdcfgdir = os.path.expandvars(smdcfgdir)        
        if not path.exists(smdcfgdir):
            logging.warning(
                "Source Manager config file directory does not exist" +
                smdcfgdir)
        
        smdcfgs = [ f for f in listdir(smdcfgdir) 
            if isfile(join(smdcfgdir,f)) ]

        # read all sections from all config file sin the smd directory
        smdnames = []
        for f in smdcfgs:
            c = ConfigParser.ConfigParser()
            c.read(join(smdcfgdir, f))
            sections = c.sections()
            for s in sections:
                smdnames.append(s)
        return smdnames
                    

class QuiltDaemon(object):
   
    def setup_process(self, name):
        if sys.stdin.isatty():
            outdev = '/dev/tty'
        else:
            outdev = '/dev/null'

        self.stdin_path = '/dev/null'
        self.stdout_path = outdev
        self.stderr_path = outdev
        self.pidfile_path =  '/tmp/' + name + '.pid'
        self.pidfile_timeout = 5

class QueryMasterClient:
    
    _lock = threading.Lock()

    def __init__(self, basename):
        """
        use user name and pid in the name of the object
        to generate a unique enough name for this machine
        """
        self._localname = '_'.join(
            basename, getpass.getuser(), str(os.getpid()))

    def OnConnectionComplete(self):
        pass

    def GetType(self):
        raise Exception("""Abstract function is rquired to be implemented by
            subclass""")

    def ConnectToQueryMaster(self):
        """Connect to the query master, register this client"""
        
        # Access the QueryMaster's registrar's host and port from the config
        config = QuiltConfig()
        qmhost = config.GetValue("query_master", "registrar_host", None)
        qmport = config.GetValue("query_master", "registrar_port", None)

        # access the Query Master's instance name, create a proxy to it
        qmname = config.GetValue("query_master", "name", None)
        ns = Pyro4.locateNS(qmhost, qmport)
        #TODO think about adding cleanup code to client to nicly disconnect
        # store a reference to the query master as a member variable
        _qm = Pyro4.proxy(ns.lookup(qmname))
        
        # register the client with the query master, record the name
        # record the name the master assigned us as a member variable
        rport = config.GetValue("registrar", "port", None)
        if rport != None:
            rport = int(rport)
        _remotename = _qm.RegisterClient(
            config.GetValue("registrar", "host", None),
            rport,
            _localname)

        # connection complete, call our notification function
        logging.info("Connection completed for " + self._localname)
        self.OnConnectionComplete()

    def DisconnectFromQueryMaster(self):
        if _qm != None and _remotename != None:
            _qm.UnRegister(GetType(), _remotename)
            logging.info("Disconnection completed for " + self._localname)

def query_master_client_main_helper(
        clientObjectDict       # map of instances to names of objects to
                               # host as pyro objects
    ):
    """
    Used to publish the client as a remote object, and complete the
    connection with the query master
    """

    # Use QuiltConfig to read in configuration
    cfg = QuiltConfig()
    # access the registrar's host and port number from config
    registrarHost = cfg.GetValue(
        'registrar', 'host', None)
    registrarPort = cfg.GetValue(
        'registrar', 'port', None) 
    
    daemon=Pyro4.Daemon()
    ns=Pyro4.locateNS(registrarHost, registrarPort)   
    # iterate the names and objects in clientObjectDict
    for name,obj in clientObjectDict:
        # register the clientObject with the local PyRo Daemon with
        uri=daemon.register(obj)
        # use the key name as the object name
        ns.register(name,uri)
        # call the ConnectToQueryMaster to complete registration
        obj.ConnectToQueryMaster()
        
    # start the Daemon's event loop
    daemon.requestLoop() 


        
