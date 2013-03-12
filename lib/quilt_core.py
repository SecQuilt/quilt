#!/usr/bin/env python
import re
import os
import time
import ConfigParser
import logging
from os import listdir
from os.path import isfile, join

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
        if QUILT_CFG_DIR_VAR in os.environ:
            cfgdir = os.environ[QUILT_CFG_DIR_VAR]        

    def __init__(self):
        """Construct the object, read in the main quilt.cfg file"""
       
        # if config file exists at location, read it in
        quiltcfg = os.path.join(GetCfgDir(), 'quilt.cfg')

        if not os.path.exists(quiltcfg):
            raise Exception("quilt config not found at: " + quiltcfg)

        self._config = Config.ConfigParser()
        self._config.read(quiltcfg)

    def GetValue(        # [out] string
        self,
        sectionName,     # [in] string, name of section
        valueName,       # [in] string, name of value
        default)         # [in] value of default
        """Access a configuration value.  Configuraiton values can be
        specified with a section and a name.  The configuration value is
        returned.  If the specified value is not present the passed in
        default is returned."""
       
        if self._config.has_optiony(sectionName,valueName):
            return self._config.get(sectionName,valueName)

        return default


    def GetSourceManagers(self)
        """get list of all defined source managers"""
    
        # iterate through the source managers defined in the configuration
        smdcfgdir = self.GetValue('source_managers', 'config_dir',
            os.path.join(self.GetCfgDir(), 'smd.d'))

        smdcfgdir = os.path.expandvars(smdcfgdir)        
        if not path.exists(smdcfgdir):
            logging.warning(
                "Source Manager config file directory does not exist" +
                smdcfgdir)
        
        smdcfgs = [ f for f in listdir(smdcfgdir) if isfile(join(smdcfgdir,f)) ]

        # read all sections from all config file sin the smd directory
        smdnames = []
        for f in smdcfgs:
            c = ConfigParser.ConfigParser()
            c.read(join(smdcfgdir, f)
                for s in c.sections():
                    smdnames.append(s)
        return smdnames
                    

    
    
    def static query_master_client_main_helper(
            clientObjectDict       # map of names to instances of objects to
                                   # host as pyro objects
        ):
        """Used to publish the client as a remote object, and complete the
        connection with the query master"""

        # Use QuiltConfig to read in configuration
        # access the registrar's host and port number from config
        # iterate the names and objects in clientObjectDic
            # register the clientObject with the local PyRo Daemon with
            # use the key name as the object name
            # call the ConnectToQueryMaster to complete registration
        # start the Daemon's event loop

        Notes:
            We wouldn't necessarily have to register the client for two way
            communicaiton if it was going to perform a non validating submit.
            However, I feel that in the future we will care more about security of
            the system, and forcing it to register will enable the two way calling
            likely used for a secret handshake

            Is there a potential timing issue here?  If the master makes a return
            call after we submit the query before our daemon event loop gets
            rolling?  Keep and eye on this.

            TODO hardening:
            The clients are not hardened to unregister themselves when
            exceptions are thrown at this time.

        
