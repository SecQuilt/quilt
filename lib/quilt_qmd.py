#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import Pyro4
import query_master

# logging.basicConfig(level=logging.DEBUG)

class Qmd(quilt_core.QuiltDaemon):
    def __init__(self):
        quilt_core.QuiltDaemon.__init__(self)
        self.setup_process("qmd")

    def run(_self):

        # Use QuiltConfig to read in configuration
        cfg = quilt_core.QuiltConfig()
        # access the registrar's host and port number from config
        registrarHost = cfg.GetValue(
            'registrar', 'host', None)
        registrarPort = cfg.GetValue(
            'registrar', 'port', None) 
       
        # access the query master's name from the config file
        qmname = cfg.GetValue(
            'query_master', 'name', 'QueryMaster')

        logging.debug("Creating query master")
        qm = query_master.QueryMaster()
 
        daemon=Pyro4.Daemon()
        ns=Pyro4.locateNS(registrarHost, registrarPort)   
        # register the query master with the local PyRo Daemon with
        uri=daemon.register(qm)

        logging.debug("Registering: " + qmname + ", at: " + str(uri))
        # use the key name as the object name
        ns.register(qmname,uri)
        logging.debug("Done Registering: " + qmname + ", at: " + str(uri))
            
        # start the Daemon's event loop
        daemon.requestLoop() 

def main(argv):
    
    # setup command line interface
    parser = quilt_core.main_helper("qmd","Query master daemon", argv)

    parser.add_argument('action', choices=['start', 'stop', 'restart'])
    parser.parse_args()

    # start the daemon
    Qmd().main(argv)

if __name__ == "__main__":        
    main(sys.argv[1:])
