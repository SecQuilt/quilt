#!/usr/bin/env python
import os
import sys
import logging
from daemon import runner
import quilt_core
import Pyro4
import query_master

class Qmd(quilt_core.QuiltDaemon):
    def __init__(self):
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

        qm = query_master.QueryMaster()
 
        daemon=Pyro4.Daemon()
        ns=Pyro4.locateNS(registrarHost, registrarPort)   
        # register the query master with the local PyRo Daemon with
        uri=daemon.register(qm)
        # use the key name as the object name
        ns.register(qmname,uri)
            
        # start the Daemon's event loop
        daemon.requestLoop() 



daemon_runner = runner.DaemonRunner(Qmd())
daemon_runner.do_action()
