#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import Pyro4

class Registrar(quilt_core.QuiltDaemon):
    def __init__(self):
        quilt_core.QuiltDaemon.__init__(self)
        self.setup_process("registrar")

    def run(_self):

        # Use QuiltConfig to read in configuration
        cfg = quilt_core.QuiltConfig()
        # access the registrar's host and port number from config
        registrarHost = cfg.GetValue(
            'registrar', 'host', None)
        registrarPort = cfg.GetValue(
            'registrar', 'port', None) 
       
        # start the name server
        Pyro4.naming.startNSloop(registrarHost,registrarPort)




def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper("a meta server for quilt objects",
        argv)

    parser.add_argument('action', choices=['start', 'stop', 'restart'])
    parser.parse_args()
    args = parser.parse_args(argv)

    # start the daemon
    Registrar().main(argv)

if __name__ == "__main__":        
    main(sys.argv[1:])

