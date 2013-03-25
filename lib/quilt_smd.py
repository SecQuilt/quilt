#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import argparse

class SourceManager(quilt_core.QueryMasterClient):


    def __init__(self, args, sourceName):
        # chain to call super class constructor 
        quilt_core.QueryMasterClient.__init__(self,sourceName)
        self._args = args
        self._sourceName = sourceName

    def Query(self, query):
        self._lastQuery = query
        logging.info("Source Manager: " + self._sourceName + 
            " recieved query: " + query)

    def GetLastQuery(self):
        return self._lastQuery
        

    def GetType(self):
        logging.debug("Returning SourceManager client type")
        return "SourceManager"
        


def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('smd',"""Source manager
       daemon""",
        argv)

    # setup argument parser in accordance with funcitonal specification

    parser.add_argument('action', choices=['start', 'stop', 'restart'])
    args = parser.parse_args(argv)

    cfg = quilt_core.QuiltConfig()
    smnames = cfg.GetSourceManagers()

    objs = {}
    # iterate through source manager configurations
    for smname in smnames:
        logging.debug(smname + " specified from configuration")
        # create each source manager object
        objs[smname] = SourceManager(args, smname)
        
    # start the client with all the source managers
    quilt_core.query_master_client_main_helper(objs)
        
if __name__ == "__main__":
    main(sys.argv[1:])

