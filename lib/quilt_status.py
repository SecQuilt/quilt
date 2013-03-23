#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import Pyro4
import query_master
import argparse

class QuiltStatus(quilt_core.QueryMasterClient):

    def __init__(self, args):
        # chain to call super class constructor 
        # super(QuiltStatus, self).__init__(GetType())
        quilt_core.QueryMasterClient.__init__(self,self.GetType())
        self._args = args

    def OnRegisterEnd(self):
        print 'TODO call _qm.GetSourceManagerStats()'
        
        # return false (prevent event loop from beginning)
        return False
        

    def GetType(self):
        logging.debug("Returning QuiltStatus client type")
        return "QuiltStatus"
        



def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper("""Display information 
        about the quilt system, including registered source managers""",
        argv)

    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltStatus(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client._localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

