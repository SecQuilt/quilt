#!/usr/bin/env python
import os
import sys
import logging
from daemon import runner
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

    def OnConnectionComplete(self):
        print _qm.GetSourceManagerStats()
        _qm.UnRegisterClient(_remotename)
        
    def GetType(self):
        return "QuiltStatus"
        


def main(argv):
    
    print "Status args: ", str(argv)

    # setup command line interface
    parser = argparse.ArgumentParser(description="""Display information 
        about the quilt system, including registered source managers""")
    parser.add_argument('-l','--log-level',nargs='?',
        help='logging level (DEBUG,INFO,WARN,ERROR) default: WARN')

    args = parser.parse_args(argv)

    # log level needs to be set if user desires
    if (args.log_level is not None):
        # if log level was specified, set the log level
        # TODO eval security issue
        strlevel = 'logging.' + args.log_level
        logging.basicConfig(level=eval(strlevel))

    # create the object and begin its life
    client = QuiltStatus(args)
    quilt_core.query_master_client_main_helper({
        client._localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

