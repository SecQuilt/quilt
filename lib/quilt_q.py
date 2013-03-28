#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import Pyro4
import query_master
import argparse

class QuiltQueue(quilt_core.QueryMasterClient):

    def __init__(self, args):
        # chain to call super class constructor 
        # super(QuiltQueue, self).__init__(GetType())
        quilt_core.QueryMasterClient.__init__(self,self.GetType())
        self._args = args

    def OnRegisterEnd(self):
        
        if self._args.query_id == None:
            o = self._qm.GetQueryQueueStats()
        else:
            o = self._qm.TryGetQueryStats(self._args.query_id)
            
        if o != None:
            print o
        
        # return false (prevent event loop from beginning)
        return False

    def GetType(self):
        return "QuiltQueue"
        



def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('qque',"""Quilt Queue will display 
        information about queries in the queue.  If id is specified, only 
        that query's information will be displayed, if that id is not present
        nothing is displayed""",
        argv)

    parser.add_argument('-qid','--query-id',nargs='?',
        help="a query ID for a query in the quilt queue")

    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltQueue(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client._localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

