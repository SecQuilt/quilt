#!/usr/bin/env python
#REVIEW
import os
import sys
import logging
import quilt_core
import Pyro4
import query_master
import argparse

class QuiltHistory(quilt_core.QueryMasterClient):

    def __init__(self, args):
        # chain to call super class constructor 
        quilt_core.QueryMasterClient.__init__(self,self.GetType())
        self._args = args

    def OnRegisterEnd(self):
        
        o = self._qm.GetQueryHistoryStats()
            
        if o != None:
            print o
        
        # return false (prevent event loop from beginning)
        return False

    def GetType(self):
        return "QuiltHistory"
        



def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('qhist',"""
        Quilt history will display information about completed queries.  If ID
        is provided, the Query's information will describe the variables used 
        it its definition, the state of the query, and any results that are
        available.  If there are no completed queries with the specified ID 
        present, An error occurs.""",
        argv)

    parser.add_argument('query_id',nargs='?',
        help="a query ID for a query in the history")

    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltHistory(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client._localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

