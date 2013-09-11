#!/usr/bin/env python
import sys
import quilt_core

class QuiltHistory(quilt_core.QueryMasterClient):

    def __init__(self, args):
        # chain to call super class constructor 
        quilt_core.QueryMasterClient.__init__(self,self.GetType())
        self._args = args

    def OnRegisterEnd(self):
        
        with self.GetQueryMasterProxy() as qm:
            o = qm.GetQueryHistory(self._args.query_id)
            
        if o != None:
            quilt_core.ui_show([o])
        
        # return false (prevent event loop from beginning)
        return False

    def GetType(self):
        return "qhist"
        



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
        client.localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

