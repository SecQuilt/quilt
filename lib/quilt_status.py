#!/usr/bin/env python
import sys
import quilt_core

class QuiltStatus(quilt_core.QueryMasterClient):

    def __init__(self, args):
        # chain to call super class constructor 
        # super(QuiltStatus, self).__init__(GetType())
        quilt_core.QueryMasterClient.__init__(self,self.GetType())
        self._args = args

    def OnRegisterEnd(self):
        
        print 'Sources', self._qm.GetSourceManagerStats()
        print 'Patterns', self._qm.GetPatternStats()
        # return false (prevent event loop from beginning)
        return False
        

    def GetType(self):
        return "qstat"
        



def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('qstat',"""Display information 
        about the quilt system, including registered source managers""",
        argv)

    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltStatus(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client.localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

