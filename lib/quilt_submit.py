#!/usr/bin/env python
import os
import sys
import logging
import quilt_core
import Pyro4
import query_master
import argparse

class QuiltSubmit(quilt_core.QueryMasterClient):
    """
    Submit a query to the query master.  Hang around and wait to post a
    validation to the user if desired.  User can then abort the submission
    then exit
    """

    _processEvents = True
    """
    the event loop control variable, if false event loop will
    not run, or stop running.  This variable is shared between threads
    and should not be accessed without a lock
    """

    def __init__(self, args):
        # chain to call super class constructor 
        # super(QuiltSubmit, self).__init__(GetType())
        quilt_core.QueryMasterClient.__init__(self,self.GetType())
        self._args = args

    def ValidateQuery(self, queryMetricMsg, queryId):
        """
        string queryMetricMsg    message to user about time and space
                                 requirements for the generated query
        string queryID           the ID that the query master assigned to
                                 the query
        """
        try:
            print "Query ID is:", queryId

            logging.info("Reciving validation request for query: " + 
                str(queryId))

            if not self._args.confirm_query:
                print queryMetricMsg
                while True:
                    print "Would you like to confirm query? [y,n]: "
                    c = sys.stdin.readline()
                    if len(c) >= 1:
                        c = c[0]
                        if c == 'n' or c == 'N':
                            return False
                        if c == 'y' or c == 'Y':
                            break

            logging.info("Done Recieved validation request for query: " + 
                str(queryId))
            return True
        finally:
            # set process events flag to false end event loop, allowing
            # submitter to exit
            self.SetProcesssEvents(False)
            
    def OnRegisterEnd(self):
        """After registration is complete we submit the query to the 
        query master"""
        
        logging.info('Submiting query: ' + self._args.query)
        
        # call remote method asyncronysly, this will return right away
        Pyro4.async(self._qm).Query(
            self._remotename,
            self._args.query,
            self._args.notifcation_email)

        logging.info('Done Submiting query: ' + self._args.query)
            
        # Validate query will be remote called from query master
        
        # return True to allow event loop to start running, which
        # should soon recieve a validation callback from query master
        return True
        

    def GetType(self):
        return "QuiltSubmit"
        


    def SetProcesssEvents(self, value):
        """
        value : boolean to set (in threadsafe way) that tells this client
                whether it can stop processing events
        """
        if not value:
            logging.debug("soon will no longer process events")
        with self._lock:
            # set _processEvents to the specified value
            self._processEvents = value

    def OnEventLoopBegin(self):
        """
        void OnEventLoopBegin()
            lock the class lock
            read and return value of _processEvents
        """
        with self._lock:
            return self._processEvents

    def OnEventLoopEnd(self):
        """
        void OnEventLoopBegin()
            lock the class lock
            read and return value of _processEvents
        """
        with self._lock:
            return self._processEvents

def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('qsub',
        """ 
        Quilt Submit will allow submission of a query.  It will communicate 
        with the query master, recieve estimated time/space metrics of the
        query, get user confirmation (if no -y), then deliver the query, to 
        the master for processing, print the query id, and then exit.""",
        argv)

    parser.add_argument('query',help="The query to run on the system")
    parser.add_argument('-e','--notifcation-email',nargs='?',
        help="comma seperated list of emails to supply with notifcations")
    parser.add_argument('-y','--confirm-query',action='store_true',
        default=False, help="whether to automatically confirm the query")
    
    # parse command line
    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltSubmit(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client._localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

