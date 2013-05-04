#!/usr/bin/env python
import sys
import logging
import quilt_core
import Pyro4
import quilt_data
import pprint

class QuiltQuery(quilt_core.QueryMasterClient):
    """
    Issue source queries from a partial querySpec, wait for the results,
    do semantic processing on the results, publish results to the 
    QueryMaster
    """

#   """
#   the event loop control variable, if false event loop will
#   not run, or stop running.  This variable is shared between threads
#   and should not be accessed without a lock
#   """
    _processEvents = True

    def __init__(self, args):
        # chain to call super class constructor 
        # super(QuiltQuery, self).__init__(GetType())
        quilt_core.QueryMasterClient.__init__(self,self.GetType())
        self._args = args
        self._querySpec = None
        self._srcQueriesSent = False

        
    def OnRegisterEnd(self):
        """
        Retrive the querySpec from the query master and issue the
        srcQueries to the source
        """

        try:
            # use query id passed in arguments
            qid = self._args.query_id

            #   set the state to ACTIVE by calling BeginQuery
            # store query as a data memeber
            self._querySpec = _qm.BeginQuery(qid)

            # get the query spec from query master
            queryState = quilt_data.query_spec_get(querySpec,state=True)
            if queryState is not quilt_data.INITIALIZED:
                raise Exception("Query: " + qid + ", must be in " +
            quilt_data.INITIALIZED + " state.")

        except Exception, error:
            try:
                self._qm.OnQueryError(
                    self._remotename, qid, error)
            except Exception, error2:
                logging.error("Unable to send query startup error to " +
                    "query master")
                logging.exception(error2)
            finally:
                logging.error("Failed to start query")
                logging.exception(error)
        
        
        # return True to allow event loop to start running
        return True

    def GetType(self):
        return "QuiltQuery"
        


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

        # iterate the sourceQuerySpec's in srcQueries list by source
        srcQuerySpecs = quilt_data.query_spec_get(
                querySpec,sourceQuerySpecs=True)
        for srcName,srcQuerySpec in srcQuerySpecs.items():
            # get proxy to the source master
            # mark the sourceQuery ACTIVE, NOTE: lock proably not needed
            # because our event loop will not start !!!!!
            # DEfect, submit loop can take a long time, we could drop
            # mexssages, move to on query loop begin!
            with query_master.get_client_proxy_from_type_and_name(
                    self._qm, "SourceManager", srcName) as smgr:

                # query the source by sending it the source query specs as
                #   asyncronous call
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

    def OnSourceQueryError(self, srcQueryId, exception):
        """Recieve a message from the source manager about a problem that
        occured when running the source query"""

        try:
            # print out the query id and the message
            logging.error("Source Query error for: " + str(srcQueryId) + 
                " : " + str(type(exception)) + " : " + str(exception))

            # I guess exception's don't keep their stacktrace over the pyro
            #   boundary
            # logging.exception(exception)

            with self._lock:
                #FIXME set state
                pass

        finally:
            # set process events flag to false end event loop, allowing
            # queryter to exit
            self.SetProcesssEvents(False)
        


def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('qsub',
        """ 
        Quilt Query will run a query.  It will communicate 
        with the query master, recieve the specifications for the 
        query, the issue source queries to sources.
        
        Quilt Query should not be called directly by a user
        """,
        argv)

    parser.add_argument('queryId',nargs=1,
        help="The specification of which query to process")

    # parse command line
    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltQuery(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client.localname : client})
        

if __name__ == "__main__":
    main(sys.argv[1:])

