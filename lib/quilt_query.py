#!/usr/bin/env python
#REVIEW
import sys
import logging
import quilt_core
import Pyro4
import quilt_data
import query_master

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
        self._srcQuerySpecs = None
        self._processEvents = False
        self._srcResults = {}

        
    def OnRegisterEnd(self):
        """
        Retrive the querySpec from the query master and issue the
        srcQueries to the source
        """
        try:
            # use query id passed in arguments
            if self._args.query_id != None:
                qid = self._args.query_id[0]
            else:
                raise Exception("Query ID is required")

            # Access the QuiltQuery's registrar's host and port from the config
            config = quilt_core.QuiltConfig()
            rport = config.GetValue("registrar", "port", None)
            if rport != None:
                rport = int(rport)
            rhost = config.GetValue("registrar", "host", None)

            #   set the state to ACTIVE by calling BeginQuery
            # store query as a data memeber
            self._querySpec = self._qm.BeginQuery(qid)

            # get the query spec from query master
            queryState = quilt_data.query_spec_get(self._querySpec,state=True)
            if queryState != quilt_data.STATE_ACTIVE:
                raise Exception("Query: " + qid + ", must be in " +
                        quilt_data.STATE_ACTIVE + " state. It is currently in "+
                        queryState + " state.")

            # iterate the sourceQuerySpec's in srcQueries list
            srcQuerySpecs = quilt_data.query_spec_tryget(
                    self._querySpec,sourceQuerySpecs=True)

            # there are no source query specs specified
            if srcQuerySpecs == None:
                # so just don't do anything
                self._processEvents = False
                return
                
            self._srcQuerySpecs = srcQuerySpecs
            for srcQuerySpec in srcQuerySpecs.values():
                
                # mark the sourceQuery ACTIVE
                quilt_data.src_query_spec_set(srcQuerySpec,
                        state=quilt_data.STATE_ACTIVE)

                # get proxy to the source manager
                source = quilt_data.src_query_spec_get(srcQuerySpec, source=True)
                smgrRec = self._qm.GetClientRec("smd", source)
                # TODO make more efficient by recycling proxys to same source
                # in the case when making multi source queries to same source
                with query_master.get_client_proxy(smgrRec) as smgr:
                    # query the source by sending it the source query specs as
                    #   asyncronous call
                    Pyro4.async(smgr).Query(
                            qid, srcQuerySpec, self.localname, rhost, rport)
                    # Note: no locking needed 
                    #   asyncronous call, and returnign messages not processe until
                    #   this funciton exits

            self._processEvents = True

        except Exception, error:
            try:
                self._qm.OnQueryError( qid, error)
            except Exception, error2:
                logging.error("Unable to send query startup error to query master")
                logging.exception(error2)
            finally:
                logging.error("Failed to start query")
                logging.exception(error)
        
        
        # return True to allow event loop to start running
        return self._processEvents

    def OnSourceQueryError(self, srcQueryId, exception):
        """Recieve a message from the source manager about a problem that
        occured when running the source query"""

        try:
            # log the source's exception
            # print out the source query id and the message
            logging.error("Source Query error for: " + str(srcQueryId) + 
                    " : " + quilt_core.exception_to_string(exception))

            # I guess exception's don't keep their stacktrace over the pyro
            #   boundary
            # logging.exception(exception)

            # lock self to modify memeber data
            with self._lock:
                # mark source query state with ERROR
                srcQuerySpec = quilt_data.src_query_spec_get(
                        self._srcQuerySpecs, srcQueryId)
                quilt_data.src_query_spec_set(srcQuerySpec,
                        state=quilt_data.STATE_ERROR)
                qid = quilt_data.query_spec_get(srcQuerySpec, name=True)

            # call query Master's Error function
            self._qm.OnQueryError(qid, exception)

        except Exception, error2:
            logging.error("Unable to send source query error to query master")
            logging.exception(error2)


        finally:
            # set process events flag to false end event loop, allowing
            # queryter to exit
            self.SetProcesssEvents(False)

    def AppendSourceQueryResults( self, srcQueryId, sourceResults):
        """
        string srcQueryId   # id for the source query
        sourceResults       # results of the source Query

        Called by sourceManager to push results back after running
        a source query.
        """

        logging.info("Recieved results for query: " + str(srcQueryId))

        # acquire lock
        with self._lock:
            # if srcQueryId are not yet in 
            #   the results dictionary, add their keys in
            if srcQueryId not in self._srcResults:
                results = []
                self._srcResults[srcQueryId] = results
            else:
                # get any previous results at this key
                results = self._srcResults[srcQueryId]

            # append the sourceResults at this key
            results.append(sourceResults)



    def CompleteSrcQuery(self, srcQueryId):
        """
        string srcQueryId   # id for the source query)
       
        Called by sourceManager when it is done processing the query
        """
        logging.info("Completing source query: " + str(srcQueryId))
        try:
            # NOTE: No reason to really lock here because each source will be
            #     writing to one designated state field, but it is probably 
            #     just a safe idea to lock in case of API misuse/murphy's law

            # acquire lock
            with self._lock:
                # set srcQuerie's state to COMPLETED
                srcQuerySpec = quilt_data.src_query_specs_get(
                        self._srcQuerySpecs, srcQueryId)

                # If query is progressing through the system properly it
                # should be an active state when it gets here
                srcQueryState = quilt_data.src_query_spec_get(srcQuerySpec,
                        state=True)
                if srcQueryState != quilt_data.STATE_ACTIVE:
                    raise Exception("Source Query is: " + srcQueryState +
                    ". Can only complete a query that is " +
                            quilt_data.STATE_ACTIVE)

                quilt_data.src_query_spec_set(srcQuerySpec,
                        state=quilt_data.STATE_COMPLETED)

            try:

                # TODO in future we will proabbly syncronusly process semantics
                #   here, just do the simple thing for now

                with self._lock:
                    # Detect if all src queries are completed
                    completed = True
                    for srcQuerySpec in self._srcQuerySpecs.values():
                        srcState = quilt_data.src_query_spec_get(srcQuerySpec,
                                state=True)
                        if (srcState != quilt_data.STATE_COMPLETED and 
                                srcState != quilt_data.STATE_ERROR):
                            completed = False
                            logging.debug("At least " + srcQuerySpec['name'] +
                                    " is still in progress")
                            break

                    # if this was the last source query, 
                    if completed:
                        qid = quilt_data.query_spec_get(self._querySpec, name=True)
                        logging.info("All source queries completed for: " + str(qid))
                        # Aggregate all the source queries
                        allResults = []
                        for srcResult in self._srcResults.values(): 
                            allResults += (srcResult)
                        self._qm.AppendQueryResults(qid, allResults)
                        # call query masters CompleteQuery
                        self._qm.CompleteQuery(qid)

            # catch exceptions
            except Exception, error3:
                # log out the exceptions, do not pass along to
                # source as it wasn't his fault
                logging.error(
                        "Unable to properly check for last query")
                logging.exception(error3)
                completed = True

            finally:
                # set process events flag to false end event loop, allowing
                # query client to exit
                if completed:
                    self.SetProcesssEvents(False)
        # catch exceptions
        except Exception, error2:
            # log out the exceptions, do not pass along to
            # source as it wasn't his fault
            logging.error(
                    "Unable to tell query master that the query is complete")
            logging.exception(error2)


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
        with self._lock:
            return self._processEvents

    def OnEventLoopEnd(self):
        """
        void OnEventLoopEnd()
            lock the class lock
            read and return value of _processEvents
        """
        with self._lock:
            return self._processEvents

        


def main(argv):
    
    # setup command line interface
    parser =  quilt_core.main_helper('qury',
        """ 
        Quilt Query will run a query.  It will communicate 
        with the query master, recieve the specifications for the 
        query, the issue source queries to sources.
        
        Quilt Query should not be called directly by a user
        """,
        argv)

    parser.add_argument('query_id',nargs=1,
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

