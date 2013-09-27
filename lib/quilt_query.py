#!/usr/bin/env python
# Copyright (c) 2013 Carnegie Mellon University.
# All Rights Reserved.
# Redistribution and use in source and binary forms, with or without 
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice, 
# this list of conditions and the following acknowledgments and disclaimers.
# 2. Redistributions in binary form must reproduce the above copyright notice, 
# this list of conditions and the following acknowledgments and disclaimers in 
# the documentation and/or other materials provided with the distribution.
# 3. Products derived from this software may not include "Carnegie Mellon 
# University," "SEI" and/or "Software Engineering Institute" in the name of 
# such derived product, nor shall "Carnegie Mellon University," "SEI" and/or 
# "Software Engineering Institute" be used to endorse or promote products 
# derived from this software without prior written permission. For written 
# permission, please contact permission@sei.cmu.edu.
# Acknowledgments and disclaimers:
# This material is based upon work funded and supported by the Department of 
# Defense under Contract No. FA8721-05-C-0003 with Carnegie Mellon University 
# for the operation of the Software Engineering Institute, a federally funded 
# research and development center. 
#  
# Any opinions, findings and conclusions or recommendations expressed in this 
# material are those of the author(s) and do not necessarily reflect the views 
# of the United States Department of Defense. 
#  
# NO WARRANTY. THIS CARNEGIE MELLON UNIVERSITY AND SOFTWARE ENGINEERING 
# INSTITUTE MATERIAL IS FURNISHEDON AN "AS-IS" BASIS.  CARNEGIE MELLON 
# UNIVERSITY MAKES NO WARRANTIES OF ANY KIND, EITHER EXPRESSED OR IMPLIED, AS 
# TO ANY MATTER INCLUDING, BUT NOT LIMITED TO, WARRANTY OF FITNESS FOR PURPOSE 
# OR MERCHANTABILITY, EXCLUSIVITY, OR RESULTS OBTAINED FROM USE OF THE 
# MATERIAL. CARNEGIE MELLON UNIVERSITY DOES NOT MAKE ANY WARRANTY OF ANY KIND 
# WITH RESPECT TO FREEDOM FROM PATENT, TRADEMARK, OR COPYRIGHT INFRINGEMENT. 
#  
# This material has been approved for public release and unlimited distribution.
#
# Carnegie Mellon(r), CERT(r) and CERT Coordination Center(r) are registered 
# marks of Carnegie Mellon University. 
#  
# DM-0000632
import sys
import logging
import quilt_core
import Pyro4
import quilt_data
import query_master
import quilt_interpret


def at(x):
    return quilt_interpret.at(x)


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
        quilt_core.QueryMasterClient.__init__(self, self.GetType())
        self._args = args
        self._patternSpec = None
        self._querySpec = None
        self._srcQuerySpecs = None
        self._processEvents = False
        self._srcResults = {}
        self._registrarPort = None
        self._registrarHost = None


    def OnRegisterEnd(self):
        """
        Retrieve the querySpec from the query master and issue the
        srcQueries to the source
        """
        qid = None
        try:
            # use query id passed in arguments
            if self._args.query_id is not None:
                qid = self._args.query_id[0]
            else:
                raise Exception("Query ID is required")

            # Access the QuiltQuery's registrar's host and port from the config
            config = quilt_core.QuiltConfig()
            rport = config.GetValue("registrar", "port", None, int)
            if rport is not None:
                rport = int(rport)
            rhost = config.GetValue("registrar", "host", None)
            self._registrarPort = rport
            self._registrarHost = rhost

            #   set the state to ACTIVE by calling BeginQuery
            # store pattern and query as a data member
            with self.GetQueryMasterProxy() as qm:
                self._patternSpec, self._querySpec = qm.BeginQuery(qid)

                # get the query spec from query master
                queryState = quilt_data.query_spec_get(self._querySpec,
                                                       state=True)
                if queryState != quilt_data.STATE_ACTIVE:
                    raise Exception("Query: " + qid + ", must be in " +
                                    quilt_data.STATE_ACTIVE +
                                    " state. It is currently in " +
                                    queryState + " state.")

                # iterate the sourceQuerySpec's in srcQueries list
                srcQuerySpecs = quilt_data.query_spec_tryget(
                    self._querySpec, sourceQuerySpecs=True)

                # there are no source query specs specified
                if srcQuerySpecs is None:
                    # so just don't do anything
                    self._processEvents = False
                    return

                self._srcQuerySpecs = srcQuerySpecs
                for srcQuerySpec in srcQuerySpecs.values():
                    # mark the sourceQuery ACTIVE
                    quilt_data.src_query_spec_set(srcQuerySpec,
                                                  state=quilt_data.STATE_ACTIVE)

                    # get proxy to the source manager
                    source = quilt_data.src_query_spec_get(
                        srcQuerySpec, source=True)
                    smgrRec = qm.GetClientRec("smd", source)
                    # TODO make more efficient by recycling proxies to same source
                    # in the case when making multi source queries to same source
                    with query_master.get_client_proxy(smgrRec) as smgr:
                        # query the source by sending it the source query specs as
                        #   asynchronous call
                        Pyro4.async(smgr).Query(
                            qid, srcQuerySpec, self.localname, rhost, rport)
                        # Note: no locking needed 
                        #   asynchronous call, and returning messages not
                        #   processed until this function exits

            self._processEvents = True


        except Exception, error:
            try:
                with self.GetQueryMasterProxy() as qm:
                    qm.OnQueryError(qid, error)
            except Exception, error2:
                logging.error(
                    "Unable to send query startup error to query master")
                logging.exception(error2)
            finally:
                logging.error("Failed to start query")
                logging.exception(error)


        # return True to allow event loop to start running
        return self._processEvents

    def OnSourceQueryError(self, srcQueryId, exception):
        """Receive a message from the source manager about a problem that
        :param srcQueryId:
        :param exception:
        occurred when running the source query"""

        try:
            # log the source's exception
            # print out the source query id and the message
            logging.error("Source Query error for: " + str(srcQueryId) +
                          " : " + quilt_core.exception_to_string(exception))

            # I guess exception's don't keep their stacktrace over the pyro
            #   boundary
            # logging.exception(exception)

            # lock self to modify member data
            with self._lock:
                # mark source query state with ERROR
                srcQuerySpec = quilt_data.src_query_spec_get(
                    self._srcQuerySpecs, srcQueryId)
                quilt_data.src_query_spec_set(srcQuerySpec,
                                              state=quilt_data.STATE_ERROR)
                qid = quilt_data.query_spec_get(srcQuerySpec, name=True)

            # call query Master's Error function
            with self.GetQueryMasterProxy() as qm:
                qm.OnQueryError(qid, exception)

        except Exception, error2:
            logging.error("Unable to send source query error to query master")
            logging.exception(error2)


        finally:
            # set process events flag to false end event loop, allowing
            # query to exit
            self.SetProcesssEvents(False)

    def AppendSourceQueryResults(self, srcQueryId, sourceResults):
        """
        string srcQueryId   # id for the source query
        sourceResults       # results of the source Query

        Called by sourceManager to push results back after running
        a source query.
        """

        logging.info("Received results for query: " + str(srcQueryId))

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
            # noinspection PyUnusedLocal
            results += sourceResults


    def CompleteSrcQuery(self, srcQueryId):
        """
        string srcQueryId   # id for the source query)
       
        Called by sourceManager when it is done processing the query
        """
        logging.info("Completing source query: " + str(srcQueryId))
        try:
            # NOTE:
            #   Because the member data query specs are not entries are not
            #   modified (Except during initialization) we do not have to lock
            #   before reading from the list.  Also we assume no results
            #   should be appended after a call to complete query so that we
            #   can sort the list of data outside of a lock.  Also see note 
            #   in AppendSourceQueryResults which could change the logic here


            # get the source query from the member data collection of src 
            #   queries using the srcQueryId
            srcQuerySpec = quilt_data.src_query_specs_get(
                self._srcQuerySpecs, srcQueryId)


            # if src query specifies that source returns out of order results
            if not quilt_data.src_query_spec_get(srcQuerySpec, ordered=True):
                results = None
                with self._lock:
                    if srcQueryId in self._srcResults:
                        results = self._srcResults[srcQueryId]

                if results is not None:
                    # sort results  by timestamp using interpret's 'at'
                    #   function
                    results.sort(key=lambda rec: at(rec))


            # acquire lock
            with self._lock:
                # set srcQueries's state to COMPLETED

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
                        # get proxy to self
                        with Pyro4.Proxy(self.uri) as selfProxy:
                            # asynchronously call self's CompleteQuery
                            Pyro4.async(selfProxy).CompleteQuery()

            # catch exceptions
            except Exception, error3:
                # log out the exceptions, do not pass along to
                # source as it wasn't his fault
                logging.error(
                    "Unable to properly check for last query")
                logging.exception(error3)

                # set process events flag to false end event loop, allowing
                # query client to exit
                self.SetProcesssEvents(False)
                raise

        # catch exceptions
        except Exception, error2:
            # log out the exceptions, do not pass along to
            # source as it wasn't his fault
            logging.error(
                "Unable to tell query master that the query is complete")
            logging.exception(error2)
            # set process events flag to false end event loop, allowing
            # query client to exit
            self.SetProcesssEvents(False)
            raise


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

    def CompleteQuery(self):

        qid = None
        # try the following
        try:
            # lock self
            with self._lock:
                code = quilt_data.pat_spec_tryget(
                    self._patternSpec, code=True)
                qid = quilt_data.query_spec_get(self._querySpec, name=True)

                # if there is no query code
                if code is None:
                    with self.GetQueryMasterProxy() as qm:
                        # for each source result
                        for curResult in self._srcResults.values():
                            # append source results to query master
                            qm.AppendQueryResults(qid, curResult)
                            # call query masters CompleteQuery
                        qm.CompleteQuery(qid)

                else:
                    logging.info("Interpreting query: " + str(qid))
                    # evaluate the query by calling quilt_interpret
                    #   pass the query spec, and source Results
                    result = quilt_interpret.evaluate_query(self._patternSpec,
                                                            self._querySpec,
                                                            self._srcResults)
                    # append the returned results to the query master's
                    #   results for this query
                    logging.info("Posting results for query: " + str(qid))
                    with self.GetQueryMasterProxy() as qm:
                        qm.AppendQueryResults(qid, result)
                        # call query masters CompleteQuery
                        qm.CompleteQuery(qid)

        # catch exceptions
        except Exception, error:
            # log out the exceptions, 
            # Call query master's OnQueryError
            try:
                with self.GetQueryMasterProxy() as qm:
                    qm.OnQueryError(qid, error)
            except Exception, error2:
                logging.error(
                    "Unable to send query interpret error to query master")
                logging.exception(error2)
            finally:
                logging.error("Failed to interpret query")
                logging.exception(error)
        finally:
            # set process events flag to false end event loop, allowing
            # query client to exit
            self.SetProcesssEvents(False)


def main(argv):
    # setup command line interface
    parser = quilt_core.main_helper('qury',
                                    """
        Quilt Query will run a query.  It will communicate
        with the query master, receive the specifications for the
        query, the issue source queries to sources.

        Quilt Query should not be called directly by a user
        """,
                                    argv)

    parser.add_argument('query_id', nargs=1,
                        help="The specification of which query to process")

    # parse command line
    args = parser.parse_args(argv)

    # create the object and begin its life
    client = QuiltQuery(args)

    # start the client
    quilt_core.query_master_client_main_helper({
        client.localname: client})


if __name__ == "__main__":
    main(sys.argv[1:])

