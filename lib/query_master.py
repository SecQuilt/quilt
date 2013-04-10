#!/usr/bin/env python
import os
import logging
import Pyro4
import threading
import quilt_smd
import pprint

class QueryMaster:

    _clients = {}
    _queries = {}
    _patterns = {}
    _lock = threading.Lock()

    def RegisterClient(
        self,
        nameServerHost,
        nameServerPort,
        clientName,
        clientType):
        """
        Register a client with the query master
        string RegisterClient(   # return key name for the client
            string nameServerHost,      # ip address of nameserver
            int nameServerPort,         # port for the nameserver
            string clientName           # name of the source manager on
                                        # the specified nameserver
            string clientType           # name of the type of the client
        """
        with self._lock:
            logging.info("registering client: " + clientName + "...")

            # get the proxy handle to the remote object 
            logging.debug(clientName + " is a " + clientType)
            
            # determine unique name for client (hopefuly just using the
            # passed in name, but double check registered list.
            uniqueName = clientName
            index = 1;
            if clientType not in self._clients:
                self._clients[clientType] = {}

            while uniqueName in self._clients[clientType]:
                uniqueName = '_'.join(clientName,str(index))
                index = index + 1
               
            # Store in list of registered clients
            # use object's name and type to key it in the registerd list
            self._clients[clientType][uniqueName] = {
                'registrarHost' : nameServerHost,
                'registrarPort' : nameServerPort,
                'clientName' : clientName }
                

            # return the determined name
            logging.info("registered client: " + clientName + " as " +
                uniqueName + ".")
            return uniqueName

    def UnRegisterClient(
        self,
        clientType,
        clientNameKey):
        """
        UnRegister a client with the query master
         string clientType - part of key
         string clientNameKey - key that the qmd knows to id a client
        """
        clientDict = None
        with self._lock:
            # if not found in the list, do nothing
            if clientType not in self._clients:
                return
            if clientNameKey not in self._clients[clientType]:
                return
            logging.info("Unregistering client: " + clientNameKey + "...")
            clientDict = self._clients[clientType][clientNameKey]
            # remove the specified sm from registered list
            del self._clients[clientType][clientNameKey]

        # client not found exit silently
        if clientDict == None:
            return


    def GetSourceManagerStats(self):
        """
        format a string with information about all source managers
        describe all of the source managers, and return as string 
        """
        # Get Clients is thread safe
        smgrs = self.GetClients("SourceManager")
            
        if smgrs == None:
            return "0 source managers"

        # oterate source managers, gather nfo
        s = str(len(smgrs)) + " source manager(s): \n" + pprint.pformat(smgrs)

        return s

    def GetClients(self,objType):
        """return the list of registered clients matching the specified
        type"""
        
        clients = []
        with self._lock:
            if objType in self._clients:
                clients = self._clients[objType].copy()

        return clients

    def GetClientRec(self,objType,clientName):
        """return the master's client record for the specified client"""
        with self._lock:
            return self._clients[objType][clientName].copy()

    #REVIEW
    def DefinePattern(self, patternSpec):
        """Define the specified pattern in the query master, return
        the finalized (unique) name of the pattern"""

        # determine unique name for the pattern based off suggested name
        #   in the spec
        if name in patternSpec:
            rootName = patternSpec['name']
        else:
            rootName = "pattern"
        patternName = rootName
        i = 1;

        with self._lock
            while patternName in self._patterns:
                patternName = rootName + str(i)
                i = i + 1

            # store pattern spec in the member data
            self._patterns[patternName] = patternSpec
            patternSpec[name] = patternName
    
        # return the unique name for the pattern
        return patternName
            
            
    #REVIEW
    def Query(self, submitterNameKey, querySpec):
        """
        string Query(                       # return the ID of the query
            string submitterNameKey         # key that the submitter recieved
                                            #   when it was registered with this
                                            #   master
            dict querySpec                  # the details of the query
                                            #   necessary from the user
        )
        """

        # try the following 
        try:

            # get copy of sources
            # TODO better locking, we are locking in this func and then
            #   locking again below
            sources = self.GetClients("SourceManager")

            # acquire lock
            with self._lock:
                # copy the patternSpec the query references
                patternSpec = self._patterns[querySpec['patternName']].copy()
                # generate a query id
                baseqid = submitterNameKey
                i = 0
                qid = baseqid + "_query_" + str(i)
                while qid in self._queries:
                    i = i + 1
                    qid = baseqid + "_query_" + str(i)
                
                # store querySpec in Q to reserve query id
                self._queries[qid] = querySpec
                

            # group variable mapping's by target source and source pattern
            #   store in local collection.  We later want to iterare the
            #   source mangers efficiently, so we preprocess a srcPatDict here
            #   which provides a direct mapping from srcVariables to 
            #   queryVariables, grouped by sources and patterns
            srcPatDict = {}
            if 'variables' in patternSpec:
                varSpecs = patternSpec['variables']
                for varName, varSpec in varSpecs.items():
                    if 'sourceMapping' in varSpec:
                        mappings = varSpec['sourceMapping']
                        for m in mappings:
                            src = m['sourceName']
                            pat = m['sourcePattern']
                            var = m['sourceVariable']
                            if src not in srcPatDict:
                                patDict = {}
                                srcPatDict[src] = patDict
                            else:
                                patDict = srcPatDict[src]
                            if pat not in patDict:
                                element = {}
                                patDict[pat] = element
                            else:
                                element = patDict[pat]
                            element[src] = varName

            # iterate the collection of sources
            for source in sources:
                # use variable mapping's source name to get proxy to 
                #   that source manager
                with get_client_proxy_from_type_and_name(
                    self, "SourceManager", source):

                    # iterate the collection of sourcePatterns for current 
                    #   source
                    patterns = source.GetSourcePatterns()
                    for patternName in patterns:
                        srcPatSpec = source.GetSourcePattern(patternName)
                        # get the sourcePatternSpec from the proxy as
                        #   a new sourceQuerySpec
                        #   logicaly we copy this into a sourceQuery and 
                        #   begin to fill it with variables from the query.  
                        #   But because we use pyro, we are already working 
                        #   with the copy
                        srcQuerySpec = srcPatSpec

                        if 'variables' in srcQuerySpec:
                            srcVars = srcQuerySpec['variables']
                            # iterate the variables in the new 
                            #   sourceQuerySpec
                            for srcVarName, srcVarSpec in srcVars.items():
                                fill_source_variable(
                                    srcVarSpec, srcPatDict, source)

                                # find that source variable in the pattern's 
                                #   mappings
                                # get the name of the query variable that maps to it
                            # get the value of the query variable from the
                            #   querySpec
                            # assign the sourceVariable's value to that value

                        # append completed sourceQuerySpec to querySpec

                    
            # use querySpec to create a validation  string                    
            # ask submitter to validate the source Queries
            # if submitter refuses to validate, 
                # acuire lock remove query id from q
                # return early

            # acquire lock
                #   store querySpec state as INITIALIZED

            # Process query...
            # iterate the sourceQuerySpec's in the querySpec by source
                # get proxy to the source master
                # query the source by sending it the source query spec as
                #   asyncronous call

            #catch exception! 
                # submit launched this call asyncronysly and must be made
                # aware of the unexpected error
                # call submit's OnSubmitProblem
                # acquire lock, remove query id from pool

                        
                
        
            # iterate the collection of sources
                # use variable mapping's source name to get proxy to 
                #   that source manager

                # iterate the collection of sourcePatterns for current source
                    # get the sourcePatternSpec from the proxy as
                    #   a new sourceQuerySpec
                    #   logicaly we copy this into a sourceQuery and begin to 
                    #   fill it with variables from the query.  But because we
                    #   use pyro, we are already working with the copy

                    # iterate the variables in the new sourceQuerySpec
                        # find that source variable in the pattern's mappings                        # get the name of the query variable that maps to it
                        # get the value of the query variable from the
                        #   querySpec
                        # assign the sourceVariable's value to that value

                    # append completed sourceQuerySpec to querySpec

                    
            # use querySpec to create a validation  string                    
            # ask submitter to validate the source Queries
            # if submitter refuses to validate, 
                # acuire lock remove query id from q
                # return early

            # acquire lock
                #   store querySpec state as INITIALIZED

            # Process query...
            # iterate the sourceQuerySpec's in the querySpec by source
                # get proxy to the source master
                # query the source by sending it the source query spec as
                #   asyncronous call

        #catch exception! 
            # submit launched this call asyncronysly and must be made
            # aware of the unexpected error
            # call submit's OnSubmitProblem
            # acquire lock, remove query id from pool


        queryRec = {}

        # assign unique query id and insert a query record
        # lock the query master because we need exclusive acccess to 
        #   the query list
        baseqid = submitterNameKey
        i = 0
        qid = baseqid + "_query_" + str(i)
        with self._lock:
            while qid in self._queries:
                i = i + 1
                qid = baseqid + "_query_" + str(i)
            self._queries[qid] = queryRec

        # record the actual query
        queryRec[qid] = {
            'query':query,
            'notificationAddress':notificationAddress}
                    
        # GetClients is threadsafe, but when it returns the lock is off
        smgrs = self.GetClients("SourceManager")
        # iterate the source managers, construct validation question string
        validStr = "You are about to query: "
        for name,obj in smgrs.items():
            validStr += obj["clientName"] + ", "

        # get_client function is threadsafe, returns with the lock off
        with get_client_proxy_from_type_and_name(
            self, "QuiltSubmit", submitterNameKey) as submitter:
            
            # call back to the submitter to get validation
            if not submitter.ValidateQuery( validStr, qid):
                logging.info("Submitter: " + submitterNameKey + """ did not
                    validate the query: """ + qid)
                # delete the query record, it was determined invalid
                with self._lock:
                    del self._queries[qid]
                return
            else:
                logging.info("Submitter: " + submitterNameKey + """ did 
                    validate the query: """ + qid)
            
        # iterate the source managers
        # send them the query
        for name,obj in smgrs.items():
            with get_client_proxy(obj) as smgr:
                logging.debug("Submitting query: " + qid + " to: " + 
                    obj["clientName"])
                smgr.Query(qid, query)

    def GetQueryQueueStats(self):
        """
        format a string with information about all the Query's in the q   
        """
        with self._lock:
            return pprint.pformat(self._queries.keys())

    def TryGetQueryStats(self, queryId):
        """
        format a string with information about the specified query
        return None if not found
        """
        
        with self._lock:
            if queryId not in self._queries:
                return None
            else:
                return pprint.pformat(self._queries[queryId])

    #REVIEW
    def GetQueryHistoryStats(self, queryId = None):
        """
        format a string with information about the specified query
        from the history, or provide stats for all if no queryId is 
        specified
        """
        # acquire lock
        with self._lock
        # if queryID specified
            if queryId != None:
                # throw error if query not found in history, 
                # otherwise return the query's record
                if queryId not in self._history:
                    raise Exception("query id: " + str(queryId) + 
                        " is not present in history")
                results = pprint.pformat(self._history[queryId])
            else:
                # return complete history summary
                results = pprint.pformat(self._history)

        return results
    
    #REVIEW
    def GetPatternStats(self)
        """Return a string describing the patterns defined in the query
        master"""

        with self._lock:
            return pprint.pformat(self._patterns)

def fill_source_variable(srcVarSpec, srcPatDict, source)
    
        # find that source variable in the pattern's 
        #   mappings
        # get the name of the query variable that maps to it
    # get the value of the query variable from the
    #   querySpec
    # assign the sourceVariable's value to that value

def get_client_proxy( clientRec):
    """
    return a pyro proxy object to the specified by the client record
    """
    pyroname = clientRec["clientName"]
    nshost = clientRec["registrarHost"]
    nsport = clientRec["registrarPort"]
    
    ns = Pyro4.locateNS(nshost, nsport)
    uri = ns.lookup(pyroname)

    return Pyro4.Proxy(uri)

def get_client_proxy_from_type_and_name( qm, clientType, clientName):
    # lock access to the clients of the query masteter
    with qm._lock:
        rec = qm._clients[clientType][clientName].copy()
    return get_client_proxy(rec)
    
