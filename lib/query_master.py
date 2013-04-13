#!/usr/bin/env python
import os
import logging
import Pyro4
import threading
import quilt_smd
import pprint
import quilt_date

class QueryMaster:

    _clients = {}
    _queries = quilt_data.query_specs_create()
    _patterns = quilt_data.pat_specs_create()
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
        rootName = quilt_data.pat_spec_tryget(patternSpec, name=True)
        if rootName == None:
            rootName = "pattern"
        patternName = rootName
        i = 1;

        with self._lock
            while patternName in self._patterns:
                patternName = rootName + str(i)
                i = i + 1

            # store pattern spec in the member data
            quilt_data.pat_spec_set(patternSpec, name=patternName)
            quilt_data.pat_specs_append(self._patterns, patternSpec)
    
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

            # placeholder query spec
            tmpQuerySpec = querySpec.copy()

            # acquire lock
            with self._lock:
                # copy the patternSpec the query references
                patternName = quilt_data.query_spec_get(patternName=True)
                patternSpec = quilt_data.pat_specs_get(
                    self._patterns,patternName).copy()
                # generate a query id
                baseqid = submitterNameKey
                i = 0
                qid = baseqid + "_query_" + str(i)
                while qid in self._queries:
                    i = i + 1
                    qid = baseqid + "_query_" + str(i)
                
                # store querySpec in Q to reserve query id
                quilt_data.query_spec_set(tmpQuerySpec, name=qid)
                quilt_data.query_specs_append(self._queries, tmpQuerySpec)

            # this the query spec we will be manipulating, give it the id
            # that we generate, we will eventually replace the placeholder
            # in the list
            quilt_data.query_spec_set(querySpec, name=qid)
                

            # group variable mapping's by target source and source pattern
            #   store in local collection.  We later want to iterare the
            #   source mangers efficiently, so we preprocess a srcPatDict here
            #   which provides a direct mapping from srcVariables to 
            #   queryVariables, grouped by sources and patterns
            srcPatDict = {}
            patVarSpecs = quilt_data.pat_spec_tryget(patternSpec,variables=True)
            if patVarSpecs != None:
                for varName, varSpec in patVarSpecs.items():
                    
                    mappings = quilt_data.var_spec_tryget(
                        varSpec,sourceMapping=True)
                    if mappings:
                        for m in mappings:
                            src = quilt_data.src_var_mapping_spec_get(
                                m,sourceName=True)
                            pat = quilt_data.src_var_mapping_spec_get(
                                m,sourcePattern=True)
                            var = quilt_data.src_var_mapping_spec_get(
                                m,sourceVariable=True)

                            if src not in srcPatDict:
                                patDict = {}
                                srcPatDict[src] = patDict
                            else:
                                patDict = srcPatDict[src]
                            
                            # need to reconcile what happens when there is a desire to
                            # use the same srcPattern multiple times in one query

                            patPresent = False
                            if pat not in patDict:
                                element = {}
                                patDict[pat] = element
                            else:
                                element = patDict[pat]
                            element[src] = varName

            varSpecs = quilt_data.query_spec_tryget(querySpec,variables=True)
            srcQuerySpecsDict = {}

            # iterate the collection of sources, and build collection of 
            # srcQuerySpecs for each source
            for source,patDict in srcPatDict.items():
                
                # initialize list of srcQueries
                srcQuerySpecsDict[source = None

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
                        srcQuerySpec = fill_src_query_spec(
                            srcPatSpec, srcPatDict, varSpecs, patVarSpecs):

                        # name for source pattern is generated from qid, which
                        # is already globally unique, then by taking on source
                        # and pattern
                        srcQueryName = '_'.join([qid,source,patternName])
                        
                        # append completed sourceQuerySpec to querySpec
                        srcQuerySpecsDict[source] = (
                            quilt_data.src_query_specs_append(
                                srcQuerySpecsDict[source], srcQuerySpec))
                        
            # use querySpec and srcQuery list
            # to create a validation  string                    
            msg = {}
            msg['Query to run'] = querySpec
            msg['Sources to be queried'] = srcQuerySpecsDict.keys()
            msg['Source Queries to run'] = srcQuerySpecsDict

            validStr = pprint.pformat(msg)
            
            
            # ask submitter to validate the source Queries
            # if submitter refuses to validate, 
            # get_client function is threadsafe, returns with the lock off
            with get_client_proxy_from_type_and_name(
                self, "QuiltSubmit", submitterNameKey) as submitter:
                
                # call back to the submitter to get validation
                validated = submitter.ValidateQuery( validStr, qid):

            if not validated:
                logging.info("Submitter: " + submitterNameKey + """ did not
                    validate the query: """ + qid)
                # acuire lock remove query id from q
                # delete the query record, it was determined invalid
                # return early
                with self._lock:
                    quilt_data.query_specs_del(self._queries, qid)
                return
            else:
                logging.info("Submitter: " + submitterNameKey + """ did 
                    validate the query: """ + qid)

            # acquire lock
                with self._lock:
                    # store querySpec state as INITIALIZED, and place 
                    # validated contnts in member data
                    quilt_data.query_spec_set(querySpec,
                        state='initialized')
                    quilt_data.query_specs_append(self._queries,
                        querySpec)

            # Process query...
            
            # iterate the sourceQuerySpec's in the src queries by source
            for source,srcQuerySpec in srcQuerySpecsDict.items():
                # get proxy to the source manager
                with get_client_proxy_from_type_and_name( self,
                    "SourceManager", source) as smgr
                    logging.debug("Submitting query: " + qid + " to: " + 
                        obj["clientName"])
                    # query the source by sending it the source query spec as
                    #   asyncronous call
                    Pyro4.async(smgr).Query(qid, srcQuerySpec)

        # catch exception! 
        except:
            error = sys.exc_info()[0]
            try:
                # submit launched this call asyncronysly and must be made
                # aware of the unexpected error
                # call submit's OnSubmitProblem
                with get_client_proxy_from_type_and_name(
                    self, "QuiltSubmit", submitterNameKey) as submitter:
                    submitter.OnSubmitProblem(qid,str(error))
            except:
                # stuff is going horribly wrong, we couldn't notify submitter
                logging.error("Unable to notify submitter: " + str(submitterNameKey) +
                    " of error when submiting query: " + str(qid))
                logging.error(error)

            try:
                # acquire lock, remove query id from pool
                querySpec = quilt_data.query_specs_del(self._queries, qid)
                # add it to the history with an error state
                quilt_data.query_spec_set(querySpec, state="error")
                quilt_data.query_specs_append(self._history, querySpec)
            except:
                logging.error("Failed to move errored query to history: " + 
                    str(sys.exc_info()[0]))
                

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

def create_source_variable(srcVar, queryVarToSrcVarDict, source):
    varSpec = { 'name' : srcVar,
                'value' : 
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
    nsport = clientRec["trarPort"]
    
    ns = Pyro4.locateNS(nshost, nsport)
    uri = ns.lookup(pyroname)

    return Pyro4.Proxy(uri)

def get_client_proxy_from_type_and_name( qm, clientType, clientName):
    # lock access to the clients of the query masteter
    with qm._lock:
        rec = qm._clients[clientType][clientName].copy()
    return get_client_proxy(rec)
    
def fill_src_query_spec(srcQuerySpec, srcPatDict, varSpecs, patVarSpecs):
    """Helper function for filling out a srcQuerySpec with variable values"""

    srcVars = quilt_data.src_query_spec_get(
        srcQuerySpec,variables=True)
    if srcVars == None:
        return

    # iterate the variables in the "new" 
    #   sourceQuerySpec
    for srcVarName, srcVarSpec in srcVars.items():
        
        # find that source variable in the pattern's 
        #   mappings
        # get the name of the query variable that maps to it
        varName = srcPatDict[source][patternName][srcVarName]

        # get the value of the query variable from the
        #   querySpec
        varSpec = quilt_data.var_specs_get(varSpecs, varName)
        varValue = quilt_data.var_spec_tryget(varSpec, value=True)

        if varValue == None:
            # user did not supply value for the variable in the
            # query, use the default in the pattern
            patVarSpec = quilt_data.var_specs_tryget(
                patVarSpecs, varName)
            varValue = quilt_data.var_spec_tryget(
                patVarSpec,default=True)

            if varValue == None:
                # pattern definer did not supply a default, check
                # to see if there is a default in the source pattern
                varValue = quilt_data.var_spec_tryget(
                    srcVarSpec, default=True)

                if varValue == None:
                    # could not determine a value for this variable
                    # must throw error
                    raise Exception("""No value set or default found
                        for the query variable: """ + varName)
        
        # assign the sourceVariable's value to the determined value
        quilt_data.var_spec_set(srcVarSpec, value=varValue)
                            

