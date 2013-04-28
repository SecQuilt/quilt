#!/usr/bin/env python
import logging
import Pyro4
import threading
import pprint
import quilt_data
import quilt_parser

class QueryMaster:

    clients = {}
    _queries = quilt_data.query_specs_create()
    _history = quilt_data.query_specs_create()
    _patterns = quilt_data.pat_specs_create()
    lock = threading.Lock()

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
        with self.lock:

            # get the proxy handle to the remote object 
            
            # determine unique name for client (hopefuly just using the
            # passed in name, but double check registered list.
            uniqueName = clientName
            index = 1
            if clientType not in self.clients:
                self.clients[clientType] = {}

            while uniqueName in self.clients[clientType]:
                uniqueName = '_'.join(clientName,str(index))
                index = index + 1
               
            # Store in list of registered clients
            # use object's name and type to key it in the registerd list
            self.clients[clientType][uniqueName] = {
                'registrarHost' : nameServerHost,
                'registrarPort' : nameServerPort,
                'clientName' : clientName }
                

            # return the determined name
            logging.debug("Registered client: " + uniqueName)
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
        with self.lock:
            # if not found in the list, do nothing
            if clientType not in self.clients:
                return
            if clientNameKey not in self.clients[clientType]:
                return
            logging.debug("Unregistering client: " + clientNameKey + "...")
            clientDict = self.clients[clientType][clientNameKey]
            # remove the specified sm from registered list
            del self.clients[clientType][clientNameKey]

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
        with self.lock:
            if objType in self.clients:
                clients = self.clients[objType].copy()

        return clients

    def GetClientRec(self,objType,clientName):
        """return the master's client record for the specified client"""
        with self.lock:
            return self.clients[objType][clientName].copy()

    def DefinePattern(self, patternSpec):
        """Define the specified pattern in the query master, return
        the finalized (unique) name of the pattern"""

        try:
            # determine unique name for the pattern based off suggested name
            #   in the spec
            rootName = quilt_data.pat_spec_tryget(patternSpec, name=True)
            if rootName == None:
                rootName = "pattern"
            patternName = rootName
            i = 1

            with self.lock:
                while patternName in self._patterns.keys():
                    patternName = '_'.join([rootName,str(i)])
                    i = i + 1

                # store pattern spec in the member data
                quilt_data.pat_spec_set(patternSpec, name=patternName)
                quilt_data.pat_specs_add(self._patterns, patternSpec)
                
        
            # return the unique name for the pattern
            return patternName

        except Exception, e:
            logging.exception(e)
            raise
            
            
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

        
        # qid must be set because we use it when reporting the error
        if submitterNameKey != None:
            qid = str(submitterNameKey) + " unnamed query"
        else:
            qid = "Unknown query"

        # try the following 
        try:

            # a slightly better name for the query id
            tqid = quilt_data.query_spec_tryget(querySpec, name=True)
            if tqid != None:
                qid = tqid

            # placeholder query spec to reserve the unique qid
            tmpQuerySpec = querySpec.copy()

            # get pattern name from the query spec
            patternName = quilt_data.query_spec_get(
                querySpec, patternName=True)

            # acquire lock
            with self.lock:
                # copy the patternSpec the query references
                patternSpec = quilt_data.pat_specs_get(
                    self._patterns, patternName).copy()
                # generate a query id
                baseqid = submitterNameKey + "_" + patternName
                i = 0
                qid = baseqid 
                while qid in self._queries or qid in self._history:
                    i = i + 1
                    qid = baseqid + "_" + str(i)
                
                # store querySpec in Q to reserve query id
                quilt_data.query_spec_set(tmpQuerySpec, name=qid)
                quilt_data.query_specs_add(self._queries, tmpQuerySpec)

            # this the query spec we will be manipulating, give it the id
            # that we generate, we will eventually replace the placeholder
            # in the member q
            quilt_data.query_spec_set(querySpec, name=qid)

            code = quilt_data.pat_spec_tryget(
                    patternSpec, code=True)
            # if pattern code specified
            #   parse the pattern text, and get set of variables mentioned
            #   in the pattern code
            codeVars = None
            if code != None:
                codeVars = quilt_parser.get_pattern_vars(code)



#REVIEW
            # using the set of sources described in the query code if
            #   they exist, or the ones described by mappings otherwise
            # group variable mapping's by target source and source pattern
            #   store in local collection, if mentioned in the pattern code
            #   map{source:
            #       map{sourcePattern:
            #           map{sourcePatternInstance:
            #               map{srcVar:Var}}}}

            #   store in local collection.  We later want to iterare the
            #   source mangers efficiently, so we preprocess a srcPatDict here
            #   which provides a direct mapping from srcVariables to 
            #   queryVariables, grouped by sources and patterns
            srcPatDict = {}
            patVarSpecs = quilt_data.pat_spec_tryget(
                patternSpec, variables=True)
                
            mappings = quilt_data.pat_spec_tryget(
                patternSpec, mappings=True)


            #logging.info("got patVarSpecs: " + str(patVarSpecs))
            #logging.info("got mappings: " + str(mappings))
            if patVarSpecs != None and mappings != None:

                # if code was specified, only bother making the map for
                #   variables referenced in the code, otherwise use anything
                #   that was declared in the pattern spec
                patVars = patVarSpecs.keys()
                if codeVars != None:
                    patVars = codeVars

                for varName in patVars:
                    for m in mappings:
                        src = quilt_data.src_var_mapping_spec_get(
                            m, sourceName=True)
                        pat = quilt_data.src_var_mapping_spec_get(
                            m, sourcePattern=True)
                        var = quilt_data.src_var_mapping_spec_get(
                            m, sourceVariable=True)
                        ins = quilt_data.src_var_mapping_spec_tryget(
                                m, sourcePatternInstance=True)

                        if src not in srcPatDict:
                            patDict = {}
                            srcPatDict[src] = patDict
                        else:
                            patDict = srcPatDict[src]
                        
#REVIEW
                        if pat not in patDict:
                            insDict = {}
                            patDict[pat] = insDict
                        else:
                            insDict = patDict[pat]

                        if ins not in insDict:
                            varDict = {}
                            insDict[ins] = varDict
                        else:
                            vardict = insDict[ins]

                        varDict[var] = varName



            #logging.info("got srcPatDict: " + str(srcPatDict))

            varSpecs = quilt_data.query_spec_tryget(
                querySpec, variables=True)

            logging.debug("query varspecs: " + str(varSpecs))

            srcQuerySpecsDict = {}

            # iterate the collection of sources, and build collection of 
            # srcQuerySpecs for each source
            for source, patDict in srcPatDict.items():
                
                # initialize list of srcQueries
                srcQuerySpecsDict[source] = None

                # use variable mapping's source name to get proxy to 
                #   that source manager
                with get_client_proxy_from_type_and_name(
                    self, "SourceManager", source) as srcMgr:

                    patterns = srcMgr.GetSourcePatterns()

                    # iterate the collection of sourcePatterns for current 
                    #   source
                    for patternName in patterns:
                        # NOTE we could have decided to build a temp map
                        # of all patterns and close the source manager 
                        # connection sooner, but it will have no practical
                        # effect to keep the source manager connection a
                        # might longer while doing the below processing

                        # get the sourcePatternSpec from the proxy 
                        srcPatSpec = srcMgr.GetSourcePattern(patternName)

                        # create a query spec objects
                        srcQuerySpecs = create_src_query_specs(
                            srcPatSpec, srcPatDict, varSpecs, patVarSpecs, 
                            qid, source, patternName)

                        for srcQuerySpec in srcQuerySpecs.values():
                            # append completed sourceQuerySpec to querySpec
                            srcQuerySpecsDict[source] = (
                                quilt_data.src_query_specs_add(
                                    srcQuerySpecsDict[source], srcQuerySpec))
                        
            # use querySpec and srcQuery list
            # to create a validation string                    
            msg = {}
            msg['Query to run'] = querySpec
            msg['Sources to be queried'] = srcQuerySpecsDict.keys()
            msg['Source Queries to run'] = srcQuerySpecsDict

            validStr = pprint.pformat(msg)
            
            
            # ask submitter to validate the source Queries
            # get_client function is threadsafe, returns with the lock off
            with get_client_proxy_from_type_and_name(
                self, "QuiltSubmit", submitterNameKey) as submitter:
                
                # call back to the submitter to get validation
                validated = submitter.ValidateQuery( validStr, qid)

            # if submitter refuses to validate, 
            if not validated:
                logging.info("Submitter: " + submitterNameKey + 
                    " did not validate the query: " + qid)
                # acuire lock remove query id from q
                # delete the query record, it was determined invalid
                # return early
                with self.lock:
                    quilt_data.query_specs_del(self._queries, qid)
                return

            logging.info("Submitter: " + submitterNameKey + 
                "did validate the query: " + qid)

            # acquire lock
            with self.lock:
                # store querySpec state as INITIALIZED, and place 
                # validated contnts in member data
                quilt_data.query_spec_set(querySpec,
                    state=quilt_data.STATE_INITIALIZED)
                quilt_data.query_specs_add(self._queries,
                    querySpec)

            # Process query...
            
#REVIEW
            # iterate the sourceQuerySpec's in the src queries by source
            for source, srcQuerySpecs in srcQuerySpecsDict.items():
                # get proxy to the source manager
                with get_client_proxy_from_type_and_name( self,
                    "SourceManager", source) as smgr:

                    # NOTE:
                    # we possibly make multiple calls to the same source
                    #   this could be considered inefficient, if this happens
                    #   a lot because of the overhead with the call.  But we
                    #   get free parallelism on the client side, so not
                    #   changing this now.  Otherwise we would send all the
                    #   queries at once, and then let the client have to worry
                    #   about how to parrallelize these, but it might help the
                    #   client if the client is trying to consolidate things
                    #   to make things efficient
                    for srcQuerySpec in srcQuerySpecs.values():
                        logging.debug("Submitting query: " + qid + " to: " + 
                            source)
                        # query the source by sending it the source query spec 
                        #   as asyncronous call
                        Pyro4.async(smgr).Query(qid, srcQuerySpec)

        # catch exception! 
        except Exception, error:

            logging.exception(error)

            try:
                # submit launched this call asyncronysly and must be made
                # aware of the unexpected error
                # call submit's OnSubmitProblem
                logging.info("Attempting to get proxy for errror report")
                with get_client_proxy_from_type_and_name(
                    self, "QuiltSubmit", submitterNameKey) as submitter:
                    logging.info("Attempting to send error to submitter")
                    Pyro4.async(submitter).OnSubmitProblem(qid,error)

            except Exception, error2:
                # stuff is going horribly wrong, we couldn't notify submitter
                logging.error("Unable to notify submitter: " + 
                    str(submitterNameKey) +
                    " of error when submiting query: " + str(qid))
                logging.exception(error2)

            try:
                logging.info("Cleaning up after query error")
                # acquire lock, remove query id from pool
                querySpec = quilt_data.query_specs_del(self._queries, qid)
                # add it to the history with an error state
                quilt_data.query_spec_set(querySpec,
                    state=quilt_data.STATE_ERROR)
                quilt_data.query_specs_add(self._history, querySpec)
            except Exception, error2:
                logging.error("Failed to move error'd query to history")
                logging.exception(error2)
                

    def GetQueryQueueStats(self):
        """
        format a string with information about all the Query's in the q   
        """
        with self.lock:
            return pprint.pformat(self._queries.keys())

    def TryGetQueryStats(self, queryId):
        """
        format a string with information about the specified query
        return None if not found
        """
        
        with self.lock:
            if queryId not in self._queries:
                return None
            else:
                return pprint.pformat(self._queries[queryId])

    def GetQueryHistoryStats(self, queryId=None):
        """
        format a string with information about the specified query
        from the history, or provide stats for all if no queryId is 
        specified
        """
        try:
            # acquire lock
            with self.lock:
            # if queryID specified
                if queryId != None:
                    # throw error if query not found in history, 
                    # otherwise return the query's record
                    if queryId not in self._history:
                        raise Exception("query id: " + str(queryId) + 
                            " is not present in history")
                    results = pprint.pformat(quilt_data.query_specs_get(
                        self._history, queryId))
                else:
                    # return complete history summary
                    results = pprint.pformat(self._history)

            return results

        except Exception, e:
            logging.exception(e)
            raise
    
    def GetPatternStats(self):
        """Return a string describing the patterns defined in the query
        master"""

        with self.lock:
            return pprint.pformat(self._patterns)

#REVIEW
    def AppendQueryResults(self, queryId, srcQueryId, eventList):
        """Append the specified eventList to the specified queryId,
        and mark it complete, and move it to history list"""

        #TODO use srcQueryId to allow results to know where they came form

        srcQueryId=srcQueryId
        # acquire lock
        with self.lock:
            # mark the query as completed in state
            querySpec = quilt_data.query_specs_trydel(self._queries, queryId)
            # else find queryId in history
            if querySpec == None:
                querySpec = quilt_data.query_specs_get(self._history,
                        querySpec)

            quilt_data.query_spec_set(querySpec,
                state=quilt_data.STATE_COMPLETED)
            # set the results into the query spec
            existingEvents = quilt_data.query_spec_tryget(
                    querySpec, results=True)

            if existingEvents == None:
                existingEvents = []

            # append the results into the query spec
            quilt_data.query_spec_set(querySpec, 
                    results=(existingEvents + eventList))
            # move query spec from q to history member collection
            quilt_data.query_specs_add(self._history, querySpec)

    def OnSourceQueryError(self, source, qid, error):
        """Called when an asyncronys source query produces an exeption"""

        try:
            with self.lock:
                # mark the query as completed in state
                querySpec = quilt_data.query_specs_del(self._queries, qid)
                quilt_data.query_spec_set(querySpec,
                    state=quilt_data.STATE_ERROR)
                # move query spec from q to history member collection
                quilt_data.query_specs_add(self._history, querySpec)
        except Exception, e:
            logging.error("Unable to properly process source error")
            logging.exception(e)
            raise
        
        finally:
            logging.error("Source: " + source + 
                " was unable to process query: " + qid)
            logging.error(str(type(error)) + " : " + str(error))
            
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
    with qm.lock:
        rec = qm.clients[clientType][clientName].copy()
    return get_client_proxy(rec)
    
def create_src_query_specs(
    srcPatSpec, srcPatDict, varSpecs, patVarSpecs, qid, source, patternName):
    """
    get new sourceQuery specs from the sourcePatternSpec and the
    srcPatDict
    """

    srcQuerySpecs = None

    patInstanceDict = srcPatDict[source][patternName]

    for patInstanceName in patInstanceDict.keys():

        curSrcQuerySpec = create_src_query_spec(
                srcPatSpec, patInstanceName, varSpecs, patVarSpecs, qid, source,
                patternName)
        srcQuerySpecs = quilt_data.src_query_specs_add( srcQuerySpecs,
                curSrcQuerySpec)

    return srcQuerySpecs





def create_src_query_spec(
    srcPatSpec, srcPatInstance, varSpecs, patVarSpecs, qid, source, patternName):
    """Helper function for filling out a srcQuerySpec with variable values"""

    srcPatVars = quilt_data.src_pat_spec_get(
        srcPatSpec, variables=True)
    if srcPatVars == None:
        return

    srcVarToVarDict = srcPatDict[source][patternName][srcPatInstance]

    # logging.info("srcPatDict is " + str(srcPatDict))
    srcQueryVarSpecs = None
    # iterate the variables in the "new" 
    #   sourceQuerySpec
    for srcVarName, srcPatVarSpec in srcPatVars.items():
        
        # find that source variable in the pattern's 
        #   mappings
        # get the name of the query variable that maps to it
        varName = srcVarToVarDict[srcVarName]

        # get the value of the query variable from the
        #   querySpec, if it was given in the query spec
        varValue = None
        varSpec = quilt_data.var_specs_tryget(varSpecs, varName)
        if varSpec != None:
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
                    srcPatVarSpec, default=True)

                if varValue == None:
                    # could not determine a value for this variable
                    # must throw error
                    raise Exception("""No value set or default found
                        for the query variable: """ + varName)
        
        # create a source query variable value with the determined value
        srcQueryVarSpec = quilt_data.var_spec_create(
            name=srcVarName, value=varValue)
        # append it to a list of src query variables
        srcQueryVarSpecs = quilt_data.var_specs_add(
            srcQueryVarSpecs, srcQueryVarSpec)
    
    # Combine strings to get a name for this source query
    instName = ""
    if srcPatInstance != None:
        instName = str(srcPatInstance)
    srcQueryName = '_'.join([qid,source,patternName,srcPatInstance])

    quilt_data.src_query_spec_set(srcQuerySpec,
        name=srcQueryName)
    # create and return a src query spec
    # TODO see Issue I001, this name may not be unique
    # logging.info("srcPatSpec looks like: " + str(srcPatSpec))
    srcPatName = quilt_data.src_pat_spec_get(srcPatSpec, name=True)
    return quilt_data.src_query_spec_create(
        name=srcQueryName,
        srcPatternName=srcPatName,
        variables=srcQueryVarSpecs)


                            

