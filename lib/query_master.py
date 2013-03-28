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


    def Query(self, submitterNameKey, query, notificationAddress):
        """
        string Query(                       # return the ID of the query
            string submitterNameKey         # key that the submitter recieved
                                            # when it was registered with this
                                            # master
            string query                    # the content of the query
            string notificatoinAddress      # the email addresses for
                                            #   notification
        )
        """

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

    def GetQueryStats(self, queryId):
        """
        format a string with information about the specified query
        """
        with self._lock:
            return pprint.pformat(self._queries[queryId])

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
    
