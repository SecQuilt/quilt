import os
import logging
import Pyro4
import threading

class QueryMaster:

    _clients = {}
    _lock = threading.Lock()

    def RegisterClient(
        self,
        nameServerHost,
        nameServerPort,
        clientName):
        """
        Register a client with the query master
        string RegisterClient(   # return key name for the client
            string nameServerHost,      # ip address of nameserver
            int nameServerPort,         # port for the nameserver
            string clientName           # name of the source manager on
                                        # the specified nameserver
        """
        with self._lock:
            logging.info("Connecting client: " + clientName + "...")

            # get the proxy handle to the remote object 
            ns = Pyro4.locateNS(nameServerHost, nameServerPort)
            client = Pyro4.proxy(ns.lookup(clientName))
            clientType = client.GetType()
            
            # determine unique name for client (hopefuly just using the
            # passed in name, but double check registered list.
            uniqueName = clientName
            index = 1;
            if clientType not in clients:
                clients[clientType] = {}

            while uniqueName in clients[clientType]:
                uniqueName = '_'.join(clientName,str(index))
                index = index + 1
               
            # Store in list of registered source masters
            # use object's name and type to key it in the registerd list
            self._clients[clientType][uniqueName] = client

            # return the determined name
            logging.info("Done Connecting client: " + clientName + ".")
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
        with self._lock:
            # if not found in the list, do nothing
            if clientType not in clients:
                return
            if clientNameKey not in clients[clientType]:
                return
            client = clients[clientType][clientNameKey]
            # remove the specified sm from registered list
            del self._clients[clientType][clientNameKey]
             
            # call the remote object's shutdown method
            #TODO if this blocks too long, may need to async
            logging.info("Shutting down client: " + clientNameKey + "...")
            client.shutdown()
            logging.info("done Shutting down client: " + clientNameKey + ".")

