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

        # call the remote object's shutdown method
#       ns = Pyro4.locateNS(
#           clientDict['registrarHost'], clientDict['registrarPort'])
#       with Pyro4.Proxy(ns.lookup(clientDict['clientName'])) as client:
#           logging.info("Shutting down client: " + 
#               clientNameKey + "...")
#           client.shutdown()
#           logging.info("Done Shutting down client: " + 
#               clientNameKey + ".")


