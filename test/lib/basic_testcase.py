#!/usr/bin/env python
import os
import sys
import unittest
import quilt_test_core
import sei_core
import quilt_core
import query_master
import random
import pickle
import logging

class BasicTestcase(unittest.TestCase):

    # check qmd to be sure that all quilt_status's 
    # have unregistered when process exits
    def verify_unregistration(self, clientType, cfg):
        with quilt_core.GetQueryMasterProxy(cfg) as qm:
            qs = qm.GetClients(clientType)
            self.assertTrue( len(qs) == 0 )

    #just make sure that the scripts execute at all
    #without throwing an exception
    def test_basic_query(self): #PASSING
        try:
            qstr = 'find me ' + str(random.random())

            # call quilt_submit "find me" (check return code)
            quilt_test_core.call_quilt_script('quilt_submit.py',
                [ qstr, '-y' ], checkCall=False)

            # sleep a small ammount
            quilt_test_core.sleep_small()

        # Use QuiltConfig, Call GetSourceManagers, for each one
            # use pyro, create proxy for the smd
            # call GetLastQuery()
            # assure it is equal to "find me again [uniqueval]"
        #cfg = quilt_core.QuiltConfig()
        #with quilt_core.GetQueryMasterProxy(cfg) as qm:
        #    smgrs = qm.GetClients("SourceManager")

        #    for smgr in smgrs:
        #        clientRec =  qm.GetClientRec("SourceManager", smgr)
        #        with query_master.get_client_proxy(clientRec) as smgrClient:
        #            lastQuery = smgrClient.GetLastQuery()
                    # after more functionality was added sources won't be called
                    # unless referenced in the query.  we should no longer
                    # see the unique query string down on the source
                    # we wouldn't anyway because it isn't the ID.  The 
                    # last query stuff was just a hack to get testing in
                    # 0.1.c1, and is added to issue list to remove
                    # TODO see ISSUE004
        #            self.assertTrue(lastQuery != qstr )

            #self.verify_unregistration('QuiltSubmit', cfg)
            
        except Exception, e:
            logging.exception(e)
            self.fail('Exception thrown when calling quilt_submit.py')
        
    # test submitting a query by making sure:
    # (1) that you can run the query, and 
    # (2) that the query returns more than just an empty string
    def test_submit(self): #PASSING

        try:
            qstr = 'find me ' + str(random.random())

            # define the query
            argv = [ '-n', qstr ]
            quilt_test_core.call_quilt_script('quilt_define.py', argv)

            # call quilt_submit "find me" (check return code)
            argv = [ qstr, '-y' ]
            o = quilt_test_core.call_quilt_script('quilt_submit.py', argv)
            objs = quilt_test_core.retrieve_objects(o)
            quilt_test_core.log_objs("test_submit:quilt_submit.py", objs)

            # sleep a small ammount
            quilt_test_core.sleep_small()

            #self.assertTrue(o != None and len(o) > 1)
            #a = o.index("Query ID is: ") + len(str("Query ID is: "))
            #qid = o[a:]
            qid = objs[1]

            # make sure that we get a good query id back from the submit
            self.assertTrue(len(qid) > 0)
        
            # call quilt_q (check return code, capture output)
            # this line of code appears to be superfluous
            #o = quilt_test_core.call_quilt_script('quilt_q.py')
        
            # call quilt_q -q query_id (check return code, capture output)
            o = quilt_test_core.call_quilt_script('quilt_q.py',[qid])
            objs = quilt_test_core.retrieve_objects(o)
            quilt_test_core.log_objs("test_submit:quilt_q.py", objs)

            # ensure output isn't empty 
            self.assertTrue(o != None and len(o) > 0)

            #self.verify_unregistration('QuiltSubmit', cfg)

        except Exception, e:
            logging.exception(e)
            self.fail('Exception thrown when calling quilt_submit.py')

    def test_basic_status(self): #PASSING

        quilt_lib_dir = quilt_test_core.get_quilt_lib_dir()

        # assemble the filename for query master
        quilt_status_file = os.path.join(quilt_lib_dir,'quilt_status.py')

        # call quilt_status (check return code, capture output)
        o = sei_core.run_process([quilt_status_file,'-l','DEBUG'],
            whichReturn=sei_core.STDOUT, logToPython=False)
        objs = quilt_test_core.retrieve_objects(o)
        quilt_test_core.log_objs("+++++test_basic_status:quilt_status.py", objs)
        
        # ensure that all of the registered source managers specified in
        # testing config smd.d dir appear in the output of the quilt_status call
        cfg = quilt_core.QuiltConfig()
        smgrs = cfg.GetSourceManagers()
        for smgr in smgrs:
            logging.debug("+++++smgr = " + str(smgr))
            self.assertTrue(quilt_test_core.pattern_found(str(smgr), objs[1]))

        self.verify_unregistration('QuiltStatus', cfg)
        

    # make sure that a fake qid does not return any data
    def test_incorrect_qid(self): #PASSING

        try:
            qstr = 'find me again ' + str(random.random())

            # define the query
            quilt_test_core.call_quilt_script('quilt_define.py',
                [ '-n', qstr ] )

            # call quilt_submit "find me again" (check return code)
            quilt_test_core.call_quilt_script('quilt_submit.py',
                [ qstr, '-y' ])

            # sleep a small ammount
            quilt_test_core.sleep_small()

            # call quilt_q -q FAKEid (check return code, capture output)
            o = quilt_test_core.call_quilt_script('quilt_q.py',['FAKE'])
            objs = quilt_test_core.retrieve_objects(o)
            quilt_test_core.log_objs("test_incorrect_qid:quilt_q.py", objs)

            # make sure that there is no output when a fake id is supplied 
            self.assertTrue(objs == None or len(objs)==0)

            # check qmd to be sure that all quilt_q's have unregistered
            # do this by accessing Config, finding qmd name,
            # create pyro proxy for qmd, call getRegistedObjects(type(QuiltQ))
            # make sure list is empty
            cfg = quilt_core.QuiltConfig()
            self.verify_unregistration('QuiltQueue', cfg)

        except Exception, e:
            logging.exception(e)
            self.fail('Exception thrown when calling quilt_submit.py')

        
if __name__ == "__main__":
    quilt_test_core.unittest_main_helper("Run the most basic of test cases",sys.argv)
    unittest.main()
