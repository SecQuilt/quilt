Quilt Testing README

Quilt testing is setup to run 3 daemons on the test machine.  The Master,
SourceManager, and the Registrar. 

To run the full test suite:

$ test/bin/test_start


This will leave some daemons running.  You can run this to stop them

$ test/bin/test_stop


Typical manual testing interaction:

$ source test/bin/testenv.rc
$ daemons_start
$ define_test_pattern
$ submit_test_query
$ quilt_status

You should see the results of the test query.

You may then use quilt, for the user documentation refer to
sweng/0.1/hld_functional_specification.txt.

To stop the daemons:

$ test_stop



