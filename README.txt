Dependncies:

    python-pyro (Pyro4)
    python-daemon
    python-argparse

The rest should be std python modules.

Users
-----

Installation:

Setup soflinks in /etc/init.d and /etc/profile.d, and create the log and lock
directories

$ sudo bin/quilt install

Now. edit bin/quilt script.  Modify the dostart and dostop functions to
control which daemons will run on this machine.

You may also neet to add and edit configuration files.  Example files can be
taken from test/etc/quilt and placed in the project root

Then you could start and stop quilt with

$ sudo quilt stop
$ sudo quilt start

You can then check the status of the system.

$ source bin/quiltenv.rc
$ quilt_status

This should print out some information about the system.  For the majoirty
of the user documentation please check the project website.  But you may
also read the commmand line specification for quilt here

$ cat sweng/0.1/hld_functional_specification.txt



Developers
----------

There is no need to install.  The testing environment will run self
contained, you may begin by refrering to

test/README.txt

Design documentation is located in sweng/0.1 folder.

For more informaiton contact dhkarimi@sei.cmu.edu

