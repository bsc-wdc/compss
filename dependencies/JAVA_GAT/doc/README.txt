INTRODUCTION
------------

This is the Java implementation of the GridLab GAT (Grid Application Toolkit).
You normally install this software on your workstation or laptop, you don't 
have to install it on the grid sites you want to use.
Documentation for this release is located in the directory "doc", including 
the language-independent specification of the GAT interface, and a tutorial 
for the Java GAT.

For more information on the GAT in general, see www.gridlab.org.

This software consists of three two parts. The GAT API, the GAT engine
and the GAT adaptors.

SETTING UP YOUR ENVIRONMENT
---------------------------

You can set the GAT_LOCATION environment variable to the location
where you installed GAT in.  This is normally not needed, if you don't
set this variable, the GAT will try to use a default, but this might
not work in all cases. So if something went wrong, try to set this
variable.

For example (depending on the os and shell you use):

export GAT_LOCATION=/home/rob/JavaGAT-2.1.1.

or 

set GAT_LOCATION=C:\JavaGAT-2.1.1.

you might want to put this line in your .bashrc (or equivalent)



BUILDING THE JAVAGAT
--------------------

To build the JavaGAT, you need the Ant tool.
Ant is open source software and can be downloaded from www.apache.org.

You can build the JavaGAT engine by typing "ant" in the Java GAT root
directory. To build the javadoc of the JavaGAT API, type "ant javadoc".

Of course, if you downloaded the binary release, you don't have to build
JavaGAT (in fact, you can't because that release does not contain sources).


RUNNING GAT APPLICATIONS
------------------------
To run GAT applications, you can use the provided example script 
scripts/run_gat_app, both on Windows and Unix systems. You do not have to use 
this script, it is just an easy example script that works in simple cases.

If you don't use this script, you have to specify the directory that
contains the .jar files of the adaptors with:
-Dgat.adaptor.path=<PATH>.
Also, you'll have to make sure that all jar-files in the lib directory
are on your classpath.

You can also control the logging
of JavaGAT using log4j.properties (see http://logging.apache.org/log4j/)


DOCUMENTATION
-------------
There is some documentation in the doc directory. It contains the GAT API 
specification. The javadocs for the Java GAT application programmers interface 
are located in the "doc/javadoc" directory.

For more documentation, please go to 

http://www.gridlab.org/WorkPackages/wp-1/documentation.html


JavaGAT has its own web-page: http://www.cs.vu.nl/ibis/javagat.html.

REPOSITORY
----------

The current JavaGAT source repository tree is accessible through SVN at
"https://gforge.cs.vu.nl/svn/javagat/trunk".  You can check it
out anonymously using the command
"svn checkout https://gforge.cs.vu.nl/svn/javagat/trunk javagat".

There is a branch for the current release (2.1.1), at
https://gforge.cs.vu.nl/svn/javagat/branches/2.1.1. Bug fixes
will find its way into this branch, and also in the trunk.

Have fun!

Roelof Kemp
rkemp@cs.vu.nl

Ceriel Jacobs
ceriel@cs.vu.nl

Rob van Nieuwpoort
rob@cs.vu.nl
