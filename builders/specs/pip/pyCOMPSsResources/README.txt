########
PyCOMPSs 
########

PyCOMPSs is a framework which aims to ease the development and execution of Python parallel applications for distributed infrastructures, such as Clusters and Clouds.

Overview
--------

PyCOMPSs is the Python binding of COMPSs, a programming model and runtime which aims to ease the development of parallel applications for distributed infrastructures, such as Clusters and Clouds. The Programming model offers a sequential interface but at execution time the runtime system is able to exploit the inherent parallelism of applications at task level. The framework is complemented by a set of tools for facilitating the development, execution monitoring and post-mortem performance analysis. 

A PyCOMPSs application is composed of tasks, which are methods annotated with decorators following the PyCOMPSs syntax. At execution time, the runtime builds a task graph that takes into account the data dependencies between tasks, and from this graph schedules and executes the tasks in the distributed infrastructure, taking also care of the required data transfers between nodes. 

Official web page: http://compss.bsc.es

Documentation
-------------

PyCOMPSs documentation can be found at http://compss.bsc.es (Documentation tab)

(See "PIP" section in the installation manual)

Installation
------------

First, be sure that the target machine satisfies the mentioned dependencies on the installation manual.

The installation can be done in various alternative ways:

Use PIP to install the official PyCOMPSs version from the pypi live repository:

.. code:: bash

    $ sudo -E python2.7 -m pip install pycompss -v

Use PIP to install PyCOMPSs from a pycompss-version.tar.gz file::

.. code:: bash

    $ sudo -E python2.7 -m pip install pycompss-version.tar.gz -v

Use the setup.py script::

.. code:: bash

    $ sudo -E python2.7 setup.py install
    

*******************************************
** Workflows and Distributed Computing   **
** Department of Computer Science        **
** Barcelona Supercomputing Center       **
*******************************************  
