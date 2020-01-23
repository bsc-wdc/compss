-----------------------------------
PyCOMPSs programming model Player
-----------------------------------

Introduction
============

The PyCOMPSs player (pycompss) provides a tool to use PyCOMPSs within
local machines interactively through docker images. This tool has been
implemented on top of `PyCOMPSs programming
model <http://compss.bsc.es>`__, and it is being developed by the
`Workflows and Distributed Computing
group <https://github.com/bsc-wdc>`__ of the `Barcelona Supercomputing
Center <https://www.bsc.es/>`__.

Contents
========

-  `Quickstart <#quickstart>`__
-  `License <#license>`__

Quickstart
==========

There are two ways in which you can get started with PyCOMPSs. You can
perform a local installation by installing the `pycompss
package <https://pypi.org/project/pycompss/>`__, or you can use it
through our ready-to-use docker image thorugh this `pycompss-player
tool <#Installation>`__.

Installation
~~~~~~~~~~~~

Dependencies
^^^^^^^^^^^^

pycompss-player currently requires:

-  docker >= 17.12.0-ce

Installation steps
^^^^^^^^^^^^^^^^^^

1. Install docker (continue with step 2 if already installed)

   -  pycompss-player requires **docker 17.12.0-ce** or greater.

   1. Follow these instructions

      -  `Docker for
         Mac <https://store.docker.com/editions/community/docker-ce-desktop-mac>`__.
         Or, if you prefer to use `Homebrew <https://brew.sh/>`__.

      -  `Docker for
         Ubuntu <https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce-1>`__.

      -  `Docker for Arch
         Linux <https://wiki.archlinux.org/index.php/Docker#Installation>`__.

      Be aware that for some distros the docker package has been renamed
      from ``docker`` to ``docker-ce``. Make sure you install the new
      package.

   2. Add user to docker group to run pycompss as a non-root user.

      -  `Instructions <https://docs.docker.com/install/linux/linux-postinstall/>`__

   3. Check that docker is correctly installed

   .. code:: bash

       docker --version
       docker ps # this should be empty as no docker processes are yet running.

   4. Install
      `docker-py <https://docker-py.readthedocs.io/en/stable/>`__

   .. code:: bash

       python3 -m pip install docker

2. Install pycompss-player:

.. code:: bash

    python3 -m pip install pycompss-player

This should add the pycompss-player executables (``pycompss`` and
``compss``) to your path. They can be used indiferently.

**Warning:** The user executable path may not be automatically exported
into the ``PATH`` environment variable. So, take this into account if
installed with the ``--user`` flag, since the
``pycompss``\ \|\ ``compss`` command will be unreachable until the path
is exported into ``PATH``.

Usage
~~~~~

Start ``pycompss`` in your development directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Initialize the COMPSs infrastructure where your source code will be (you
can re-init anytime). This will allow docker to access your local code
and run it inside the container.

**Note** that the first time needs to download the docker image from the
registry, and it may take a while.

.. code:: bash

    # Without a path it operates on the current working directory.
    pycompss init

    # You can also provide a path
    pycompss init -w /home/user/replace/path/

    # Or the COMPSs docker image to use
    pycompss init -i compss/compss-tutorial:2.6

    # Or both
    pycompss init -w /home/user/replace/path/ -i compss/compss-tutorial:2.6

Running applications
^^^^^^^^^^^^^^^^^^^^

First clone the PyCOMPSs' tutorial apps repository:

.. code:: bash

    git clone https://github.com/bsc-wdc/tutorial_apps.git

Init the COMPSs environment in the root of the repository. The source
files path are resolved from the init directory which sometimes can be
confusing. As a rule of thumb, initialize the library in a current
directory and check the paths are correct running the file with
``python3 path_to/file.py`` (in this case
``python3 python/simple/src/simple.py``).

.. code:: bash

    cd tutorial_apps
    pycompss init
    pycompss run python/simple/src/simple.py 1

The log files of the execution can be found at $HOME/.COMPSs.

You can also init the COMPSs environment inside the examples folder.
This will mount the examples directory inside the container so you can
execute it without adding the path:

.. code:: bash

    cd python/simple/src
    pycompss init
    pycompss run simple.py 1

Running the COMPSs monitor
^^^^^^^^^^^^^^^^^^^^^^^^^^

The COMPSs monitor can be started using the ``pycompss monitor start``
command. This will start the COMPSs monitoring facility which enables to
check the application status while running. Once started, it will show
the url to open the monitor in your web browser
(http://127.0.0.1:8080/compss-monitor)

**Reminder**: Include the monitor flag in the execution before the
binary to be executed.

.. code:: bash

    cd python/simple/src
    pycompss init
    pycompss run --monitor=1000 -g simple.py 1

If running a notebook, just add the monitoring parameter into the COMPSs
runtime start call.

Once finished, it is possible to stop the monitoring facility by using
the ``pycompss monitor stop`` command.

Running Jupyter notebooks
^^^^^^^^^^^^^^^^^^^^^^^^^

Notebooks can be run using the ``pycompss jupyter`` command. Run the
following snippet from the root of the project:

.. code:: bash

    cd tutorial_apps/python
    pycompss init
    pycompss jupyter ./notebooks

An alternative and more flexible way of starting jupyter is using the
``pycompss run`` command in the following way:

.. code:: bash

    pycompss run jupyter-notebook ./notebooks --ip=0.0.0.0  --allow-root

Access your notebook by ctrl-clicking or copy pasting into the browser
the link shown on the CLI (e.g.
http://127.0.0.1:8888/?token=TOKEN\_VALUE).

If the notebook process is not properly closed, you might get the
following warning when trying to start jupyter notebooks again:

``The port 8888 is already in use, trying another port.``

To fix it, just restart the pycompss container with ``pycompss init``.

Generating the task graph
^^^^^^^^^^^^^^^^^^^^^^^^^

COMPSs is able to produce the task graph showing the dependencies that
have been respected. In order to producee it, include the graph flag in
the execution command:

.. code:: bash

    cd python/simple/src
    pycompss init
    pycompss run --graph simple.py 1

Once the application finishes, the graph will be stored into the
``~\.COMPSs\app_name_XX\monitor\complete_graph.dot`` file. This dot file
can be converted to pdf for easier visualilzation through the use of the
``gengraph`` parameter:

.. code:: bash

    pycompss gengraph .COMPSs/simple.py_01/monitor/complete_graph.dot

The resulting pdf file will be stored into the
``~\.COMPSs\app_name_XX\monitor\complete_graph.pdf`` file, that is, the
same folder where the dot file is.

Tracing applications or notebooks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

COMPSs is able to produce tracing profiles of the application execution
through the use of EXTRAE. In order to enable it, include the tracing
flag in the execution command:

.. code:: bash

    cd python/simple/src
    pycompss init
    pycompss run --tracing simple.py 1

If running a notebook, just add the tracing parameter into the COMPSs
runtime start call.

Once the application finishes, the trace will be stored into the
``~\.COMPSs\app_name_XX\trace`` folder. It can then be analysed with
Paraver.

Adding more nodes
^^^^^^^^^^^^^^^^^

**Note**: adding more nodes is still in beta phase. Please report
issues, suggestions, or feature requests on
`Github <https://github.com/bsc-wdc/>`__.

To add more computing nodes, you can either let docker create more
workers for you or manually create and config a custom node.

For docker just issue the desired number of workers to be added. For
example, to add 2 docker workers:

.. code:: bash

    pycompss components add worker 2

You can check that both new computing nodes are up with:

.. code:: bash

    pycompss components list

If you want to add a custom node it needs to be reachable through ssh
without user. Moreover, pycompss will try to copy the ``working_dir``
there, so it needs write permissions for the scp.

For example, to add the local machine as a worker node:

.. code:: bash

    pycompss components add worker '127.0.0.1:6'

-  '127.0.0.1': is the IP used for ssh (can also be a hostname like
   'localhost' as long as it can be resolved).
-  '6': desired number of available computing units for the new node.

**Please be aware** that ``pycompss components`` will not list your
custom nodes because they are not docker processes and thus it can't be
verified if they are up and running.

Removing existing nodes
^^^^^^^^^^^^^^^^^^^^^^^

**Note**: removing nodes is still in beta phase. Please report issues,
suggestions, or feature requests on
`Github <https://github.com/bsc-wdc/>`__.

For docker just issue the desired number of workers to be removed. For
example, to remove 2 docker workers:

.. code:: bash

    pycompss components remove worker 2

You can check that the workers have been removed with:

.. code:: bash

    pycompss components list

If you want to remove a custom node, you just need to specify its IP and
number of computing units used when defined.

.. code:: bash

    pycompss components remove worker '127.0.0.1:6'


License
=======

Apache License Version 2.0


*******

Workflows and Distributed Computing

Department of Computer Science

Barcelona Supercomputing Center (http://www.bsc.es)
