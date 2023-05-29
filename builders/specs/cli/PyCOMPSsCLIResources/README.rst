-----------------------------------
PyCOMPSs programming model CLI
-----------------------------------

Introduction
============

The PyCOMPSs CLI (pycompss) provides a standalone tool to use PyCOMPSs interactively within
docker environments, local machines and remote clusters. This tool has been
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
through our ready-to-use docker image thorugh this `pycompss-cli
tool <#Installation>`__.

Installation
~~~~~~~~~~~~

.. code:: bash

    python3 -m pip install pycompss-cli

This should add the pycompss-cli executables (``pycompss``,
``compss`` and ``dislib``) to your path. They can be used indiferently.

**Warning:** The user executable path may not be automatically exported
into the ``PATH`` environment variable. So, take this into account if
installed with the ``--user`` flag, since the
``pycompss``\ \|\ ``compss`` command will be unreachable until the path
is exported into ``PATH``.


Dependencies (Optional)
^^^^^^^^^^^^^^^^^^^^^^^

For creating docker environments pycompss-cli currently requires:

-  docker >= 17.12.0-ce

1. Install docker

   -  pycompss-cli requires **docker 17.12.0-ce** or greater.

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


Usage
~~~~~

For detailed instructions and examples of usage please visit official documentation.
   -  `PyCOMPSs-CLI Usage Documentation <https://compss-doc.readthedocs.io/en/stable/Sections/08_PyCOMPSs_CLI/02_Usage.html>`__

License
=======

Apache License Version 2.0


*******

Workflows and Distributed Computing

Department of Computer Science

Barcelona Supercomputing Center (http://www.bsc.es)
