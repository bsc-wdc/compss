===========
PyCOMPSs
===========

PyCOMPSs is the Python binding for the COMP Superscalar (COMPSs) framework. It allows to run Python applications with COMPSs.


CONTENT
-------

src
  L ext         -> external C module to interact with the binding-commons
  L pycompss    -> Python modules
    L api       -> API modules to be imported by the programmer in the application
    L dds       -> DDS library
    L functions -> Helper functions
    L runtime   -> Master runtime
    L streams   -> Streaming functions
    L util      -> utilities
    L worker    -> Worker runtime
    L interactive.py -> Integration with jupyter utilities.


DEPENDENCIES
------------
- Python 3.x
- COMPSs Java runtime
- COMPSs binding-commons


INSTALLATION
------------

- Execute the install script (requires root privileges):
    ./install

This will install PyCOMPSs in /usr/local/lib/pythonX.Y/site-packages.

- Alternatively, specify an installation directory:
    ./install <install_dir> <create_symlinks> <unify_installs> <only_python_version>

    <install_dir>: Target directory to install.
    <create_symlinks>: Create symbolic links within site-packages (options: true | false).
    <unify_installs>: Remove sources from 3 and link to 2 if exists.
    <only_python_version>: Install a specific version (options: python2 | python 3).
