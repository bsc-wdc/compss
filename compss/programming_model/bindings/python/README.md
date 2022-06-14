========
PyCOMPSs
========

PyCOMPSs is the Python binding for the COMP Superscalar (COMPSs) framework.
It allows to run Python applications with COMPSs.


CONTENT
-------

src
L ext                     -> External C modules
|   L compssmodule.cc     -> C module to interact with the binding-commons
|   L process_affinity.cc -> C module to set the affinity
|   L process_affinity.h  -> C module to set the affinity header
L pycompss                -> Python modules
    L api                 -> API modules to be imported by the programmer in the application
    L dds                 -> DDS library
    L functions           -> Helper functions
    L interactive.py      -> Integration with Jupyter
    L runtime             -> Runtime modules
    L streams             -> Streaming functions
    L tests               -> Unittests
    L util                -> Utilities
    L worker              -> Worker modules

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
    ./install <install_dir> <create_symlinks> <specific_python_version> <compile>

    <install_dir>: Target directory to install.
    <create_symlinks>: Create symbolic links within site-packages (options: true | false).
    <specific_python_version>: Install a specific version (e.g. python3.10).
    <compile>: Compile the installation (options: true | false).
