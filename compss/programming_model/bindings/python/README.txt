========
PyCOMPSs
========

PyCOMPSs is the Python binding for the COMP Superscalar (COMPSs) framework.
It allows to run Python applications with COMPSs.


CONTENT
-------

src
├── ext                     -> External C modules
│   ├── compssmodule.cc     -> C module to interact with the binding-commons
│   ├── thread_affinity.cc  -> C module to set the affinity
│   └── thread_affinity.h   -> C module to set the affinity header
└── pycompss                -> Python modules
    ├── api                 -> API modules to be imported by the programmer in the application
    ├── dds                 -> DDS library
    ├── functions           -> Helper functions
    ├── interactive.py      -> Integration with Jupyter
    ├── runtime             -> Runtime modules
    ├── streams             -> Streaming functions
    ├── tests               -> Unittests
    ├── util                -> Utilities
    └── worker              -> Worker modules


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
    ./install <install_dir> <create_symlinks> <specific_python_version>

    <install_dir>: Target directory to install.
    <create_symlinks>: Create symbolic links within site-packages (options: true | false).
    <specific_python_version>: Install a specific version (e.g. python3.10).
