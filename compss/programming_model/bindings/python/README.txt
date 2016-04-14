===========
PyCOMPSs
===========

PyCOMPSs is the Python binding for the COMP Superscalar (COMPSs) framework. It allows to run Python applications with COMPSs.


CONTENT
-------

src
    ├── ext         -> external C module to interact with the COMPSs C/C++ binding
    └── pycompss    -> Python modules
        ├── api     -> API modules to be imported by the programmer in the application
        ├── runtime -> master runtime
        └── worker  -> worker runtime

apps                -> sample applications


DEPENDENCIES
------------
- Python 2.x
- COMPSs Java runtime
- COMPSs C/C++ binding


INSTALLATION
------------

- Execute the install script (requires root privileges):
    ./install

This will install PyCOMPSs in /usr/local/lib/pythonX.Y/site-packages.

- Alternatively, specify an installation directory:
    ./install <install_dir>
