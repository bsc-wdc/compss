===========
PyCOMPSs
===========

PyCOMPSs is the Python binding for the COMP Superscalar (COMPSs) framework. It allows to run Python applications with COMPSs.


CONTENT
-------

src
  L ext             -> external C module to interact with the COMPSs C/C++ binding
    L pycompss      -> Python modules
        L api       -> API modules to be imported by the programmer in the application
        L functions -> Helper functions
        L matlib    -> Math library
        L runtime   -> master runtime
        L util      -> utilities
        L worker    -> worker runtime


DEPENDENCIES
------------
- Python 2.x (optionally Python 3.x)
- COMPSs Java runtime
- COMPSs C/C++ binding


INSTALLATION
------------

- Execute the install script (requires root privileges):
    ./install

This will install PyCOMPSs in /usr/local/lib/pythonX.Y/site-packages.

- Alternatively, specify an installation directory:
    ./install <install_dir> <create_symlinks> <only_python_version>

    create_symlinks: Create symbolic links within site-packages (options: true | false)
    only_python_version: Install a specific version (options: python2 | python 3)
