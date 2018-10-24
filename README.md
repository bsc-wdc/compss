# COMP SUPERSCALAR FRAMEWORK

COMP Superscalar (COMPSs) is a programming model which aims to ease the development
of applications for distributed infrastructures, such as Clusters, Grids and Clouds.
COMP Superscalar also features a runtime system that exploits the inherent parallelism
of applications at execution time.


## Documentation

COMPSs documentation can be found at the [COMPSs Webpage][1] or at 
the `doc/` folder.

  * COMPSs_Installation_Manual.pdf
  * COMPSs_User_Manual_App_Development.pdf
  * COMPSs_User_Manual_App_Execution.pdf
  * COMPSs_Supercomputers_Manual.pdf
  * Tracing_Manual.pdf
  * COMPSs_Developer_Manual.pdf


## Packages

The COMP Superscalar Framework packages are available at the [COMPSs Webpage][1] or 
can be found on the `builders/packages/` directory.


## Sources Structure

  * **builders**: Packages, scripts for local installations, scripts for supercomputers
   installation and package building scripts
  * **compss** : COMPSs Runtime
  * **dependencies** : COMPSs embeded dependencies
  * **doc** : COMPSs documentation
  * **files** : Dependency files (i.e. paraver configurations)

## Sample Applications

You can find extended information about COMPSs Sample applications at the 
Sample_Applications manual available at the [COMPSs Webpage][1] or at the 
`doc/Sample_applications.pdf`.


## Building COMPSs

* COMPSs Dependencies:
        * openjdk-8-jre
        * openjdk-8-jdk
        * graphviz
        * xdg-utils
        * libxml2
        * libxml2-dev
        * python (>=2.7)
        * libpython2.7
        * build-essential
        * autoconf
        * automake
        * autotools-dev
        * libtool
        * libboost-serialization-dev
        * libboost-iostreams-dev
        * gfortran

* Building dependencies
	* wget
	* maven (3.0.x version)

* Getting submodule dependencies:

    Before installing COMPSs you need to download the git submodules that contain its dependencies. To do that execute the following two commands which are located at the root of the repo.

```
./submodules_get.sh
./submodules_patch.sh
```

* Building COMPSs for all users

**Note**: you need to get COMPSs dependencies before installing. See previous section 'Getting submodule dependencies'

```
cd builders/
INSTALL_DIR=/opt/COMPSs/
sudo -E ./buildlocal [options] ${INSTALL_DIR}
```

* Building COMPSs for current user

**Note**: you need to get COMPSs dependencies before installing. See previous section 'Getting submodule dependencies'

```
cd builders/
INSTALL_DIR=$HOME/opt/COMPSs/
./buildlocal [options] ${INSTALL_DIR}
```

# Contact

:envelope: COMPSs Support <support-compss@bsc.es> :envelope:

Workflows and Distributed Computing Group (WDC)

Department of Computer Science (CS)

Barcelona Supercomputing Center (BSC) 


[1]: http://compss.bsc.es
