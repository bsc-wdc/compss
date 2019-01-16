<h1 align="center">
  <br>
  <a href="https://www.bsc.es/">
    <img src="doc/logos/bsc_logo.png" alt="Barcelona Supercomputing Center" height="60px">
  </a>
  <a href="https://www.bsc.es/research-and-development/software-and-apps/software-list/comp-superscalar/">
    <img src="doc/logos/COMPSs_logo.png" alt="COMP Superscalar" height="60px">
  </a>
  <br>
  <br>
  COMPSs Framework
  <br>
</h1>

<h3 align="center">Component Superscalar framework and programming model for HPC.</h3>
<p align="center">

  <a href='http://bscgrid05.bsc.es/jenkins/job/COMPSs_Framework-Docker_testing'>
    <img src='http://bscgrid05.bsc.es/jenkins/job/COMPSs_Framework-Docker_testing/badge/icon'
         alt="Build Status">
  </a>

</p>


<p align="center"><b>
    <a href="https://www.bsc.es/research-and-development/software-and-apps/software-list/comp-superscalar/">Website</a> •  
    <a href="https://www.bsc.es/research-and-development/software-and-apps/software-list/comp-superscalar/documentation">Documentation</a> •
    <a href="https://github.com/bsc-wdc/compss/releasess">Releases</a>
</b></p>

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

## Running docker tests 

### 1. Install Docker and docker-py


Follow these instructions

 - [Docker for Mac](https://store.docker.com/editions/community/docker-ce-desktop-mac). Or, if you prefer to use [Homebrew](https://brew.sh/).
 - [Docker for Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce-1).
 - [Docker for Arch Linux](https://wiki.archlinux.org/index.php/Docker#Installation).


Add user to docker group to run docker as non-root user.

 - [Instructions](https://docs.docker.com/install/linux/linux-postinstall/).
    

### 2. Build the docker image 

Run the following command at the root of the project to build the image that will used for testing. The command create an image named **compss** and install the current branch into the image.

```
docker build -t compss .
```

### 3. Run the tests

To run the tests inside the docker image use the script found in `./tests/scripts/docker_main`. This command is a wrapper for the `./main` test command
so it has de the syntax and options. For example, you can run the first test without retrials as follows:
```
./docker_main -R local_1 local.cfg
```
The docker main command creates a new docker container each time you run it (replacing the last one used). It copies the current framework inside it
and runs its tests. **Note**: the testing scripts assumes you have named the testing image `compss`.


**Please be aware that:**
 
* Code changes affecting the tests sources, config files (e.g. `local.cfg`, and scripts (like `./local`) __will be__ visible inside the newly created container.
* Code changes affecting the installation __will not be__ visible in the installation because framework is not reinstalled. To do that rebuild the docker image as explained in step 3.
* If you run the command once, the container will be available for manual inspection (such as logs). You can log into in issuing `docker exec --user jenkins -it compss_test bash` and use the CLI as usual.
* To delete the created image issue `docker rmi compss`
* To delete the compss_test container use `docker rm -f compss_test`.

# Contact

:envelope: COMPSs Support <support-compss@bsc.es> :envelope:

Workflows and Distributed Computing Group (WDC)

Department of Computer Science (CS)

Barcelona Supercomputing Center (BSC) 


[1]: http://compss.bsc.es
