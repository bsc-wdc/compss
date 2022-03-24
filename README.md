<!-- LOGOS AND HEADER -->
<h1 align="center">
  <br>
  <a href="https://www.bsc.es/">
    <img src="files/logos/bsc_logo.png" alt="Barcelona Supercomputing Center" height="60px">
  </a>
  <a href="https://www.bsc.es/research-and-development/software-and-apps/software-list/comp-superscalar/">
    <img src="files/logos/COMPSs_logo.png" alt="COMP Superscalar" height="60px">
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
    <a href="https://github.com/bsc-wdc/compss/releasess">Releases</a> •
    <a href="https://bit.ly/bsc-wdc-community">Slack</a>
</b></p>

COMP Superscalar (COMPSs) is a programming model which aims to ease the development
of applications for distributed infrastructures, such as Clusters, Grids and Clouds.
COMP Superscalar also features a runtime system that exploits the inherent parallelism
of applications at execution time.


<!-- SECTIONS -->

<!-- DOCUMENTATION -->
# Documentation

COMPSs documentation can be found at the [COMPSs Webpage][1] or at
the `doc/` folder.

  * COMPSs_Installation_Manual.pdf
  * COMPSs_User_Manual_App_Development.pdf
  * COMPSs_User_Manual_App_Execution.pdf
  * COMPSs_Supercomputers_Manual.pdf
  * Tracing_Manual.pdf
  * COMPSs_Developer_Manual.pdf


<!-- PACKAGES -->
# Packages

The COMP Superscalar Framework packages are available at the [COMPSs Webpage][1] or
can be found on the `builders/packages/` directory.


<!-- SOURCES STRUCTURE -->
# Sources Structure

  * **builders**: Packages, scripts for local installations, scripts for supercomputers
   installation and package building scripts
  * **compss** : COMPSs Runtime
  * **dependencies** : COMPSs embeded dependencies
  * **doc** : COMPSs documentation
  * **files** : Dependency files (i.e. paraver configurations)
  * **tests** : COMPSs integration tests
  * **utils** : Misc utils (i.e. OVA scripts, Docker generation, Storage implementations)


<!-- SAMPLE APPLICATIONS -->
# Sample Applications

You can find extended information about COMPSs Sample applications at the
Sample_Applications manual available at the [COMPSs Webpage][1] or at the
`doc/Sample_applications.pdf`.


<!-- BUILDING COMPSS -->
# Building COMPSs

Follow the next steps to build COMPSs in your current machine.

## 1. Install dependencies

Install the listed dependencies for each component you wish to install. For a complete build please install all the dependencies.

* COMPSs Runtime dependencies
  * openjdk-8-jre
  * openssh-server
  * uuid-runtime
  * graphviz
  * xdg-utils
* Build dependencies
  * wget
  * openjdk-8-jdk
  * maven
  * curl
  * jq
  * OSX dependencies(use Brew to install it)
    * libtool
    * coreutils
    * boost
    * util-linux
* Bindings-common dependencies
  * build-essential
  * autoconf
  * automake
  * autotools-dev
  * libtool
* C-binding dependencies
  * libboost-all-dev
  * libxml2-dev
  * csh
* Python-binding dependencies
  * python-dev | python3-dev
  * python-pip | python3-pip
  * python-setuptools | python3-setuptools
  * libpython3
  * wheel
  * numpy
  * dill
  * guppy
* Extrae dependencies
  * libxml2
  * gfortran
  * libpapi-dev
  * papi-tools
* AutoParallel dependencies
  * libgmp3-dev
  * flex
  * bison
  * libbison-dev
  * texinfo
  * libffi-dev
  * astor
  * sympy
  * enum34
  * islpy
* Streaming dependencies
  * Gradle
* Testing dependencies
  * openmpi-bin
  * openmpi-doc
  * libopenmpi-dev
  * bc
  * decorator
  * mpi4py
  * redis-py-cluster
  * enum34
  * tabulate


## 2. Get GIT submodules

Before installing COMPSs you need to download the git submodules that contain its dependencies. To do that execute the following two commands at the root of the repository.

```
./submodules_get.sh
```

## 3. Build COMPSs

**Note**: Remember to install the COMPSs dependencies and to get the GIT submodules before trying to build COMPSs from sources.

* Building COMPSs for all users (not supported in OSX)

```
cd builders/
INSTALL_DIR=/opt/COMPSs/
sudo -E ./buildlocal [options] ${INSTALL_DIR}
```

* Building COMPSs for current user

```
cd builders/

INSTALL_DIR=$HOME/opt/COMPSs/
./buildlocal [options] ${INSTALL_DIR}
```
For OSX:
```
cd builders/
alias libtoolize=/usr/local/bin/glibtoolize
alias readlink=/usr/local/bin/greadlink

export LIBTOOL=`which glibtool`
export LIBTOOLIZE=`which glibtoolize`

INSTALL_DIR=$HOME/opt/COMPSs/
./buildlocal -K -T -M ${INSTALL_DIR}
```


Many COMPSs modules can be activated/deactivated during the build using different options in the `buildlocal` command. You may check the available options by running the following command:

```
cd builders
./buildlocal -h
```

<!-- RUNNING DOCKER TESTS -->
# Running docker tests

## 1. Install Docker and docker-py

Follow these instructions

 - [Docker for Mac](https://store.docker.com/editions/community/docker-ce-desktop-mac). Or, if you prefer to use [Homebrew](https://brew.sh/).
 - [Docker for Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-docker-ce-1).
 - [Docker for Arch Linux](https://wiki.archlinux.org/index.php/Docker#Installation).

Add user to docker group to run docker as non-root user.

 - [Instructions](https://docs.docker.com/install/linux/linux-postinstall/).


## 2. Build the docker image

Run the following command at the root of the project to build the image that will used for testing. The command create an image named **compss** and install the current branch into the image.

```
docker build --target= ci -t compss .
```


## 3. Run the tests

To run the tests inside the docker image use the script found in `./tests/scripts/docker_main`. This command is a wrapper for the `./main` test command
so it has de the syntax and options. For example, you can run the first test without retrials as follows:

```
./docker_main -R -t 1
```

The docker main command creates a new docker container each time you run it (replacing the last one used). It copies the current framework inside it
and runs its tests. **Note**: the testing scripts assumes you have named the testing image `compss`.

**Please be aware that:**

* Code changes affecting the tests sources, config files (e.g. `local.cfg`, and scripts (like `./local`) __will be__ visible inside the newly created container.
* Code changes affecting the installation __will not be__ visible in the installation because framework is not reinstalled. To do that rebuild the docker image as explained in step 3.
* If you run the command once, the container will be available for manual inspection (such as logs). You can log into in issuing `docker exec --user jenkins -it compss_test bash` and use the CLI as usual.
* To delete the created image issue `docker rmi compss`
* To delete the compss_test container use `docker rm -f compss_test`.


<!-- CONTACT -->
# Contact

:envelope: COMPSs Support <support-compss@bsc.es> :envelope:

Workflows and Distributed Computing Group (WDC)

Department of Computer Science (CS)

Barcelona Supercomputing Center (BSC)


<!-- LINKS -->
[1]: http://compss.bsc.es
