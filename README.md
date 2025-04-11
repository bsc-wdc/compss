<!-- LOGOS AND HEADER -->
<h1 align="center">
  <br>
  <a href="https://www.bsc.es/">
    <img src="files/logos/bsc_logo.png" alt="Barcelona Supercomputing Center" height="60px">
  </a>
  <a href="https://www.bsc.es/research-and-development/software-and-apps/software-list/comp-superscalar/">
    <img src="files/logos/COMPSs_logo.png" alt="COMP Superscalar" height="60px">
  </a>
  COMPSs/PyCOMPSs Framework
</h1>

<p align="center">
  <a href='https://eu.badgr.com/public/assertions/DyM-w_V-SEKU64D90AsrEA'>
  <img src='https://api.eu.badgr.io/public/assertions/DyM-w_V-SEKU64D90AsrEA/image' alt="SQAaaS silver badge achieved" height="75px"/>
  </a>
  <a href='https://compss.bsc.es/jenkins/job/COMPSs_Framework_Create_Release/'>
  <img src='https://compss.bsc.es/jenkins/buildStatus/icon?job=COMPSs_Framework_Create_Release' alt="Build Status">
  </a>
  <a href='https://compss-doc.readthedocs.io/en/stable/?badge=stable'>
  <img src='https://readthedocs.org/projects/compss-doc/badge/?version=stable' alt='Documentation Status' />
  </a>
  <a href="https://doi.org/10.5281/zenodo.6362594">
  <img src="https://zenodo.org/badge/DOI/10.5281/zenodo.6362594.svg" alt="DOI">
  </a>
  <a href='https://opensource.org/licenses/Apache-2.0'>
  <img src='https://img.shields.io/badge/License-Apache_2.0-blue.svg' alt='License'/>
  </a>
</p>

<p align="center"><b>
    <a href="https://www.bsc.es/research-and-development/software-and-apps/software-list/comp-superscalar/">Website</a> •  
    <a href="https://compss-doc.readthedocs.io/en/latest/">Documentation</a> •
    <a href="https://github.com/bsc-wdc/compss/releasess">Releases</a> •
    <a href="https://bit.ly/bsc-wdc-community">Slack</a> •
    <a href="mailto:support-compss@bsc.es">&#9993</a>
</b></p>

COMP Superscalar (COMPSs) is a programming model which aims to ease the development
of applications for distributed infrastructures, such as Clusters, Grids and Clouds.
COMP Superscalar also features a runtime system that exploits the inherent parallelism
of applications at execution time.

<!-- SOURCES STRUCTURE -->
## Repository Structure

  * **builders**: Packages, scripts for local installations, scripts for supercomputers
   installation and package building scripts
  * **compss** : Programming Model, Bindings and Runtime source code
  * **dependencies** : Embeded dependencies
  * **files** : Dependency files (i.e. paraver configurations)
  * **tests** : Integration tests
  * **utils** : Misc utils (i.e. OVA scripts, Docker generation, Storage implementations)


## Supported Systems

COMPSs/PyCOMPSs fully supports Linux systems for x86_64, amd64, ppc64, arm64 and riscv64 architectures. macOS systems are also supported with some limitations. 


<!-- BUILDING COMPSS -->
## Building From Sources

Follow the next steps to build COMPSs in your current machine.

### 1. Install dependencies

For an updated list of COMPSs dependencies and how to install them for different systems visit the [dependencies section](https://compss-doc.readthedocs.io/en/latest/Sections/01_Installation/01_Dependencies.html) of the COMPSs documentation website.


### 2. Get GIT submodules

Before installing COMPSs you need to download the git submodules that contain its dependencies. To do that execute the following two commands at the root of the repository.

```
./submodules_get.sh
```

### 3. Build COMPSs

**Note**: Remember to install the COMPSs dependencies and to get the GIT submodules before trying to build COMPSs from sources.

* Building COMPSs for all users (not supported in macOS)

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
For macOS visit the [Buidling from sources section](https://compss-doc.readthedocs.io/en/latest/Sections/01_Installation/02_Building_from_sources.html) of the COMPSs documentation website.


Many COMPSs modules can be activated/deactivated during the build using different options in the `buildlocal` command. You may check the available options by running the following command:

```
cd builders
./buildlocal -h
```

<!-- RUNNING DOCKER TESTS -->
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
docker build --target=ci -t compss .
```


### 3. Run the tests

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

### 4. Run the tests locally on macOS

In order to run Jenkins tests locally on macOS, GNU sed (gsed) is needed. To install it, use:

```
brew install gsed
```

This will install gsed at `/opt/homebrew/bin/gsed`.

Finally, the `NIO_mac.cfg` file needs to be updated with any specific features of the local macOS environment, commonly variables such as the `java_home`, `compss_home` and `runcompss_opts` parameters should be updated.

<!-- CONTACT -->
## Support
For support please send and e-mail to support-compss@bsc.es

(c) Workflows and Distributed Computing Group (WDC) - Department of Computer Science (CS) - Barcelona Supercomputing Center (BSC)



