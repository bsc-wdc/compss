========================================
	pycompss-cli PYPI DISTRIBUTABLE
========================================

This README contains information about the PIP distributable creation process.

It is assumed that this README is placed (alongside with buildpipcli and
the whole PyCOMPSsCLIResources directory) in framework/builders/specs/pip-cli.
If you want to install pycompss-cli via distutils and/or PIP there are a
set of depedencies that must be met. Current version requires docker.

It is encouraged to read all sections of this README before proceeding on any
modification or action on this package.


==============================
	CREATING THE DISTRIBUTABLE
==============================

The PyPI distributable can be created with the following command:
	sudo -E ./buildpipcli $VERSION
Where $VERSION contains the desired target version. For example, if
you want to create a distributable for the 2.10 version then
the following command should be executed:
	sudo -E ./buildpipcli 2.10

This will create the following files and directories:
	- A PyCOMPSsCLI directory in /trunk/builders/packages/pip-cli.
	  This directory contains the scripts and files necessary to
	  install PyCOMPSsCLI via distutils and/or PIP.

	- A pycompss-cli-2.10.tar.gz file in /trunk/builders/packages/pip-cli/PyCOMPSsCLI/dist
	  This .tar.gz contains the whole PyCOMPSsCLI directory (except for the dist
		folder). This file is useful for testing. For example, if you type
		 sudo -E python -m pip install pycompss-cli-2.10.tar.gz -v
	  pip will try to install PyCOMPSsCLI from this .tar.gz



================================
	INSTALLING pycompss-cli
================================

In order to install pycompss-cli from a distributable you must:
	- Have docker >= 17.12.0-ce

A PyCOMPSsCLI distributable can be installed in any of the following ways:
	- From a .tar.gz distributable. Let's assume you have a
	  pycompss-cli-<VERSION>.tar.gz created from the buildpipcli script.
		Then the following command:
		  sudo python -m pip install pycompss-cli-<VERSION>.tar.gz
			or
			python -m pip install pycompss-cli --user
	  Will install PyCOMPSsCLI in your site-packages.

	- From a pyPI repository (this will be clarified later).
	  For example, if you type
		  sudo -E python -m pip install pycompss-cli
	  The latest PyCOMPSsCLI version that was released on the live PyPI repository
		will be downloaded and installed.

	- From the PyCOMPSsCLI folder. The PyCOMPSsCLI folder contains the
	  very same files as the dist/pycompss-cli-<VERSION>.tar.gz (except for
		the tgz itself!).
	  You can install pycompss-cli from this folder with
		  python setup.py install


==================================
	UNINSTALLING pycompss-cli
==================================

pycompss-cli can be uninstalled (if it was previously installed with distutils
and/or PIP) with the following command:
	sudo -E python -m pip uninstall pycompss-cli
  or
	python -m pip uninstall pycompss-cli --user


=============================
	UPLOADING A DISTRIBUTABLE
=============================

*** PLEASE, READ THIS SECTION CAREFULLY ***

It is recommended to read this guide about PIP packages and it is encouraged to
have some experience with it (for example, create small, dummy packages and
upload them to the test repository):
	https://packaging.python.org/tutorials/packaging-projects/

And take a look at twine (and install it if necessary):
  https://pypi.org/project/twine/

A PyCOMPSsCLIdistributable can be uploaded to a repository and then be
downloadable and installable by anyone with python and pip.

In order to upload a PyPI package the following command must be executed:
  cd builders/packages/pip-cli/PyCOMPSsCLI/dist
	# Check that the package is valid (the result must be PASSED)
	twine check pycompss-cli-<version>.tar.gz
	# Upload to the test repository
	twine upload --repository-url https://test.pypi.org/simple/ pycompss-cli-<version>.tar.gz
	# Check that the installation works fine:

	# Upload the definitive package to pypi:
	twine upload pycompss-cli-<version>.tar.gz

The login credentials requested by twine are available at the wiki.

WARNING: Be very careful when uploading PyCOMPSsCLI distributables on pyPI.
pyPI does not allow to re-upload a .tar.gz distributable for the same release.
**Upload only well-tested installables!**


======================================
	CONTAINED FILES
======================================

This folder contains the following folder hierarchy and files:

.
├── buildpipcli
├── PyCOMPSsCLIResources
│   ├── pycompss-cli
│   |   ├── __init__.py
│   |   ├── compss
│   |   ├── pycompss
|   |   └── pycompss_cmd.py
│   ├── CHANGELOG.md
│   ├── LICENSE.txt
│   ├── MANIFEST.in
│   ├── README.rst
│   ├── requirements.txt
│   ├── setup.py
│   └── VERSION.txt
└── README


README:
	- This file.

buildpipcli:
	- Main script. sudo -E ./buildpipcli VERSION will create a PIP distributable
	  named pycompss-cli-${PyCOMPSs_VERSION}.tar.gz in
		framework/builders/packages/pip-cli/PyCOMPSsCLI/dist/
	  Example: sudo -E ./buildpipcli 2.10 will create a pycompss-cli-2.10.tar.gz

PyCOMPSsResources/pycompss-cli/compss:
	- pycompss alias.

PyCOMPSsResources/pycompss-cli/pycompss:
	- Main bash script which enables users to interact with the docker instances.

PyCOMPSsResources/pycompss-cli/pycompss_cmd.py:
	- Auxiliary script which implements the functionalities offered by the
	  "pycompss" script.

PyCOMPSsResources/CHANGELOG.md:
	- Changes history track.

PyCOMPSsResources/LICENSE.txt:
	- Package licences.

PyCOMPSsResources/MANIFEST.in:
	- A distutils manifest. This will determine which files will be included in
	  the Python distributables.
		See https://docs.python.org/3/distutils/commandref.html
	  for more clarification on how this file works.

PyCOMPSsResources/README.rst:
	- An user-oriented README file.

PyCOMPSsResources/requirements.txt:
	- Required dependencies.

PyCOMPSsResources/setup.py:
		- Pip installable main script. This installs PyCOMPSsCLI.

An additional file named VERSION.txt will be created (and NOT deleted) by the
buildpipcli script.
A manual modification of this file will have no effect on future PIP
distributable builds since it will be automatically replaced by a new one.



======================================
	           GENERATED
      PyCOMPSsCLI FOLDER
======================================

A succesfull buildpipcli execution will leave in framework/builders/packages/pip-cli
the folder hierarchy and files listed below.
Do not modify, add or delete any file on this folder.

PyCOMPSsCLI
├── dist
│   └── pycompss-cli-<VERSION>.tar.gz
├── LICENSE.txt
├── MANIFEST.in
├── pycompss-cli
│   ├── __init__.py
│   ├── compss
│   ├── pycompss
│   └── pycompss_cmd.py
├── pycompss_cli.egg-info
│   ├── dependency_links.txt
│   ├── PKG-INFO
│   ├── requires.txt
│   ├── SOURCES.txt
│   └── top_level.txt
├── README.rst
├── requirements.txt
├── setup.py
└── VERSION.txt


**********************************************
** Workflows and Distributed Computing Team **
** Department of Computer Science           **
** Barcelona Supercomputing Center          **
**********************************************
