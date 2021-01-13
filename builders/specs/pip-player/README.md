========================================
	pycompss-player PYPI DISTRIBUTABLE
========================================

This README contains information about the PIP distributable creation process.

It is assumed that this README is placed (alongside with buildpipplayer and
the whole pyCOMPSsPlayerResources directory) in framework/builders/specs/pip-player.
If you want to install pycompss-player via distutils and/or PIP there are a
set of depedencies that must be met. Current version requires docker.

It is encouraged to read all sections of this README before proceeding on any
modification or action on this package.


==============================
	CREATING THE DISTRIBUTABLE
==============================

The PyPI distributable can be created with the following command:
	sudo -E ./buildpipplayer $VERSION
Where $VERSION contains the desired target version. For example, if
you want to create a distributable for the 2.8 version then
the following command should be executed:
	sudo -E ./buildpipplayer 2.8

This will create the following files and directories:
	- A pyCOMPSsPlayer directory in /trunk/builders/packages/pip-player.
	  This directory contains the scripts and files necessary to
	  install pyCOMPSsPlayer via distutils and/or PIP.

	- A pycompss-player-2.8.tar.gz file in /trunk/builders/packages/pip-player/pyCOMPSsPlayer/dist
	  This .tar.gz contains the whole pyCOMPSsPlayer directory (except for the dist
		folder). This file is useful for testing. For example, if you type
		 sudo -E python -m pip install pycompss-player-2.8.tar.gz -v
	  pip will try to install pyCOMPSsPlayer from this .tar.gz



================================
	INSTALLING pycompss-player
================================

In order to install pycompss-player from a distributable you must:
	- Have docker >= 17.12.0-ce

A pyCOMPSsPlayer distributable can be installed in any of the following ways:
	- From a .tar.gz distributable. Let's assume you have a
	  pycompss-player-<VERSION>.tar.gz created from the buildpipplayer script.
		Then the following command:
		  sudo python -m pip install pycompss-player-<VERSION>.tar.gz
			or
			python -m pip install pycompss-player --user
	  Will install pyCOMPSsPlayer in your site-packages.

	- From a pyPI repository (this will be clarified later).
	  For example, if you type
		  sudo -E python -m pip install pycompss-player
	  The latest pyCOMPSsPlayer version that was released on the live PyPI repository
		will be downloaded and installed.

	- From the pyCOMPSsPlayer folder. The pyCOMPSsPlayer folder contains the
	  very same files as the dist/pycompss-player-<VERSION>.tar.gz (except for
		the tgz itself!).
	  You can install pycompss-player from this folder with
		  python setup.py install


==================================
	UNINSTALLING pycompss-player
==================================

pycompss-player can be uninstalled (if it was previously installed with distutils
and/or PIP) with the following command:
	sudo -E python -m pip uninstall pycompss-player
  or
	python -m pip uninstall pycompss-player --user


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

A pyCOMPSsPlayerdistributable can be uploaded to a repository and then be
downloadable and installable by anyone with python and pip.

In order to upload a PyPI package the following command must be executed:
  cd builders/packages/pip-player/pyCOMPSsPlayer/dist
	# Check that the package is valid (the result must be PASSED)
	twine check pycompss-player-<version>.tar.gz
	# Upload to the test repository
	twine upload --repository-url https://test.pypi.org/legacy/ pycompss-player-<version>.tar.gz
	# Check that the installation works fine:

	# Upload the definitive package to pypi:
	twine upload pycompss-player-<version>.tar.gz

The login credentials requested by twine are available at the wiki.

WARNING: Be very careful when uploading pyCOMPSsPlayer distributables on pyPI.
pyPI does not allow to re-upload a .tar.gz distributable for the same release.
**Upload only well-tested installables!**


======================================
	CONTAINED FILES
======================================

This folder contains the following folder hierarchy and files:

.
├── buildpipplayer
├── pyCOMPSsPlayerResources
│   ├── pycompss-player
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

buildpipplayer:
	- Main script. sudo -E ./buildpipplayer VERSION will create a PIP distributable
	  named pycompss-player-${PyCOMPSs_VERSION}.tar.gz in
		framework/builders/packages/pip-player/pyCOMPSsPlayer/dist/
	  Example: sudo -E ./buildpipplayer 2.8 will create a pycompss-player-2.8.tar.gz

PyCOMPSsResources/pycompss-player/compss:
	- pycompss alias.

PyCOMPSsResources/pycompss-player/pycompss:
	- Main bash script which enables users to interact with the docker instances.

PyCOMPSsResources/pycompss-player/pycompss_cmd.py:
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
		- Pip installable main script. This installs pyCOMPSsPlayer.

An additional file named VERSION.txt will be created (and NOT deleted) by the
buildpipplayer script.
A manual modification of this file will have no effect on future PIP
distributable builds since it will be automatically replaced by a new one.



======================================
	           GENERATED
      pyCOMPSsPlayer FOLDER
======================================

A succesfull buildpipplayer execution will leave in framework/builders/packages/pip-player
the folder hierarchy and files listed below.
Do not modify, add or delete any file on this folder.

pyCOMPSsPlayer
├── dist
│   └── pycompss-player-<VERSION>.tar.gz
├── LICENSE.txt
├── MANIFEST.in
├── pycompss-player
│   ├── __init__.py
│   ├── compss
│   ├── pycompss
│   └── pycompss_cmd.py
├── pycompss_player.egg-info
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
