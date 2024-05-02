#!/usr/bin/python
#
#  Copyright 2002-2024 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -*- coding: utf-8 -*-

"""
PyCOMPSs installer.

This script will be called by pip when:
- We want to create a distributable (sdist) tar.gz
- We want to install PyCOMPSs (install)

It can be invoked to do these functionalities with
python3 -m pip (install|build|build --sdist)
"""

import glob
import os
import site
import subprocess
import sys
import tarfile
import urllib.request as urllib
import setuptools


# Used for .bashrc export if necessary
EXPORT_LABEL = "##PyCOMPSs_EXPORTS___DO_NOT_REMOVE_THIS_LINE##"
SETUP_ENVIRONMENT_LABEL = "##PyCOMPSs_SETUP___DO_NOT_REMOVE_THIS_LINE##"
# Other labels that can also be used in virtual environment activate
PRE_ENVIRONMENT_LABEL = "##PyCOMPSs_PRE___DO_NOT_REMOVE_THIS_LINE##"
POST_ENVIRONMENT_LABEL = "##PyCOMPSs_POST___DO_NOT_REMOVE_THIS_LINE##"
# Set the names of the functions to call (they must be defined in pycompssenv file)
PRE_ENVIRONMENT_CALL = "pre_COMPSs_environment " + PRE_ENVIRONMENT_LABEL + "\n"
SETUP_ENVIRONMENT_CALL = "setup_COMPSs_environment " + SETUP_ENVIRONMENT_LABEL + "\n"
POST_ENVIRONMENT_CALL = "cleanup_COMPSs_environment " + POST_ENVIRONMENT_LABEL + "\n"

DECODING_FORMAT = "utf-8"
LINE = "*" * 50


class InstallationException(Exception):
    """Custom installation exception."""

    def __init__(self, message):
        print(f"PyCOMPSs installation Exception: {message}")


def get_virtual_env_target_path():
    """Get the site pacakges and target path within a virtual environment
    :return: site packages path and target path
    """
    from distutils.sysconfig import get_python_lib

    site_packages = get_python_lib()
    return site_packages, os.path.join(site_packages, "pycompss")


def get_root_target_path():
    """Get the target path for root installations
    :return: site packages path and target path
    """
    site_packages = site.getsitepackages()[0]
    return site_packages, os.path.join(site_packages, "pycompss")


def get_user_target_path():
    """Get the target path for user installation
    :return: site packages path and target path
    """
    site_packages = site.getusersitepackages()
    return site_packages, os.path.join(site_packages, "pycompss")


def check_system():
    """Check that we have a proper python version and a proper OS (i.e: not windows)
    Also, check that we have JAVA_HOME defined.
    This does NOT intend to perform an exhaustive system check, and it is neither
    the role nor in the scope of a distutils installer
    """
    # check Python version
    assert sys.version_info[:2] >= (
        3,
        6,
    ), f"COMPSs does not support Python version {sys.version}, only Python >= 3.6.x is supported."

    # check os version not Windows
    assert sys.platform != "win32", "COMPSs does not support Windows"
    assert sys.platform != "cygwin", "COMPSs does not support Windows/Cygwin"
    assert sys.platform != "msys", "COMPSs does not support Windows/MSYS2"

    # check we have JAVA_HOME defined
    assert "JAVA_HOME" in os.environ, "JAVA_HOME is not defined"


def command_runner(cmd):
    """Run the command defined in the cmd list.

    :param cmd: <list[str]> Command to execute as list.
    :returns: <int> Exit code
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()  # blocks until cmd is done
    stdout = stdout.decode(DECODING_FORMAT)
    stderr = stderr.decode(DECODING_FORMAT)
    exit_code = proc.returncode

    print("----------------- STDOUT -----------------", flush=True)
    print(stdout, flush=True)
    print("------------------------------------------", flush=True)
    if stderr:
        print("----------------- STDERR -----------------", file=sys.stderr, flush=True)
        print(stderr, file=sys.stderr, flush=True)
    print("------------------------------------------", flush=True)
    return exit_code


def backend_install(compss_target_path, site_packages_path, venv):
    """This function downloads the COMPSs installable from the specified repository and version
    and installs it. It is intended to be called from the setup.py script when we are installing.
    It also leaves a script on /etc/profile.d/compss.sh equivalent to the "compssenv" one on
    the supercomputers packages.

    Since this script calls another script which calls another script which calls... it may be
    possible that not all errors will be properly caught, leading to incomplete installations.
    :param compss_target_path: <string> Destination folder
    :param site_packages_path: <string> Site packages folder
    :param venv: <boolean> True if installed within virtual environment. False on the contrary.
    :return: a list of strings (messages)
    """
    messages = []

    # download and decompress the COMPSs_version.tar.gz file
    with open("url", "r") as url_fd:
        base_url = url_fd.read().rstrip()
    with open("VERSION.txt", "r") as version_fd:
        version_name = version_fd.read().rstrip()
    tgz_name = f"COMPSs_{version_name}.tar.gz"

    full_url = os.path.join(base_url, tgz_name)

    urllib.urlretrieve(full_url, tgz_name)

    tar = tarfile.open(tgz_name, "r:gz")
    tar.extractall(".")
    tar.close()

    os.remove(tgz_name)

    # ###################################################################### #
    # ######################## INSTALLATION ################################ #
    # ###################################################################### #

    pref = os.path.join(compss_target_path, "COMPSs")

    # call the SuperComputers install script
    if command_runner(["./COMPSs/install", pref]) != 0:
        raise InstallationException(
            "COMPSs install script ended with some error. \
            Please check stdout & stderr for more info."
        )

    messages.append(LINE)

    # ###################################################################### #
    # ################# KEEP TRACK OF FILES TO RECORD ###################### #
    # ###################################################################### #
    # Since we add files during the installation, we could record accordingly
    # to enable the smooth uninstall. Discouraged, but only known way.
    # See note:
    # https://packaging.python.org/en/latest/specifications/recording-installed-packages/#the-record-file

    all_files_to_record = []
    # 1st.- pycompss folder
    pycompss_pkg_target_path = os.path.join(site_packages_path, "pycompss")
    for subdir, _, files in os.walk(pycompss_pkg_target_path):
        for file in files:
            all_files_to_record.append(os.path.join(subdir, file))

    # 2nd.- symbolic links
    # Appended next section
    # 3rd.- compiled modules
    # Appended in next section

    messages.append(LINE)

    # ###################################################################### #
    # ################## SETUP ENVIRONMENT VARIABLES ####################### #
    # ###################################################################### #

    # create a script that defines some environment variables
    # if called as root and pip does not detect pycompss as an installed module
    # it will delete COMPSs

    substitution_map = {
        "##VERSION##": str(sys.version_info[0]) + "." + str(sys.version_info[1]),
        "##SITE_PACKAGES##": site_packages_path,
        "##COMPSS_PATH##": pref,
        "##JAVA_HOME##": os.environ["JAVA_HOME"],
    }

    # Read the pycompssenv file for key replacement
    with open("pycompssenv", "r") as pycompssenv:
        s = pycompssenv.read()

    # Perform the key replacement
    for key, value in list(substitution_map.items()):
        s = s.replace(key, value)

    # Store the content in the appropriate destination
    try:
        # Try as sudo
        profile_script = "/etc/profile.d/compss.sh"
        with open(profile_script, "w") as compss_sh:
            compss_sh.write(s)
        # Include call to setup function (defined in pycompssenv file)
        with open(profile_script, "a") as compss_sh:
            compss_sh.write(f"\n{SETUP_ENVIRONMENT_CALL}\n")
        # Include profile script in RECORD to be uninstalled
        all_files_to_record.append(profile_script)
    except IOError:
        # Could not include system wide, then try to do it locally

        def update_export(
            sources_file, compss_target_path, pre_and_post_environment=False
        ):
            """Helper function for export update
            :param sources_file: Where to place the exports
            :param compss_target_path: where the compss.sh will be
            :param pre_and_post_environment: Boolean to include pre and cleanup (only for virtual environments)
            """
            # Update compss.sh script
            local_compss_sh = os.path.join(compss_target_path, "compss.sh")
            with open(local_compss_sh, "w") as local_compss:
                local_compss.write(s)
            # Include profile script in RECORD to be uninstalled
            all_files_to_record.append(local_compss_sh)

            # Set the source where the environment is defined
            exports = "source " + str(local_compss_sh) + " " + EXPORT_LABEL + "\n"
            messages.append(f"NOTE! ENVIRONMENT VARIABLES STORED IN {local_compss_sh}")
            if EXPORT_LABEL in open(sources_file, "r").read():
                # Contains the source label, so update all
                with open(sources_file, "r") as sources_f:
                    file_lines = sources_f.readlines()
                for i, line in enumerate(file_lines):
                    # Update the existing source line
                    if EXPORT_LABEL in line:
                        file_lines[i] = exports
                    # Update also pre, setup and post if exist (maybe i the future their name is changed)
                    if PRE_ENVIRONMENT_LABEL in line:
                        file_lines[i] = PRE_ENVIRONMENT_CALL
                    if SETUP_ENVIRONMENT_LABEL in line:
                        file_lines[i] = SETUP_ENVIRONMENT_CALL
                    if POST_ENVIRONMENT_LABEL in line:
                        file_lines[i] = POST_ENVIRONMENT_CALL
                # Write everything again
                with open(sources_file, "w") as sources_f:
                    sources_f.write("".join(file_lines))
                messages.append(f"MESSAGE: Updated {exports} within {sources_file}")
            else:
                # Get all contents
                with open(sources_file, "r") as sources_f:
                    file_lines = sources_f.readlines()
                # Append the source line:
                file_lines.append(exports)
                if pre_and_post_environment:
                    # Add pre before setup
                    file_lines.append(PRE_ENVIRONMENT_CALL)
                    # Look for the place where to put the post (cleanup)
                    # In virtual environments activate script there is a function called deactivate that is used
                    # to clean the environment and exit from the virtual environment. So, place it there
                    deactivate_line = None
                    # for i in range(len(file_lines)):
                    for i, line in enumerate(file_lines):
                        if "deactivate" in line and "(" in line:
                            # Line i is the deactivate function definition
                            deactivate_line = i
                    if deactivate_line:
                        # Found the line, then include post. Otherwise not.
                        file_lines.insert(deactivate_line + 1, POST_ENVIRONMENT_CALL)
                file_lines.append(SETUP_ENVIRONMENT_CALL)
                with open(sources_file, "w") as sources_f:
                    sources_f.write("".join(file_lines))
                messages.append(f"MESSAGE: Added {exports} within {sources_file}")
            messages.append(f"MESSAGE: Do not forget to source {sources_file}")

        if venv:
            # Add export to virtual environment activate
            update_export(
                os.path.join(os.environ["VIRTUAL_ENV"], "bin", "activate"),
                compss_target_path,
                pre_and_post_environment=True,
            )
        else:
            # Local installation (.local)
            # Add export to .bashrc
            update_export(
                os.path.join(os.path.expanduser("~"), ".bashrc"),
                compss_target_path,
                pre_and_post_environment=False,
            )

    messages.append(LINE)

    # ###################################################################### #
    # ###################### SETUP SYMBOLIC LINKS ########################## #
    # ###################################################################### #

    # create symbolic links
    def create_symlink(original_file, symbolic_place):
        try:
            os.remove(symbolic_place)
        except:
            pass
        os.symlink(original_file, symbolic_place)

    # create symbolic links to the python package contents
    original_compss_path = os.path.join(
        pref, "Bindings", "python", str(sys.version_info[0]), "pycompss"
    )
    for target_file in glob.iglob(os.path.join(original_compss_path, "*")):
        symbolic_place = os.path.join(
            compss_target_path, os.path.split(target_file)[-1]
        )
        create_symlink(target_file, symbolic_place)
        messages.append(f"SYMBOLIC LINK: From {symbolic_place} to {target_file}")
        # Include symlinks in RECORD to be uninstalled
        all_files_to_record.append(symbolic_place)

    # create symbolic links to the C extensions
    original_extensions_path = os.path.join(
        pref, "Bindings", "python", str(sys.version_info[0])
    )
    for target_file in glob.iglob(
        os.path.join(original_extensions_path, "*.so")
    ):  # just .so files
        symbolic_place = os.path.join(
            site_packages_path, os.path.split(target_file)[-1]
        )
        create_symlink(target_file, symbolic_place)
        messages.append(f"SYMBOLIC LINK: From {symbolic_place} to {target_file}")
        # Include symlinks in RECORD to be uninstalled
        all_files_to_record.append(symbolic_place)

    messages.append(LINE)

    return messages


def main():
    """Pre-install operation: download and install COMPSs.

    This will try to stop the installation if some error is
    found during this part. However, some sub-scripts do not
    propagate the errors they find, so there is not absolute
    guarantee that this script will lead to a perfect, clean
    installation.
    """
    venv = False
    if "VIRTUAL_ENV" in os.environ:
        # We are within a virtual environment
        # This is more legit than within the exception
        venv = True
        site_packages_path, target_path = get_virtual_env_target_path()
        print(f"Installing within virtual environment in: {target_path}")
    else:
        try:
            if os.getuid() == 0:
                # Installing as root
                site_packages_path, target_path = get_root_target_path()
                print(f"Installing as root in: {target_path}")
            else:
                # Installing as user
                site_packages_path, target_path = get_user_target_path()
                print(f"Installing as user in: {target_path}")
        except AttributeError:
            # This exception can be raised within virtual environments due to a bug
            # with the site module.
            venv = True
            site_packages_path, target_path = get_virtual_env_target_path()
            print(f"Installing within virtual environment in: {target_path}")

    messages = []
    # Caution: bdist_wheel should not be in the following if
    # since it is used for building wheels during build
    # process. But it is used during pip install.
    # if 'install' in sys.argv or 'bdist_wheel' in sys.argv:
    if "egg_info" in sys.argv:
        # First step during installation
        check_system()

    if "bdist_wheel" in sys.argv:
        messages = backend_install(target_path, site_packages_path, venv)

    # This is the only required line if only using pyproject.toml
    setuptools.setup()  # This uses pyproject.toml

    # Show final messages
    for message in messages:
        print(message)


if __name__ == "__main__":
    main()
