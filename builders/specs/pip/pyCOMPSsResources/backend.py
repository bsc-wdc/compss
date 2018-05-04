import os
import sys
import site
import glob
import tarfile
import subprocess

if sys.version_info >= (3, 0):
    import urllib.request as urllib
else:
    import urllib


def install(target_path, venv):
    """
    This function downloads the COMPSs installable from the specified repository and version
    and installs it. It is intended to be called from the setup.py script when we are installing.
    It also leaves a script on /etc/profile.d/compss.sh equivalent to the "compssenv" one on
    the supercomputers packages.

    Since this script calls another script which calls another script which calls... it may be
    possible that not all errors will be properly caught, leading to incomplete installations.
    :param target_path: <string> Destination folder
    :param venv: <boolean> True if installed within virtual environment. False on the contrary.
    :return: a list of strings (messages)
    """
    messages = []

    # download and decompress the COMPSs_version.tar.gz file
    base_url = open('url', 'r').read().rstrip()
    version_name = open('VERSION.txt', 'r').read().rstrip()
    tgz_name = 'COMPSs_{0}.tar.gz'.format(version_name)

    full_url = os.path.join(base_url, tgz_name)

    urllib.urlretrieve(full_url, tgz_name)

    tar = tarfile.open(tgz_name, 'r:gz')
    tar.extractall('.')

    os.remove(tgz_name)

    # ###################################################################### #
    # ######################## INSTALLATION ################################ #
    # ###################################################################### #

    # call the SuperComputers install script
    if subprocess.call(['./COMPSs/install', target_path]) != 0:
        raise Exception('COMPSs install script ended with some error. Please check stdout & stderr for more info.')

    pref = os.path.join(target_path, 'COMPSs')

    messages.append('*****************************************************')

    # ###################################################################### #
    # ################## SETUP ENVIRONMENT VARIABLES ####################### #
    # ###################################################################### #

    # create a script that defines some environment variables
    # if called as root and pip does not detect pycompss as an installed module
    # it will delete COMPSs
    try:
        site_packages = site.getsitepackages()[0]
    except AttributeError:
        # within a virtual environment
        from distutils.sysconfig import get_python_lib
        site_packages = get_python_lib()
    substitution_map = {
        '##VERSION##': str(sys.version_info[0])+'.'+str(sys.version_info[1]),
        '##SITE_PACKAGES##': site_packages,
        '##COMPSS_PATH##': pref,
        '##JAVA_HOME##': os.environ['JAVA_HOME']
    }
    s = open('pycompssenv', 'r').read()
    for (key, value) in list(substitution_map.items()):
        s = s.replace(key, value)
    try:
        # Try as sudo
        open('/etc/profile.d/compss.sh', 'w').write(s)
    except IOError:
        # Could not include system wide, then try to do it locally

        def update_export(sources_file, target_path):
            local_compss_sh = os.path.join(target_path, 'compss.sh')
            open(local_compss_sh, 'w').write(s)
            exports = 'source ' + str(local_compss_sh) + ' ##PyCOMPSs_EXPORTS##'
            messages.append('NOTE! ENVIRONMENT VARIABLES STORED IN %s' % local_compss_sh)
            if '##PyCOMPSs_EXPORTS##' in open(sources_file, 'r').read():
                # Update the existing line
                file_lines = open(sources_file, 'r').readlines()
                for i in range(len(file_lines)):
                    if '##PyCOMPSs_EXPORTS##' in file_lines[i]:
                        file_lines[i] = exports + '\n'
                open(sources_file, 'w').write(''.join(file_lines))
                messages.append('MESSAGE: Updated %s within %s' % (exports, sources_file))
            else:
                # Append the line:
                open(sources_file, 'a').write(exports)
                messages.append('MESSAGE: Added %s within %s' % (exports, sources_file))
                # TODO: INCLUDE ENVIRONMENT MANAGEMENT IF VENV WITHIN ACTIVATE
            messages.append('MESSAGE: Do not forget to source %s' % sources_file)

        if venv:
            # Add export to virtual environment activate
            update_export(os.path.join(os.environ['VIRTUAL_ENV'], 'bin', 'activate'), target_path)
        else:
            # Add export to .bashrc
            update_export(os.path.join(os.path.expanduser('~'), '.bashrc'), target_path)

    messages.append('*****************************************************')

    # ###################################################################### #
    # ###################### SETUP SYMBOLIK LINKS ########################## #
    # ###################################################################### #

    # create symbolic links
    def create_symlink(original_file, symbolic_place):
        try:
            os.remove(symbolic_place)
        except:
            pass
        os.symlink(original_file, symbolic_place)

    # create symbolic links to the python package contents
    original_compss_path = os.path.join(pref, 'Bindings', 'python', str(sys.version_info[0]), 'pycompss')
    for target_file in glob.iglob(os.path.join(original_compss_path, "*")):
        symbolic_place = os.path.join(target_path, os.path.split(target_file)[-1])
        create_symlink(target_file, symbolic_place)
        messages.append("SIMBOLIK LINK: From %s to %s" % (symbolic_place, target_file))

    # create symbolic links to the C extensions
    original_extensions_path = os.path.join(pref, 'Bindings', 'python', str(sys.version_info[0]))
    site_packages_path = os.path.split(target_path)[0]
    for target_file in glob.iglob(os.path.join(original_extensions_path, "*.so")):  # just .so files
        symbolic_place = os.path.join(site_packages_path, os.path.split(target_file)[-1])
        create_symlink(target_file, symbolic_place)
        messages.append("SIMBOLIK LINK: From %s to %s" % (symbolic_place, target_file))

    messages.append('*****************************************************')

    return messages