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

# Used for .bashrc export if necessary
EXPORT_LABEL = '##PyCOMPSs_EXPORTS___DO_NOT_REMOVE_THIS_LINE##'
SETUP_ENVIRONMENT_LABEL = '##PyCOMPSs_SETUP___DO_NOT_REMOVE_THIS_LINE##'
# Other labels that can also be used in virtual environment activate
PRE_ENVIRONMENT_LABEL = '##PyCOMPSs_PRE___DO_NOT_REMOVE_THIS_LINE##'
POST_ENVIRONMENT_LABEL = '##PyCOMPSs_POST___DO_NOT_REMOVE_THIS_LINE##'
# Set the names of the functions to call (they must be defined in pycompssenv file)
PRE_ENVIRONMENT_CALL = 'pre_COMPSs_environment ' + PRE_ENVIRONMENT_LABEL + '\n'
SETUP_ENVIRONMENT_CALL = 'setup_COMPSs_environment ' + SETUP_ENVIRONMENT_LABEL + '\n'
POST_ENVIRONMENT_CALL = 'cleanup_COMPSs_environment ' + POST_ENVIRONMENT_LABEL + '\n'


def install(target_path, venv):
    '''This function downloads the COMPSs installable from the specified repository and version
    and installs it. It is intended to be called from the setup.py script when we are installing.
    It also leaves a script on /etc/profile.d/compss.sh equivalent to the "compssenv" one on
    the supercomputers packages.

    Since this script calls another script which calls another script which calls... it may be
    possible that not all errors will be properly caught, leading to incomplete installations.
    :param target_path: <string> Destination folder
    :param venv: <boolean> True if installed within virtual environment. False on the contrary.
    :return: a list of strings (messages)
    '''
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
    
    pref = os.path.join(target_path, 'COMPSs')

    # call the SuperComputers install script
    if subprocess.call(['./COMPSs/install', pref]) != 0:
        raise Exception('COMPSs install script ended with some error. Please check stdout & stderr for more info.')

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

    # Read the pycompssenv file for key replacement
    with open('pycompssenv', 'r') as pycompssenv:
        s = pycompssenv.read()

    # Perform the key replacement
    for (key, value) in list(substitution_map.items()):
        s = s.replace(key, value)

    # Store the content in the appropriate destination
    try:
        # Try as sudo
        with open('/etc/profile.d/compss.sh', 'w') as compss_sh:
            compss_sh.write(s)
        # Include call to setup function (defined in pycompssenv file)
        with open('/etc/profile.d/compss.sh', 'a') as compss_sh:
            compss_sh.write('\n' + SETUP_ENVIRONMENT_CALL + '\n')
    except IOError:
        # Could not include system wide, then try to do it locally

        def update_export(sources_file, target_path, pre_and_post_environment=False):
            '''Helper function for export update
            :param sources_file: Where to place the exports
            :param target_path: where the compss.sh will be
            :param pre_and_post_environment: Boolean to include pre and cleanup (only for virtual environments)
            '''
            # Update compss.sh script
            local_compss_sh = os.path.join(target_path, 'compss.sh')
            with open(local_compss_sh, 'w') as local_compss:
                local_compss.write(s)

            # Set the source where the environment is defined
            exports = 'source ' + str(local_compss_sh) + ' ' + EXPORT_LABEL + '\n'
            messages.append('NOTE! ENVIRONMENT VARIABLES STORED IN %s' % local_compss_sh)
            if EXPORT_LABEL in open(sources_file, 'r').read():
                # Contains the source label, so update all
                with open(sources_file, 'r') as sources_f:
                    file_lines = sources_f.readlines()
                for i in range(len(file_lines)):
                    # Update the existing source line
                    if EXPORT_LABEL in file_lines[i]:
                        file_lines[i] = exports
                    # Update also pre, setup and post if exist (maybe i the future their name is changed)
                    if PRE_ENVIRONMENT_LABEL in file_lines[i]:
                        file_lines[i] = PRE_ENVIRONMENT_CALL
                    if SETUP_ENVIRONMENT_LABEL in file_lines[i]:
                        file_lines[i] = SETUP_ENVIRONMENT_CALL
                    if POST_ENVIRONMENT_LABEL in file_lines[i]:
                        file_lines[i] = POST_ENVIRONMENT_CALL
                # Write everything again
                with open(sources_file, 'w') as sources_f:
                    sources_f.write(''.join(file_lines))
                messages.append('MESSAGE: Updated %s within %s' % (exports, sources_file))
            else:
                # Get all contents
                with open(sources_file, 'r') as sources_f:
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
                    for i in range(len(file_lines)):
                        if 'deactivate' in file_lines[i] and '(' in file_lines[i]:
                            # Line i is the deactivate function definition
                            deactivate_line = i
                    if deactivate_line:
                        # Found the line, then include post. Otherwise not.
                        file_lines.insert(deactivate_line + 1, POST_ENVIRONMENT_CALL)
                file_lines.append(SETUP_ENVIRONMENT_CALL)
                with open(sources_file, 'w') as sources_f:
                    sources_f.write(''.join(file_lines))
                messages.append('MESSAGE: Added %s within %s' % (exports, sources_file))
            messages.append('MESSAGE: Do not forget to source %s' % sources_file)

        if venv:
            # Add export to virtual environment activate
            update_export(os.path.join(os.environ['VIRTUAL_ENV'], 'bin', 'activate'),
                          target_path,
                          pre_and_post_environment=True)
        else:
            # Local installation (.local)
            # Add export to .bashrc
            update_export(os.path.join(os.path.expanduser('~'), '.bashrc'),
                          target_path,
                          pre_and_post_environment=False)

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
        messages.append('SIMBOLIC LINK: From %s to %s' % (symbolic_place, target_file))

    # create symbolic links to the C extensions
    original_extensions_path = os.path.join(pref, 'Bindings', 'python', str(sys.version_info[0]))
    site_packages_path = os.path.split(target_path)[0]
    for target_file in glob.iglob(os.path.join(original_extensions_path, "*.so")):  # just .so files
        symbolic_place = os.path.join(site_packages_path, os.path.split(target_file)[-1])
        create_symlink(target_file, symbolic_place)
        messages.append('SIMBOLIC LINK: From %s to %s' % (symbolic_place, target_file))

    messages.append('*****************************************************')

    return messages
