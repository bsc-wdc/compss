import site
import sys
import os
from backend import install as backend_install
from setuptools import setup

'''
Setuptools installer. This script will be called by pip when:
- We want to create a distributable (sdist) tar.gz
- We want to build the C extension (build and build_ext)
- We want to install pyCOMPSs (install)

It can be invoked to do these functionalities with
python setup.py (install|sdist|build)
'''

bindings_location = os.path.join('COMPSs', 'Bindings')
venv = False

try:
    if os.getuid() == 0:
        # Installing as root
        target_path = os.path.join(site.getsitepackages()[0], 'pycompss')
        print('Installing as root in: ' + str(target_path))
    else:
        # Installing as user
        target_path = os.path.join(site.getusersitepackages(), 'pycompss')
        print('Installing as user in: ' + str(target_path))
except AttributeError:
    # we are within a virtual environment
    venv = True
    from distutils.sysconfig import get_python_lib
    target_path = os.path.join(get_python_lib(), 'pycompss')
    print('Installing within virtuel environment in: ' + str(target_path))


def check_system():
    '''
    Check that we have a proper python version and a proper OS (i.e: not windows)
    Also, check that we have JAVA_HOME defined
    '''
    # check Python version
    assert sys.version_info[:2] >= (2, 7), 'COMPSs does not support Python version %s, only Python >= 2.7.x is supported.'%sys.version
    # check os version
    assert 'win' not in sys.platform, 'COMPSs does not support Windows'
    # check we have JAVA_HOME defined
    assert 'JAVA_HOME' in os.environ, 'JAVA_HOME is not defined'


'''
Pre-install operation: download and install COMPSs
This will try to stop the installation if some error is
found during that part. However, some sub-scripts do not
propagate the errors they find, so there is not absolute
guarantee that this script will lead to a perfect, clean
installation.
'''
messages = []
if "install" in sys.argv or "bdist_wheel" in sys.argv:
    check_system()
    messages = backend_install(target_path, venv)


'''
Setup function.
'''
setup(name='pycompss',
      version=open('VERSION.txt').read().rstrip(),
      description='Python Binding for COMP Superscalar Runtime',
      long_description=open('README.txt').read(),
      author='The COMPSs team',
      author_email='support-compss@bsc.es',
      maintainer='The COMPSs team',
      maintainer_email='support-compss@bsc.es',
      url='http://compss.bsc.es',
      classifiers=['Development Status :: 5 - Production/Stable',
                   'Environment :: Console',
                   'Intended Audience :: Developers',
                   'Intended Audience :: Science/Research',
                   'License :: OSI Approved :: Apache Software License',
                   'Operating System :: POSIX :: Linux',
                   'Operating System :: Unix',
                   'Programming Language :: C',
                   'Programming Language :: C++',
                   'Programming Language :: Java',
                   'Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5',
                   'Programming Language :: Python :: 3.6',
                   'Topic :: Software Development',
                   'Topic :: Scientific/Engineering',
                   'Topic :: System :: Distributed Computing',
                   'Topic :: Utilities'],
      install_requires=['setuptools'],
      license='Apache 2.0',
      platforms=['Linux']
      )

'''
Show final messages
'''
for message in messages:
    print(message)