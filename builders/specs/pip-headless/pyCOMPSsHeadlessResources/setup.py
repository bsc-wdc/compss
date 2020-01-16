import setuptools

setuptools.setup(
    # Metadata
    name="pycompss-headless",
    version=open('VERSION.txt').read().strip(),
    description="PyCOMPSs headless",
    long_description=open('README.md').read(),
    author='Workflows and Distributed Computing Group (WDC) - Barcelona Supercomputing Center (BSC)',
    author_email='support-compss@bsc.es',
    url='https://compss.bsc.es',

    # License
    license='Apache 2.0',

    # Build
    packages=setuptools.find_packages(),
    classifiers=['Development Status :: 5 - Production/Stable',
                 'Environment :: Console',
                 'Intended Audience :: Developers',
                 'Intended Audience :: Science/Research',
                 'License :: OSI Approved :: Apache Software License',
                 'Operating System :: POSIX :: Linux',
                 'Operating System :: Unix',
                 'Operating System :: MacOS',
                 "Programming Language :: Python :: 3 :: Only",
                 'Topic :: Software Development',
                 'Topic :: Scientific/Engineering',
                 'Topic :: System :: Distributed Computing',
                 'Topic :: Utilities'],
    install_requires=['setuptools'],

    # Executable
    scripts=["pycompss-headless/pycompss", "pycompss-headless/pycompss_cmd.py"],
)
