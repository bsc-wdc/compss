import setuptools

setuptools.setup(
    # Metadata
    name="pycompss_interactive_sc",
    version="0.1.0",
    description="PyCOMPSs interactive for Supercomputers",
    long_description=open('README.md').read(),
    author='Workflows and Distributed Computing Group (WDC) - Barcelona Supercomputing Center (BSC)',
    author_email='support-compss@bsc.es',
    url='https://compss.bsc.es',

    # License
    license='Apache 2.0',

    # Build
    # packages=setuptools.find_packages(),
    packages=['pycompss_interactive_sc'],
    classifiers=['Development Status :: 5 - Production/Stable',
                 'Environment :: Console',
                 'Intended Audience :: Developers',
                 'Intended Audience :: Science/Research',
                 'License :: OSI Approved :: Apache Software License',
                 'Operating System :: POSIX :: Linux',
                 'Operating System :: Unix',
                 'Operating System :: MacOS',
                 'Operating System :: Microsoft :: Windows',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3.4',
                 'Programming Language :: Python :: 3.5',
                 'Programming Language :: Python :: 3.6',
                 'Topic :: Software Development',
                 'Topic :: Scientific/Engineering',
                 'Topic :: System :: Distributed Computing',
                 'Topic :: Utilities'
    ],
    install_requires=[
    ],

    # Executable
    entry_points={
        'console_scripts': [
            'pycompss_isc = pycompss_interactive_sc.core:main',
        ],
        'gui_scripts': [
        ]
    }
)
