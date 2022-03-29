import setuptools

setuptools.setup(
    # Metadata
    name="pycompss-cli",
    version=open("VERSION.txt").read().strip(),
    description="PyCOMPSs cli",
    long_description=open("README.rst").read(),
    long_description_content_type="text/x-rst",
    author="Workflows and Distributed Computing Group (WDC) - Barcelona Supercomputing Center (BSC)",
    author_email="support-compss@bsc.es",
    url="https://compss.bsc.es",

    # License
    license="Apache 2.0",

    # Build
    include_package_data=True,
    packages=setuptools.find_packages(),
    classifiers=["Development Status :: 5 - Production/Stable",
                 "Environment :: Console",
                 "Intended Audience :: Developers",
                 "Intended Audience :: Science/Research",
                 "License :: OSI Approved :: Apache Software License",
                 "Operating System :: POSIX :: Linux",
                 "Operating System :: Unix",
                 "Operating System :: MacOS",
                 "Programming Language :: Python :: 3 :: Only",
                 "Topic :: Software Development",
                 "Topic :: Scientific/Engineering",
                 "Topic :: System :: Distributed Computing",
                 "Topic :: Utilities"],
    install_requires=["setuptools"],

    # Executable
    entry_points={
        "console_scripts": [
            "compss=pycompss_cli.cli.compss:main",
            "dislib=pycompss_cli.cli.dislib:main",
            "pycompss=pycompss_cli.cli.pycompss:main",
        ],
    }
)
