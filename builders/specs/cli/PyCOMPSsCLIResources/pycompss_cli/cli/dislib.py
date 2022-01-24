#!/usr/bin/env python3

import os
import pycompss_cli.cli.pycompss as pycompss


def main():
    # Force to use the dislib image by default
    os.environ['DEFAULT_DISLIB_DOCKER_IMAGE'] = 'bscwdc/dislib:latest'
    pycompss.main()

if __name__ == "__main__":
    main()
