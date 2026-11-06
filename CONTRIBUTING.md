# Contributing to COMPSs

Welcome! We are keen to have you on board and look forward to working with you to improve COMPSs.


## Table of Contents
- [Ways to Contribute](#ways-to-contribute)
- [Getting Started](#getting-started)
- [Reporting Bugs and Proposing Enhancements](#reporting-bugs-and-proposing-enhancements)
- [Merge Request Process](#merge-request-process)
- [Licensing](#licensing)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)


## Ways to Contribute

We welcome contributions of all kinds:
- Reporting bugs and suggesting features.
- Improving documentation.
- Triaging issues.
- Fixing bugs or adding new functionality.


## Getting Started

Before starting, please ensure you have reviewed the **Dependencies** section in our official documentation.

Depending on your environment, you can set up COMPSs in several ways:

1. **Via pip:** The fastest way for local development and testing.
2. **Building from sources:** Recommended if you are modifying the core framework or need specific optimizations.
3. **Supercomputers/HPC:** If you are contributing specifically for HPC environments, please follow the specialized installation guide for Clusters/Clouds.

For detailed step-by-step instructions, please visit the [Installation and Configuration Guide](https://docs.compss.io/en/latest/installation/).


## Reporting Bugs and Proposing Enhancements

Before making a change, please discuss it with the owners of this repository via an issue, email, or other communication channels. 

**Specifically, any change that touches more than 3 files or adds a new dependency MUST be discussed via an issue first.**

- **Bugs:** Please use the bug report template to provide a reproducible example.
- **Enhancements:** Open an issue to describe the proposed change and the reasoning behind it.


## Merge Request Process

Once your change is discussed and approved, please follow these steps:

1. **Use the Template:** When creating your Merge Request (MR), please select the official template from the dropdown menu to ensure all required information is provided.
2. **Build Cleanliness:** Ensure any install or build dependencies are removed before the end of the layer when doing a build.
3. **Documentation:** Update the `README.md` with details of changes to the interface (including new environment variables, exposed ports, useful file locations, and container parameters).
4. **Versioning (SemVer):** Increase the version numbers in any example files and the `README.md`. We use [SemVer](http://semver.org/). If your change is a **Breaking Change**, ensure the Major version is incremented.
5. **Review & Merge:** You may merge the MR once you have the sign-off of two other developers. If you do not have merge permissions, you may request the second reviewer to merge it for you.


## Licensing

This project is licensed under the **Apache License, Version 2.0**. 

By contributing to this project, you agree that your contributions will be licensed under the same terms. Please keep in mind:
- **License Headers:** Every new source file you create must include the appropriate Apache 2.0 license header at the top.
- **NOTICE file:** If your contribution includes third-party libraries, please notify us so we can update the `NOTICE` file accordingly.


## Code of Conduct

Please follow our [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) (Contributor Covenant 3.0) in all your interactions with the project.


## Security

If you discover a security vulnerability, please do not open a public issue. Instead, report it privately to support-compss@bsc.es.
