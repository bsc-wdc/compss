## Quickstart guide

### Manual installation

#### Dependencies

pycompss_interactive_sc currently requires:

* PyCOMPSs == 2.5

#### Installation steps

1. Check which PyCOMPSs version to install.
    * Latest pycompss_interactive_sc release requires **PyCOMPSs 2.5**

2. Install PyCOMPSs following these [instructions](http://compss.bsc.es/releases/compss/latest/docs/COMPSs_Installation_Manual.pdf).

3. Install latest pycompss_interactive_sc version with ``pip3 install pycompss_interactive_sc``.

NOTE: Make sure that you have the python binaries path in your PATH environment variable (e.g. /home/user/.local/bin/).

#### Usage

Once installed, you are ready to use the *interactive_sc* command. This
command allows you to interact with the remote supercomputer.
But first, you have to make sure that COMPSs is loaded automatically when
you log in the supercomputer.

The *interactive_sc* command as a set of options that can be displayed with
```
interactive_sc -h
```

The main options are:
1. **submit** - Submit a new job to the supercomputer.
2. **status** - Check the status of a given job.
3. **connect** - Connect to a given job.
4. **list** - Show all submitted jobs to the supercomputer (and their status).
5. **cancel** - Cancel a job.
6. **template** - Show an example of the requested template.

The specific arguments of each option can be displayed by executing:
```
interactive_sc <option> -h
```

##### Differences between running in Linux and Windows

Although this package is supported in Linux and Windows, there are differences
that need to be taken into account.
For instance, the only requirement in Linux is to have ssh, while in Windows
it is necessary to have Putty available. Moreover, it is necessary to configure
the session in Putty (username and supercomputer).

Due to this difference, the *submit* option usage differs between platforms:

* Linux: Requires to use *--user_name*|*-u*  and *--supercomputer*|*-sc* flags.

* Windows: Requires to use *--session* flag.
