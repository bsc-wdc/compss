%define name	 	compss-tools
%define version		2.4
%define release		1

Requires: compss-monitor, compss-extrae, compss-storage, compss-autoparallel
Summary: The BSC COMP Superscalar Runtime
Name: %{name}
Version: %{version}
Release: %{release}
License: Apache 2.0
Group: Development/Libraries
Source: %{name}-%{version}.tar.gz
Distribution: Linux
Vendor: Barcelona Supercomputing Center - Centro Nacional de Supercomputación
URL: http://compss.bsc.es
Packager: COMPSs Support <support-compss@bsc.es>
Prefix: /opt
ExclusiveArch: x86_64

%description
The BSC COMP Superscalar Tools for COMPSs Runtime.

%prep

#------------------------------------------------------------------------------------
%build

#------------------------------------------------------------------------------------
%install

#------------------------------------------------------------------------------------
%post

#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun

#------------------------------------------------------------------------------------
%clean

#------------------------------------------------------------------------------------
%files
