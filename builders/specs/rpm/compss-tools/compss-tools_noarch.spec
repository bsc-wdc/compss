%define name	 	compss-tools 
%define version		2.1.rc1707
%define release		1

Requires: compss-monitor, compss-extrae
Summary: The BSC COMP Superscalar Runtime
Name: %{name}
Version: %{version}
Release: %{release}
License: Apache 2.0
Group: Development/Libraries
Source: %{name}-%{version}.tar.gz
Distribution: Linux
Vendor: Barcelona Supercomputing Center (BSC)
URL: http://compss.bsc.es
Packager: COMPSs Support <support-compss@bsc.es>
Prefix: /opt
BuildArch: noarch

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
