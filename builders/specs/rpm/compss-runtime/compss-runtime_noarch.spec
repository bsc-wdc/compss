%define name	 	compss-runtime 
%define version		2.4
%define release		1

Requires: compss-engine, compss-worker
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
BuildArch: noarch

%description
The BSC COMP Superscalar Runtime.

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
