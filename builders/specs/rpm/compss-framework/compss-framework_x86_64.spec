%define name	 	compss-framework 
%define version 	2.0
%define release		1

Requires: compss-runtime, compss-bindings, compss-tools, compss-cloud
Summary: The BSC COMP Superscalar Framework
Name: %{name}
Version: %{version}
Release: %{release}
License: Apache 2.0
Group: Development/Libraries
Source: %{name}-%{version}.tar.gz
Distribution: Linux
Vendor: Barcelona Supercomputing Center - Centro Nacional de Supercomputacion
URL: http://compss.bsc.es
Packager: Cristian Ramon-Cortes <cristian.ramoncortes@bsc.es>
Prefix: /opt
BuildArch: x86_64

%description
The BSC COMP Superscalar Framework.

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
