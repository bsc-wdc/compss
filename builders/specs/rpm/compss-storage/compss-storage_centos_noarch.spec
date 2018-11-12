%define name	 	compss-storage
%define version		2.4.rc1811
%define release		1

Requires: compss-engine
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
The BSC COMP Superscalar Storage Implementations for COMPSs Runtime.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build


mkdir -p COMPSs/Tools/storage

echo "* Compiling storage implementations..."

# Compile Redis, make API bundle
echo " - Compiling Redis storage implementation..."
cd storage/redisPSCO
./make_bundle.sh
cd -

# Move Redis bundle to final destination
echo " - Moving Redis to final location..."
cp -r storage/redisPSCO/COMPSs-Redis-bundle COMPSs/Tools/storage/redis

#------------------------------------------------------------------------------------
%install
%define _unpackaged_files_terminate_build 0
echo "* Installing COMPSs storage implementations..."

mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Tools/storage
cp -r COMPSs/Tools/storage/* $RPM_BUILD_ROOT/opt/COMPSs/Tools/storage
chmod 775 $RPM_BUILD_ROOT/opt/COMPSs/Tools/storage

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
%defattr(-,root, root)
/opt/COMPSs/Tools/storage/*
