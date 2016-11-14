%define name	 	compss-bindings-common 
%define version 	2.0
%define release		1

Requires: compss-engine, libtool, automake, make, gcc-c++
Summary: The C libraries shared by BSC COMP Superscalar Bindings
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
The C libraries shared by BSC COMP Superscalar Bindings.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar Bindings-common..."
echo " "

echo "   - Create deployment folders"
mkdir -p COMPSs/Bindings/bindings-common
targetFullPath=$(pwd)/COMPSs/Bindings/bindings-common

echo "   - Configure, compile and install"
cd bindings-common
./install_common ${targetFullPath}
cd ..

echo "   - Copy deployment files"
#Doc
cp changelog COMPSs/
cp LICENSE COMPSs/
cp NOTICE COMPSs/
cp README COMPSs/
cp RELEASE_NOTES COMPSs/

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "COMP Superscalar Bindings-common built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs Bindings-common..."

echo " - Creating COMPSs Bindings-common structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Bindings/
cp -r COMPSs/Bindings/bindings-common $RPM_BUILD_ROOT/opt/COMPSs/Bindings/
echo " - COMPSs Bindings-common structure created"
echo " "

echo " - Setting COMPSs Bindings-common permissions..."
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/Bindings/bindings-common
echo " - COMPSs Bindings-commmon permissions set"
echo " "

echo "Congratulations!"
echo "COMPSs Bindings-common Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing COMPSs Bindings-common..."
echo " "

echo "Congratulations!"
echo "COMPSs Bindings-common Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun 
rm -rf $RPM_BUILD_ROOT/opt/COMPSs/Bindings/bindings-common
echo "COMPSs Bindings-common Successfully uninstalled!"
echo " "

#------------------------------------------------------------------------------------
%clean
rm -rf $RPM_BUILD_ROOT

#------------------------------------------------------------------------------------
%files 
#%doc README
#%doc changelog
#%doc LICENSE
#%doc NOTICE
#%doc RELEASE_NOTES
%defattr(-,root,root)
/opt/COMPSs/
