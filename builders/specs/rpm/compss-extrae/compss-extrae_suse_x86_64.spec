%define name	 	compss-extrae 
%define version 	2.0
%define release		1

Requires: compss-engine, libxml2 >= 2.5.0, libxml2-devel >= 2.5.0, gcc-fortran
Suggests: papi, papi-devel
Summary: The BSC Extrae trace extraction tool
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
The BSC Extrae trace extraction tool.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building Extrae..."
echo " "

echo "   - Create deployment folders"
mkdir -p COMPSs/Dependencies/extrae
targetFullPath=$(pwd)/COMPSs/Dependencies/extrae

echo "   - Configure, compile and install"
cd extrae/
./install ${targetFullPath}
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

echo "Extrae built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing Extrae..."

echo " - Creating COMPSs Extrae structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Dependencies
cp -r COMPSs/Dependencies/extrae $RPM_BUILD_ROOT/opt/COMPSs/Dependencies/
echo " - COMPSs Extrae structure created"
echo " "

echo " - Setting Extrae permissions..."
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/Dependencies/extrae
echo " - Extrae permissions set"
echo " "

echo "Congratulations!"
echo "Extrae Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing Extrae..."
echo " "

echo "Congratulations!"
echo "Extrae Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun 
rm -rf $RPM_BUILD_ROOT/opt/COMPSs/Dependencies/extrae
echo "Extrae Successfully uninstalled!"
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
