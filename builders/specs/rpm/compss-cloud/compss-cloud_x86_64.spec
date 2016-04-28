%define name	 	compss-cloud 
%define version 	1.4.rc05
%define release		1

Requires: compss-engine, java-1.7.0-openjdk
Summary: The BSC COMP Superscalar Runtime Cloud Resources
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
The BSC COMP Superscalar Runtime Cloud Resources.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar Runtime Cloud Resources..."
echo " "

echo "   - Compile sources"
cd resources
mvn -U clean install
cd ..

echo "   - Create deployment folders"
mkdir -p COMPSs/Runtime/connectors

echo "   - Copy deployment files"
connectors=$(find ./resources/ -name "*.jar")
for conn in $connectors; do
  cp -f $conn COMPSs/Runtime/connectors/
done
#Doc
cp changelog COMPSs/
cp LICENSE COMPSs/
cp NOTICE COMPSs/
cp README COMPSs/
cp RELEASE_NOTES COMPSs/

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "COMP Superscalar Runtime Cloud Resources built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs Runtime Cloud Resources..."

echo " - Creating COMPSs Runtime Cloud Resource structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Runtime/
cp -r COMPSs/Runtime/connectors $RPM_BUILD_ROOT/opt/COMPSs/Runtime/
echo " - COMPSs Runtime Cloud Resources structure created"
echo " "

echo " - Setting COMPSs Runtime Cloud Resources permissions..."
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/Runtime/connectors
echo " - COMPSs Runtime Cloud Resources permissions set"
echo " "

echo "Congratulations!"
echo "COMPSs Runtime Cloud Resources Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing COMPSs Runtime Cloud Resources..."
echo " "

echo "Congratulations!"
echo "COMPSs Runtime Cloud Resources Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun 
rm -rf $RPM_BUILD_ROOT/opt/COMPSs/Runtime/connectors
echo "COMPSs Runtime Cloud Resources Successfully uninstalled!"
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
