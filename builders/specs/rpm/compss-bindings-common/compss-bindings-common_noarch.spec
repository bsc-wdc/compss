%define name	 	compss-bindings-common 
%define version 	2.0.rc1612
%define release		1

Requires: compss-engine, libtool, automake, make, gcc-c++
Summary: The C libraries shared by BSC COMP Superscalar Bindings
Name: %{name}
Version: %{version}
Release: %{release}
License: Apache 2.0.rc1612
Group: Development/Libraries
Source: %{name}-%{version}.tar.gz
Distribution: Linux
Vendor: Barcelona Supercomputing Center - Centro Nacional de Supercomputacion
URL: http://compss.bsc.es
Packager: Cristian Ramon-Cortes <cristian.ramoncortes@bsc.es>
Prefix: /opt
BuildArch: noarch

%description
The C libraries shared by BSC COMP Superscalar Bindings.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar Bindigs-common..."
echo " "

echo "COMP Superscalar Bindings-common built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs Bindings-common..."

# Find JAVA_HOME
openjdk=$(rpm -qa | grep jdk-1.8.0)
libjvm=$(rpm -ql $openjdk | grep libjvm.so | head -n 1)
export JAVA_LIB_DIR=$(dirname $libjvm)
if test "${libjvm#*/jre/lib/amd64/server/libjvm.so}" != "$libjvm"; then
  export JAVA_HOME="${libjvm/\/jre\/lib\/amd64\/server\/libjvm.so/}"
elif test "${libjvm#*/jre/lib/i386/client/libjvm.so}" != "$libjvm"; then
  export JAVA_HOME="${libjvm/\/jre\/lib\/i386\/client\/libjvm.so/}"
elif [ -z $JAVA_HOME ]; then
  echo "Please define \$JAVA_HOME"
  exit 1
fi
echo "Using JAVA_HOME=$JAVA_HOME"

# Install
echo " - Creating COMPSs Bindings-common structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Bindings/

echo "   - Configure, compile and install"
cd bindings-common
./install_common $RPM_BUILD_ROOT/opt/COMPSs/Bindings/bindings-common
cd ..

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
