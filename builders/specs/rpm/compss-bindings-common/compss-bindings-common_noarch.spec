%define name	 	compss-bindings-common 
%define version		2.0.r.rc1702
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
if [ -z ${JAVA_HOME} ]; then
  echo " - Finding JAVA_HOME installation"
  libjvm=$(rpm -ql java-1_8_0-openjdk-headless | grep libjvm.so | head -n 1)
  if [ -z $libjvm ]; then
    libjvm=$(rpm -ql java-1.8.0-openjdk-headless | grep libjvm.so | head -n 1)
    if [ -z $libjvm ]; then
      echo "ERROR: Invalid JAVA_HOME installation. No libjvm.so found"
      exit 1
    fi
  fi
  JAVA_LIB_DIR=$(dirname $libjvm)
  JAVA_HOME=${JAVA_LIB_DIR}/../../../
else
  echo " - Using defined JAVA_HOME installation: ${JAVA_HOME}"
  libjvm=$(find ${JAVA_HOME} -name libjvm.so | head -n 1)
  if [ -z $libjvm ]; then
    echo "ERROR: Invalid JAVA_HOME installation. No libjvm.so found"
    exit 1
  fi
  JAVA_LIB_DIR=$(dirname $libjvm)
fi

echo "Using JAVA_HOME=${JAVA_HOME}"
echo "Using JAVA_LIB_DIR=${JAVA_LIB_DIR}"
export JAVA_HOME=${JAVA_HOME}
export JAVA_LIB_DIR=${JAVA_LIB_DIR}

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
