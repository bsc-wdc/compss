%define name	 	compss-extrae 
%define version		2.1.rc1706
%define release		1

Requires: compss-engine, libxml2 >= 2.5.0, libxml2-devel >= 2.5.0, libtool, automake, make, gcc-c++, gcc-gfortran
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
BuildArch: noarch

%description
The BSC Extrae trace extraction tool.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building Extrae..."
echo " "

echo "Extrae built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing Extrae..."

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
echo " - Creating COMPSs Extrae structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Dependencies/extrae

echo "   - Configure, compile and install"
cd extrae
./install $RPM_BUILD_ROOT/opt/COMPSs/Dependencies/extrae false
cd ..

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
echo "* Installing COMPSs Extrae..."
echo " "

echo "Congratulations!"
echo "COMPSs Extrae Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun 
rm -rf $RPM_BUILD_ROOT/opt/COMPSs/Dependencies/extrae
echo "COMPSs Extrae Successfully uninstalled!"
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
