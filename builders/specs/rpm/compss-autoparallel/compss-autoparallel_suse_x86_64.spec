%define name	 	compss-autoparallel
%define version		2.2.rc1803
%define release		1

Requires: compss-engine, compss-python-binding, libtool, automake, make, gcc-c++
Summary: The PyCOMPSs AutoParallel module.
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
Prefix: /opt/COMPSs/Dependencies
ExclusiveArch: x86_64

%description
The PyCOMPSs AutoParallel module

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building PyCOMPSs AutoParallel..."
echo " "

echo "PyCOMPSs AutoParallel built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing PyCOMPSs AutoParallel..."

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
  libjvm=$(find -L ${JAVA_HOME} -name libjvm.so | head -n 1)
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
echo " - Creating structure..."
mkdir -p ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/
mkdir -p ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/pluto

echo "   - Configure AutoParallel..."
cp -r autoparallel ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/

echo "   - Configure, compile and install PLUTO"
cd pluto
./install_pluto ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/pluto
cd ..

echo " - Setting PyCOMPSs AutoParallel permissions..."
chmod 755 -R ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies
echo " - PyCOMPSs AutoParallel permissions set"
echo " "

echo "Congratulations!"
echo "PyCOMPSs AutoParallel Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing PyCOMPSs AutoParallel..."
echo " "

echo "Congratulations!"
echo "PyCOMPSs AutoParallel Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun
rm -rf /opt/COMPSs/Dependencies/autoparallel
rm -rf /opt/COMPSs/Dependencies/pluto
echo "PyCOMPSs AutoParallel Successfully uninstalled!"
echo " "

#------------------------------------------------------------------------------------
%clean
rm -rf ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/autoparallel
rm -rf ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/pluto

#------------------------------------------------------------------------------------
%files 
%defattr(-,root,root)
/opt/COMPSs/Dependencies/autoparallel
/opt/COMPSs/Dependencies/pluto
