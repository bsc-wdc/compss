%undefine _missing_build_ids_terminate_build

%define name	 	compss-autoparallel 
%define version		2.3.rc1810
%define release		1

Requires: compss-engine, compss-python-binding, automake, libtool, make, gcc-c++, gmp-devel, flex, bison, texinfo, gnuplot
Summary: The PyCOMPSs AutoParallel module
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

echo "   - Copy deployment files"
mkdir -p COMPSs/Dependencies/pluto_install
cp -r pluto/* COMPSs/Dependencies/pluto_install/
cp -r autoparallel COMPSs/Dependencies

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "PyCOMPSs AutoParallel built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing PyCOMPSs AutoParallel..."
  
mkdir -p ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/
cp -r COMPSs/Dependencies/autoparallel ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/
cp -r COMPSs/Dependencies/pluto_install ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/
  
echo "* Setting COMPSs C-Binding permissions..."
chmod 755 -R ${RPM_BUILD_ROOT}/opt/COMPSs/Dependencies/
  
echo "DONE!"
echo " "

#------------------------------------------------------------------------------------
%post 
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
mkdir -p /opt/COMPSs/Dependencies/pluto

echo "   - Configure AutoParallel..."
#cp -rf /opt/COMPSs/Dependencies/autoparallel /opt/COMPSs/Dependencies/

echo "   - Configure, compile and install PLUTO"
cd /opt/COMPSs/Dependencies/pluto_install
./install_pluto /opt/COMPSs/Dependencies/pluto
cd -

echo " - Setting PyCOMPSs AutoParallel permissions..."
chmod 755 -R /opt/COMPSs/Dependencies/
echo " - PyCOMPSs AutoParallel permissions set"
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
/opt/COMPSs/Dependencies/pluto_install
