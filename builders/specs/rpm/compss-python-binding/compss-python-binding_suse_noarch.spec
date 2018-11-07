%define name	 	compss-python-binding
%define version		2.4
%define release		1

Requires: compss-bindings-common, python-devel >= 2.7
Summary: The BSC COMP Superscalar Python-Binding
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
Prefix: /opt/COMPSs/Bindings/python
BuildArch: noarch

%description
The BSC COMP Superscalar Python-Binding.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar Python-Binding..."
echo " "

echo "COMP Superscalar Python-Binding built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs Python-Binding..."

# Find JAVA_HOME
if [ -z ${JAVA_HOME} ]; then
  echo " - Finding JAVA_HOME installation"
  libjvm=$(rpm -ql java-1_8_0-openjdk-headless | grep libjvm.so$ | head -n 1)
  if [ -z $libjvm ]; then
    libjvm=$(rpm -ql java-1.8.0-openjdk-headless | grep libjvm.so$ | head -n 1)
    if [ -z $libjvm ]; then
      echo "ERROR: Invalid JAVA_HOME installation. No libjvm.so found"
      exit 1
    fi
  fi
  JAVA_LIB_DIR=$(dirname $libjvm)
  JAVA_HOME=${JAVA_LIB_DIR}/../../../../
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
echo " - Creating COMPSs Python-Binding structure..."
mkdir -p ${RPM_BUILD_ROOT}/opt/COMPSs/Bindings/

echo "   - Configure, compile and install"
cd bindings-common/
./install_common
cd ../python
./install ${RPM_BUILD_ROOT}/opt/COMPSs/Bindings/python false false
cd ..

echo " - COMPSs Runtime Python-Binding structure created"
echo " "

echo " - Setting COMPSs Python-Binding permissions..."
chmod 755 -R ${RPM_BUILD_ROOT}/opt/COMPSs/Bindings/python
echo " - COMPSs Runtime Python-Binding permissions set"
echo " "

echo "Congratulations!"
echo "COMPSs Python-Binding Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post
echo "* Installing COMPSs Python-Binding..."
echo " "

# echo " - Adding PyCOMPSs to user profile..."  # Not needed. Symlinks instead.
# echo "export PYTHONPATH=\$PYTHONPATH:/opt/COMPSs/Bindings/python/" >> /etc/profile.d/compss.sh
# echo " - PyCOMPSs added to user profile"
# echo " "

# Load common functions for setting the symlinks
source /opt/COMPSs/Bindings/python/commons

echo " - Unifying PyCOMPSs sources..."
if [ -d /opt/COMPSs/Bindings/python/2 ] && [ -d /opt/COMPSs/Bindings/python/3 ]; then
  echo " - Both versions installed: unifying... "
  unify_installed_versions /opt/COMPSs/Bindings/python
fi
echo " - PyCOMPSs sources unified."

echo " - Adding PyCOMPSs symlinks to site-packages or dist-packages folder..."
if [ -d /opt/COMPSs/Bindings/python/2 ]; then
  create_symbolic_links 2 /opt/COMPSs/Bindings/python/2
fi
if [ -d /opt/COMPSs/Bindings/python/3 ]; then
  create_symbolic_links 3 /opt/COMPSs/Bindings/python/3
fi
echo " - PyCOMPSs symlinks added to site-packages or dist-packages folder."

echo "Congratulations!"
echo "COMPSs Python-Binding Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun
# Remove symbolic links
/opt/COMPSs/Bindings/python/./clean
# Remove completely the python binding
rm -rf /opt/COMPSs/Bindings/python

echo "COMPSs Python-Binding Successfully uninstalled!"
echo " "

#------------------------------------------------------------------------------------
%clean
rm -rf ${RPM_BUILD_ROOT}/opt/COMPSs/Bindings/python

#------------------------------------------------------------------------------------
%files
%defattr(-,root,root)
/opt/COMPSs/Bindings/python
