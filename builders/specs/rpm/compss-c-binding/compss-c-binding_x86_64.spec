%define name            compss-c-binding                                                                                                                                                
%define version		2.4.rc1812
%define release         1

Requires: compss-bindings-common, libxml2-devel, boost-devel, tcsh
Summary: The BSC COMP Superscalar C-Binding
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
Prefix: /opt/COMPSs/bindings/c
ExclusiveArch: x86_64

%description
The BSC COMP Superscalar C-Binding.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar C-Binding..."
echo " "

echo "   - Copy deployment files"
mkdir -p COMPSs/Bindings/c_pack
cp -r c/* COMPSs/Bindings/c_pack/

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "COMP Superscalar C-Binding built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs C-Binding..."

mkdir -p ${RPM_BUILD_ROOT}/opt/COMPSs/Bindings/
cp -r COMPSs/Bindings/c_pack/ ${RPM_BUILD_ROOT}/opt/COMPSs/Bindings/

echo "* Setting COMPSs C-Binding permissions..."
chmod 755 -R ${RPM_BUILD_ROOT}/opt/COMPSs/Bindings/c_pack

echo "DONE!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing COMPSs C-Binding..."
echo " "

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
echo " - Configure, compile and install"
cd /opt/COMPSs/Bindings/c_pack/
./install /opt/COMPSs/Bindings/c/ true

echo " - Add binaries to path"
mkdir -p /opt/COMPSs/Runtime/scripts/system/c
mkdir -p /opt/COMPSs/Runtime/scripts/user
cp /opt/COMPSs/Bindings/c_pack/bin/* /opt/COMPSs/Runtime/scripts/system/c
cp /opt/COMPSs/Bindings/c_pack/compss_build_app /opt/COMPSs/Runtime/scripts/user/

echo " - Remove unneeded sources"
rm -rf /opt/COMPSs/Runtime/Bindings/c_pack/

echo " - Adding c-binaries to profile"
echo "export PATH=\$PATH:/opt/COMPSs/Bindings/c/bin" >> /etc/profile.d/compss.sh
echo " "

echo " - Setting COMPSs C-Binding permissions..."
chmod 755 -R /opt/COMPSs/Runtime/scripts/system/c
chmod 755 /opt/COMPSs/Runtime/scripts/user/compss_build_app
chmod 755 -R /opt/COMPSs/Bindings/c

echo "Congratulations!"
echo "COMPSs C-Binding Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun 
rm -rf /opt/COMPSs/Bindings/c
rm -rf /opt/COMPSs/Runtime/scripts/system/c
rm -f /opt/COMPSs/Runtime/scripts/user/compss_build_app
echo "COMPSs C-Binding Successfully uninstalled!"
echo " "

#------------------------------------------------------------------------------------
%clean
rm -rf ${RPM_BUILD_ROOT}/opt/COMPSs/Bindings/c
rm -rf ${RPM_BUILD_ROOT}/opt/COMPSs/Runtime/scripts/system/c
rm -f ${RPM_BUILD_ROOT}/opt/COMPSs/Runtime/scripts/user/compss_build_app

#------------------------------------------------------------------------------------
%files 
%defattr(-,root,root)
/opt/COMPSs/Bindings/c_pack

