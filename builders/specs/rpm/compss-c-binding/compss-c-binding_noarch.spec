%define name	 	compss-c-binding
%define version		2.1.rc1706 
%define release		1

Requires: compss-bindings-common, libxml2-devel, boost-devel, tcsh
Summary: The BSC COMP Superscalar C-Binding
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
The BSC COMP Superscalar C-Binding.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar C-Binding..."
echo " "

echo "   - Copy deployment files"
mkdir -p COMPSs/Bindings/c_pack
cp changelog COMPSs/
cp LICENSE COMPSs/
cp NOTICE COMPSs/
cp README COMPSs/
cp RELEASE_NOTES COMPSs/
cp -r c/* COMPSs/Bindings/c_pack/

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "COMP Superscalar C-Binding built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs C-Binding..."

mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Bindings/
                                                                                                                                                                                        
cp COMPSs/changelog $RPM_BUILD_ROOT/opt/COMPSs/
cp COMPSs/LICENSE $RPM_BUILD_ROOT/opt/COMPSs/
cp COMPSs/NOTICE $RPM_BUILD_ROOT/opt/COMPSs/
cp COMPSs/README $RPM_BUILD_ROOT/opt/COMPSs/
cp COMPSs/RELEASE_NOTES $RPM_BUILD_ROOT/opt/COMPSs/    

cp -r COMPSs/Bindings/c_pack/ $RPM_BUILD_ROOT/opt/COMPSs/Bindings/

echo "* Setting COMPSs C-Binding permissions..."
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/Bindings/c_pack

echo "DONE!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing COMPSs C-Binding..."
echo " "

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
echo " - Configure, compile and install"
cd /opt/COMPSs/Bindings/c_pack/
./install /opt/COMPSs/Bindings/c/ true

echo " - Add binaries to path"
mkdir -p /opt/COMPSs/Runtime/scripts/system/c
mkdir -p /opt/COMPSs/Runtime/scripts/user
cp /opt/COMPSs/Bindings/c_pack/bin/* /opt/COMPSs/Runtime/scripts/system/c
cp /opt/COMPSs/Bindings/c_pack/buildapp /opt/COMPSs/Runtime/scripts/user/

echo " - Remove unneeded sources"
rm -rf /opt/COMPSs/Runtime/Bindings/c_pack/

echo " - Adding c-binaries to profile"
echo "export PATH=\$PATH:/opt/COMPSs/Bindings/c/bin" >> /etc/profile.d/compss.sh
echo " "

echo " - Setting COMPSs C-Binding permissions..."
chmod 755 -R /opt/COMPSs/Runtime/scripts/system/c
chmod 755 /opt/COMPSs/Runtime/scripts/user/buildapp
chmod 755 -R /opt/COMPSs/Bindings/c

echo "Congratulations!"
echo "COMPSs C-Binding Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun 
rm -rf $RPM_BUILD_ROOT/opt/COMPSs/Bindings/c
rm -rf $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/system/c
rm -f $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/user/buildapp
echo "COMPSs C-Binding Successfully uninstalled!"
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
