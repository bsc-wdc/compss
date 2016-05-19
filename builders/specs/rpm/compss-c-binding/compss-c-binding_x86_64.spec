%define name	 	compss-c-binding 
%define version 	1.4.rc05
%define release		1

Requires: compss-bindings-common, libxml2-devel, libtool, automake, make, boost-devel, tcsh, gcc-c++
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

echo "COMP Superscalar C-Binding built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs C-Binding..."

# Find JAVA_HOME
openjdk=$(rpm -qa | grep jdk-1.7.0)
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
echo " - Creating COMPSs C-Binding structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Bindings/

echo "   - Configure, compile and install"
cd bindings-common/
./install_common
cd ../c/
./install $RPM_BUILD_ROOT/opt/COMPSs/Bindings/c true
cd ..

echo "   - Add binaries to path"
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/system/c
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/user
cp c/bin/* $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/system/c
cp c/buildapp $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/user/
echo "   - Binaries added"
echo " "

echo " - Setting COMPSs C-Binding permissions..."
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/system/c
chmod 755 $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/user/buildapp
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/Bindings/c
echo " - COMPSs Runtime C-Binding permissions set"
echo " "

echo "Congratulations!"
echo "COMPSs C-Binding Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing COMPSs C-Binding..."
echo " "

echo " - Adding c-binaries to profile..."
echo "export PATH=\$PATH:/opt/COMPSs/Bindings/c/bin" >> /etc/profile.d/compss.sh
echo " - c-binaries added to user profile"
echo " "

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
