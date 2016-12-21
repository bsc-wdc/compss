%define name	 	compss-python-binding 
%define version 	2.0.rc1612
%define release		1

Requires: compss-bindings-common, python-devel
Summary: The BSC COMP Superscalar Python-Binding
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
echo " - Creating COMPSs Python-Binding structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Bindings/

echo "   - Configure, compile and install"
cd bindings-common/
./install_common
cd ../python
./install $RPM_BUILD_ROOT/opt/COMPSs/Bindings/python
cd ..

echo " - COMPSs Runtime Python-Binding structure created"
echo " "

echo " - Setting COMPSs Python-Binding permissions..."
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/Bindings/python
echo " - COMPSs Runtime Python-Binding permissions set"
echo " "

echo "Congratulations!"
echo "COMPSs Python-Binding Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing COMPSs Python-Binding..."
echo " "

echo " - Adding PyCOMPSs to user profile..."
echo "export PYTHONPATH=\$PYTHONPATH:/opt/COMPSs/Bindings/python/" >> /etc/profile.d/compss.sh
echo " - PyCOMPSs added to user profile"
echo " "

echo "Congratulations!"
echo "COMPSs Python-Binding Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun 
rm -rf $RPM_BUILD_ROOT/opt/COMPSs/Bindings/python
echo "COMPSs Python-Binding Successfully uninstalled!"
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
