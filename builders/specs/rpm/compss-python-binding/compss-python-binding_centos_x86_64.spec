%define name	 	compss-python-binding 
%define version 	2.0
%define release		1

Requires: compss-bindings-common, python
Summary: The BSC COMP Superscalar Python Binding
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
BuildArch: x86_64

%description
The BSC COMP Superscalar Python Binding.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar Python-Binding..."
echo " "

echo "   - Create deployment folders"
mkdir -p COMPSs/Bindings/python
targetFullPath=$(pwd)/COMPSs/Bindings/python

echo "   - Configure, compile and install"
cd bindings-common/
./install_common
cd ../python/
./install $targetFullPath
cd ..

echo "   - Copy deployment files"
#Doc
cp changelog COMPSs/
cp LICENSE COMPSs/
cp NOTICE COMPSs/
cp README COMPSs/
cp RELEASE_NOTES COMPSs/

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "COMP Superscalar Python-Binding built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs Python-Binding..."

echo " - Creating COMPSs Python-Binding structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Bindings/
cp -r COMPSs/Bindings/python $RPM_BUILD_ROOT/opt/COMPSs/Bindings/
echo " - COMPSs Python-Binding structure created"
echo " "

echo " - Setting COMPSs Python-Binding permissions..."
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/Bindings/python
echo " - COMPSs Python-Binding permissions set"
echo " "

echo "Congratulations!"
echo "COMPSs Python-Bindings Successfully installed!"
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
