%define name	 	compss-c-binding 
%define version 	1.4.rc05
%define release		1

Requires: compss-bindings-common, libxml2, libtool, automake, make, boost-devel, tcsh, gcc-c++
Summary: The BSC COMP Superscalar C Binding
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
The BSC COMP Superscalar C Binding.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar C-Binding..."
echo " "

echo "   - Create deployment folders"
mkdir -p COMPSs/Bindings/c
targetFullPath=$(pwd)/COMPSs/Bindings/c

echo "   - Configure, compile and install"
cd bindings-common/
./install_common
# Compile non-location dependant c-binding
cd ../c/
./install ${targetFullPath}
cd ..

# Copy location dependant c-binding
cp c/install COMPSs/Bindings/c
mkdir -p COMPSs/Bindings/c/src/
cp -r c/src/gsbuilder COMPSs/Bindings/c/src

# Copy user scripts
cp c/buildapp COMPSs/Bindings/

# Doc
echo "   - Copy deployment files"
cp changelog COMPSs/
cp LICENSE COMPSs/
cp NOTICE COMPSs/
cp README COMPSs/
cp RELEASE_NOTES COMPSs/

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "COMP Superscalar C-Binding built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs C-Binding..."

echo " - Creating COMPSs C-Binding structure..."
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Bindings/
cp -r COMPSs/Bindings/c $RPM_BUILD_ROOT/opt/COMPSs/Bindings/
cd COMPSs/Bindings/c/
./install $RPM_BUILD_ROOT/opt/COMPSs/Bindings/c/ false
cd -
echo " - COMPSs C-Binding structure created"
echo " "

echo "   - Add binaries to path"
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/system/c
mkdir -p $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/user
cp COMPSs/Bindings/c/bin/* $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/system/c
cp COMPSs/Bindings/buildapp $RPM_BUILD_ROOT/opt/COMPSs/Runtime/scripts/user/
echo " - COMPSs Runtime C-Binding binaries added"
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
