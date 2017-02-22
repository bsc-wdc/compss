%define name	 	compss-engine 
%define version		2.0.rc1702
%define release		1

Requires: java-1.8.0-openjdk, xdg-utils, graphviz
Summary: The BSC COMP Superscalar Runtime Engine
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
The BSC COMP Superscalar Runtime Engine.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar Runtime Engine..."
echo " "

echo "   - Compile sources"
mvn -U clean install

echo "   - Create deployment folders"
mkdir -p COMPSs
mkdir -p COMPSs/Doc
mkdir -p COMPSs/Dependencies
mkdir -p COMPSs/Runtime
mkdir -p COMPSs/Runtime/configuration
mkdir -p COMPSs/Runtime/scripts
mkdir -p COMPSs/Runtime/adaptors
mkdir -p COMPSs/Runtime/scheduler

echo "   - Copy deployment files"
#Doc
find doc/ -name *.html | xargs rm -rf
cp -r doc/* COMPSs/Doc
cp changelog COMPSs/
cp LICENSE COMPSs/
cp NOTICE COMPSs/
cp README COMPSs/
cp RELEASE_NOTES COMPSs/

#Dependencies
cp -r files/paraver COMPSs/Dependencies/
cp -r dependencies/JAVA_GAT COMPSs/Dependencies/

#Config
find compss/runtime/config -name src | xargs rm -rf
find compss/runtime/config -name target | xargs rm -rf
find compss/runtime/config -name pom.xml | xargs rm -rf
cp -r compss/runtime/config/* COMPSs/Runtime/configuration/

#Scripts
cp -r compss/runtime/scripts/* COMPSs/Runtime/scripts/

#Adaptors
find compss/runtime/adaptors -name pom.xml | xargs rm -rf
rm -r compss/runtime/adaptors/commons
path_source="compss/runtime/adaptors"
path_target="COMPSs/Runtime/adaptors"
adaptors=$(ls ${path_source})
for adaptor in $adaptors; do
  mkdir ${path_target}/$adaptor
  mkdir ${path_target}/$adaptor/master
  cp ${path_source}/$adaptor/master/*.jar ${path_target}/$adaptor/master
  if [ -f ${path_source}/$adaptor/master/properties ]; then
    cp ${path_source}/$adaptor/master/properties ${path_target}/$adaptor/master
  fi
  if [ -d "${path_source}/$adaptor/scripts/" ]; then
    mkdir -p COMPSs/Runtime/scripts/system/adaptors/$adaptor/
    cp -r ${path_source}/$adaptor/scripts/* COMPSs/Runtime/scripts/system/adaptors/$adaptor/
  fi
done

#Schedulers
find compss/runtime/scheduler/ -name pom.xml | xargs rm -rf
rm -r compss/runtime/scheduler/commons
schedulers=$(find compss/runtime/scheduler/ -name "*.jar")
for scheduler in $schedulers; do
  cp $scheduler COMPSs/Runtime/scheduler/
done

#Engine
cp compss/runtime/compss-engine.jar COMPSs/Runtime/

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "COMP Superscalar Runtime Engine built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs Runtime Engine..."

echo " - Creating COMPSs Runtime Engine structure..."
mkdir -p $RPM_BUILD_ROOT/opt/
cp -r COMPSs $RPM_BUILD_ROOT/opt/
echo " - COMPSs Runtime Engine structure created"
echo " "

echo " - Setting COMPSs Runtime Engine permissions..."
chmod 755 -R $RPM_BUILD_ROOT/opt/COMPSs/
chmod 777 -R $RPM_BUILD_ROOT/opt/COMPSs/Runtime/configuration/
echo " - COMPSs Runtime Engine permissions set"
echo " "

echo "Congratulations!"
echo "COMPSs Runtime Engine Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing COMPSs Runtime Engine..."
echo " "

echo " - Adding runcompss to profile..."
echo "export PATH=\$PATH:/opt/COMPSs/Runtime/scripts/user" > /etc/profile.d/compss.sh
echo " - Runcompss added to user profile"
echo " "

echo " - Adding compss-engine.jar to profile..."
echo "export CLASSPATH=\$CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar" >> /etc/profile.d/compss.sh
echo " - compss-engine.jar added to user profile"
echo " "

echo "Congratulations!"
echo "COMPSs Runtime Engine Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun

#------------------------------------------------------------------------------------
%postun 
rm -rf $RPM_BUILD_ROOT/opt/COMPSs/
echo "COMPSs Runtime Engine Successfully uninstalled!"
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
#%docdir /opt/COMPSs/Doc
#%config /opt/COMPSs/Runtime/configuration/
#%config /etc/profile.d/compss.sh
%defattr(-,root,root)
/opt/COMPSs/
