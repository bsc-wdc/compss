%define name	 	compss-engine
%define version		2.4.rc1904
%define release		1

Requires: java-1_8_0-openjdk, xdg-utils, graphviz
Summary: The BSC COMP Superscalar Runtime Engine
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
Prefix: /opt
BuildArch: noarch

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
echo $(pwd)
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
rm -r compss/runtime/adaptors/execution

path_source="compss/runtime/adaptors"
path_target="COMPSs/Runtime/adaptors"
adaptors=$(ls ${path_source})
for adaptor in $adaptors; do
  if [ "$adaptor" != "agent" ]; then
    #Regular adaptor: Copying only master part
    mkdir ${path_target}/$adaptor
    mkdir ${path_target}/$adaptor/master
    #Installing master jars and properties
    cp ${path_source}/$adaptor/master/*.jar ${path_target}/$adaptor/master
    if [ -f ${path_source}/$adaptor/master/properties ]; then
      cp ${path_source}/$adaptor/master/properties ${path_target}/$adaptor/master
    fi
    #Installing scripts
    if [ -d "${path_source}/$adaptor/scripts/" ]; then
      mkdir -p ${COMPSs_target}/Runtime/scripts/system/adaptors/$adaptor/
      cp -r ${path_source}/$adaptor/scripts/* ${COMPSs_target}/Runtime/scripts/system/adaptors/$adaptor/
    fi
  else
    # Agents adaptors
    agent_path_source=${path_source}/${adaptor}
    agents=$(ls ${agent_path_source})
    for agent in $agents; do
      #Agent: Copying
      #   - master (necessary to use other agents)
      #   - worker (necessary for being published as an agent)
      mkdir ${path_target}/$agent
      mkdir ${path_target}/$agent/master
      #Installing master jars and properties
      cp ${agent_path_source}/$agent/master/*.jar ${path_target}/$agent/master
      if [ -f ${agent_path_source}/$agent/master/properties ]; then
        cp ${agent_path_source}/$agent/master/properties ${path_target}/$agent/master
      fi
      #Installing worker jars and properties
      if [ -d "${agent_path_source}/$agent/worker/" ]; then
        mkdir ${path_target}/$agent/worker
        cp ${agent_path_source}/$agent/worker/*.jar ${path_target}/$agent/worker
        if [ -f ${agent_path_source}/$agent/worker/properties ]; then
          cp ${agent_path_source}/$agent/worker/properties ${path_target}/$agent/worker
        fi
      fi
      #Installing scripts
      if [ -d "${agent_path_source}/$agent/scripts/" ]; then
        mkdir -p ${COMPSs_target}/Runtime/scripts/system/adaptors/$agent/
        cp -r ${agent_path_source}/$agent/scripts/* ${COMPSs_target}/Runtime/scripts/system/adaptors/$agent/
      fi
    done
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
echo "export PATH=\$PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Runtime/scripts/utils" > /etc/profile.d/compss.sh
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
rm -rf /opt/COMPSs/
echo "COMPSs Runtime Engine Successfully uninstalled!"
echo " "

#------------------------------------------------------------------------------------
%clean
rm -rf ${RPM_BUILD_ROOT}/opt/COMPSs/

#------------------------------------------------------------------------------------
%files
%defattr(-,root,root)
/opt/COMPSs/Runtime/
/opt/COMPSs/Dependencies/
%doc /opt/COMPSs/README
%doc /opt/COMPSs/changelog
%doc /opt/COMPSs/LICENSE
%doc /opt/COMPSs/NOTICE
%doc /opt/COMPSs/RELEASE_NOTES
%docdir /opt/COMPSs/Doc/
/opt/COMPSs/Doc/
