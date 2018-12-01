%define name	 	compss-monitor 
%define version		2.4.rc1812
%define release		1

Requires: compss-engine, xdg-utils, graphviz
Summary: The BSC COMP Superscalar Monitor Tool
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
Prefix: /opt/COMPSs/Tools/monitor
BuildArch: noarch

%description
The BSC COMP Superscalar Monitor Tool.

%prep
%setup -q

#------------------------------------------------------------------------------------
%build
echo "* Building COMP Superscalar Monitor Tool..."
echo " "

echo "   - Compile sources"
cd monitor
mvn -U clean install
cd ..

echo "   - Create deployment folders"
mkdir -p COMPSs/Tools/monitor
mkdir -p COMPSs/etc/init.d/

echo "   - Copy deployment files"
#Apache
tar xzf apache-tomcat-*.tar.gz
rm -rf apache-tomcat-*.tar.gz
mv apache-tomcat-* COMPSs/Tools/monitor/apache-tomcat/
rm -rf COMPSs/Tools/monitor/apace-tomcat/webapps/*
rm -f COMPSs/Tools/monitor/apache-tomcat/conf/server.xml
#Monitor files
cp monitor/target/*.war COMPSs/Tools/monitor/apache-tomcat/webapps/
cp monitor/target/classes/compss-monitor.conf COMPSs/Tools/monitor/apache-tomcat/conf/
cp monitor/target/classes/server.xml COMPSs/Tools/monitor/apache-tomcat/conf/
cp monitor/target/classes/*log4j* COMPSs/Tools/monitor/
cp monitor/scripts/compss-monitor COMPSs/etc/init.d/

echo "   - Erase sources"
ls . | grep -v COMPSs | xargs rm -r

echo "COMP Superscalar Monitor Tool built"
echo " "

#------------------------------------------------------------------------------------
%install
echo "* Installing COMPSs Monitor Tool..."

echo " - Creating COMPSs Monitor Tool structure..."
mkdir -p ${RPM_BUILD_ROOT}/opt/COMPSs/Tools/
cp -r COMPSs/Tools/monitor ${RPM_BUILD_ROOT}/opt/COMPSs/Tools/
echo " - COMPSs Monitor Tool structure created"
echo " "

echo " - Adding compss-monitor to init.d..."
mkdir -p ${RPM_BUILD_ROOT}/opt/COMPSs/etc/init.d/
cp COMPSs/etc/init.d/compss-monitor ${RPM_BUILD_ROOT}/opt/COMPSs/etc/init.d/
echo " - Compss-monitor added to user init.d"
echo " "

echo " - Setting COMPSs Monitor Tool permissions..."
chmod 755 -R ${RPM_BUILD_ROOT}/opt/COMPSs/Tools/monitor
echo " - COMPSs Monitor Tool permissions set"
echo " "

echo "Congratulations!"
echo "COMPSs Monitor Tool Successfully installed!"
echo " "

#------------------------------------------------------------------------------------
%post 
echo "* Installing COMPSs Monitor Tool..."
echo " "

echo " - Adding compss-monitor to chkconfig..."
cp /opt/COMPSs/etc/init.d/compss-monitor /etc/init.d/
chkconfig --add compss-monitor
echo " - Compss-monitor added to chkconfig"
echo " "

echo "Congratulations!"
echo "COMPSs Monitor Tool Successfully installed!"
echo " "


#------------------------------------------------------------------------------------
%preun
/etc/init.d/compss-monitor stop
chkconfig --del compss-monitor

#------------------------------------------------------------------------------------
%postun 
rm -rf /opt/COMPSs/Tools/monitor
rm -f /etc/init.d/compss-monitor
echo "COMPSs Monitor Tool Successfully uninstalled!"
echo " "

#------------------------------------------------------------------------------------
%clean
rm -rf ${RPM_BUILD_ROOT}/opt/COMPSs/Tools/monitor
rm -f ${RPM_BUILD_ROOT}/etc/init.d/compss-monitor

#------------------------------------------------------------------------------------
%files 
%defattr(-,root,root)
/opt/COMPSs/Tools/monitor
/opt/COMPSs/etc/init.d/compss-monitor 

