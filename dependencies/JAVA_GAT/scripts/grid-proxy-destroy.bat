@echo off

IF "%GAT_LOCATION%X"=="X" set GAT_LOCATION=.
FOR %%i IN ("%GAT_LOCATION%\lib\adaptors\shared\*.jar") DO CALL "%GAT_LOCATION%\scripts\AddToGATClassPath.bat" %%i
java -DUID="%USERNAME%" -classpath "%GAT_LOCATION%\lib\adaptors\GlobusAdaptor\GlobusAdaptor.jar";"%GAT_CLASSPATH%" org.globus.tools.ProxyDestroy %*
