@echo off

IF "%GAT_LOCATION%X"=="X" set GAT_LOCATION=.


:: Don't touch anything below this line

set GAT_LIB_LOCATION=%GAT_LOCATION%\lib
set GAT_ADAPTOR_LIB_LOCATION=%GAT_LIB_LOCATION%\adaptors

:: Create the path with the JAR files
SET GAT_CLASSPATH=

FOR %%i IN ("%GAT_LIB_LOCATION%\*.jar") DO CALL "%GAT_LOCATION%\scripts\AddToGATClassPath.bat" %%i

java -cp "%CLASSPATH%";"%GAT_CLASSPATH%" -Dlog4j.configuration=file:"%GAT_LOCATION%"\log4j.properties -Dgat.adaptor.path="%GAT_ADAPTOR_LIB_LOCATION%" %*
