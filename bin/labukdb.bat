@echo off
REM Usage: bin\labukdb.bat catalog add logs D:\path\to\table
REM --add-opens is required for any command that decodes rows (query,
REM describe) -- see bin/labukdb's own comment for why.
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar "%~dp0..\labukdb-cli\target\labukdb-cli.jar" %*
