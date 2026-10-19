@echo off
REM Usage: bin\labukdb.bat catalog add logs D:\path\to\table
java -jar "%~dp0..\labukdb-cli\target\labukdb-cli.jar" %*
