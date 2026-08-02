@echo off
REM Usage: bin\delta-cli.bat catalog add logs D:\path\to\table
java -jar "%~dp0..\target\delta-cli.jar" %*
