@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "TASK=P2PRELAY"
set "PORT=34777"
set "CMD=%~1"
if "%CMD%"=="" set "CMD=run"
if /i "%~2"=="--port" set "PORT=%~3"

set "JAR="
for %%f in (p2pmod-*.jar) do set "JAR=%%f"

if /i "%CMD%"=="run" goto do_run
if /i "%CMD%"=="install" goto do_install
if /i "%CMD%"=="uninstall" goto do_uninstall
if /i "%CMD%"=="start" goto do_start
if /i "%CMD%"=="stop" goto do_stop
if /i "%CMD%"=="restart" goto do_restart
if /i "%CMD%"=="status" goto do_status
if /i "%CMD%"=="log" goto do_log
if /i "%CMD%"=="probe" goto do_probe
if /i "%CMD%"=="selftest" goto do_selftest
if /i "%CMD%"=="help" goto usage
echo Unknown command: %CMD%
goto usage

:usage
echo.
echo Usage: relay.bat [command] [--port N]
echo.
echo   run        Run in the foreground (default)
echo   install    Register to start at logon and start it now
echo   uninstall  Remove the logon task and stop it
echo   start      Start it
echo   stop       Stop it
echo   restart    Restart it
echo   status     Show status
echo   log        Follow the log
echo   probe      Probe a port, e.g.: relay.bat probe 1.2.3.4:34777
echo   selftest   Local self-test
echo.
echo Example: relay.bat install --port 34888
echo.
ping -n 6 127.0.0.1 >nul
exit /b 1

:no_jar
echo [error] No p2pmod-*.jar in the current directory
echo         Put the jar, the lib directory and this script in the same folder
ping -n 8 127.0.0.1 >nul
exit /b 1

:no_lib
echo [error] Missing the lib directory, which should contain slf4j-api and slf4j-simple
ping -n 8 127.0.0.1 >nul
exit /b 1

:no_java
echo [error] java was not found
echo         Install Microsoft Build of OpenJDK 25:
echo         https://aka.ms/download-jdk/microsoft-jdk-25-windows-x64.zip
echo         Unzip it, add its bin directory to PATH, then reopen this window
ping -n 12 127.0.0.1 >nul
exit /b 1

:check
if "%JAR%"=="" goto no_jar
if not exist "lib" goto no_lib
where java >nul 2>&1
if errorlevel 1 goto no_java
goto :eof

:do_run
call :check
echo Starting p2pmod relay on UDP %PORT%  (close this window to stop)
java -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -Xms32m -Xmx128m -cp "%JAR%;lib/*" dev.p2pmod.relay.RelayMain --port %PORT%
echo.
echo Process exited with code %ERRORLEVEL%
ping -n 4 127.0.0.1 >nul
exit /b 0

:do_install
call :check
for %%I in ("%~f0") do set "SELF_RUN=%%~sI"
if "%SELF_RUN%"=="" set "SELF_RUN=%~f0"
schtasks /create /tn "%TASK%" /tr "%SELF_RUN%" /sc onlogon /f
if errorlevel 1 (
  echo [error] Could not create the scheduled task
  ping -n 8 127.0.0.1 >nul
  exit /b 1
)
echo Registered to start at logon
call :kill_port 1
schtasks /run /tn "%TASK%" >nul 2>&1
echo Started, the window will appear shortly
ping -n 3 127.0.0.1 >nul
exit /b 0

:do_uninstall
schtasks /end /tn "%TASK%" >nul 2>&1
schtasks /delete /tn "%TASK%" /f >nul 2>&1
call :kill_port 0
echo Logon task removed and relay stopped
exit /b 0

:do_start
call :check
schtasks /run /tn "%TASK%" >nul 2>&1
if errorlevel 1 (
  echo [note] No scheduled task registered, starting directly instead
  start "p2pmod relay" "%~f0" run --port %PORT%
  exit /b 0
)
echo Start triggered
exit /b 0

:do_stop
call :kill_port 0
echo Stopped
exit /b 0

:do_restart
call :kill_port 1
schtasks /run /tn "%TASK%" >nul 2>&1
if errorlevel 1 start "p2pmod relay" "%~f0" run --port %PORT%
echo Restarted
exit /b 0

:do_status
echo ------------------------------------------------------
schtasks /query /tn "%TASK%" /fo LIST 2>nul | findstr /i "TaskName Status"
echo ------------------------------------------------------
netstat -ano | findstr /c:":%PORT%"
if errorlevel 1 (
  echo UDP port %PORT% is not listening
) else (
  echo UDP port %PORT% is listening
)
exit /b 0

:do_log
if not exist "relay.log" (
  echo No relay.log yet, run the relay once first
  ping -n 6 127.0.0.1 >nul
  exit /b 1
)
echo Following relay.log, press Ctrl+C to quit
powershell -NoProfile -Command "Get-Content -Path 'relay.log' -Tail 40 -Wait -Encoding UTF8"
exit /b 0

:do_probe
call :check
if "%~2"=="" (
  echo Usage: relay.bat probe ^<host^>[:port]
  exit /b 1
)
java -cp "%JAR%;lib/*" dev.p2pmod.relay.RelayMain --probe %~2
exit /b 0

:do_selftest
call :check
java -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -cp "%JAR%;lib/*" dev.p2pmod.relay.RelayMain --port %PORT% --selftest
exit /b 0

:kill_port
for /f "tokens=4" %%a in ('netstat -ano ^| findstr /c:":%PORT%" ^| findstr /i "UDP"') do (
  taskkill /f /pid %%a >nul 2>&1
)
if "%~1"=="1" ping -n 14 127.0.0.1 >nul
goto :eof
