@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" >nul 2>&1
if errorlevel 1 (
    echo Failed to locate repository root from "%~dp0".
    exit /b 1
)

set "REMOVED=0"

for /f "delims=" %%D in ('dir /s /b /ad bin 2^>nul') do (
    if exist "%%D\" (
        rd /s /q "%%D"
        echo Removed %%D
        set "REMOVED=1"
    )
)

for /f "delims=" %%D in ('dir /s /b /ad obj 2^>nul') do (
    if exist "%%D\" (
        rd /s /q "%%D"
        echo Removed %%D
        set "REMOVED=1"
    )
)

if "%REMOVED%"=="0" (
    echo No bin or obj folders found under %CD%
) else (
    echo Cleaned bin and obj folders under %CD%
)

popd >nul
exit /b 0
