@echo off
setlocal

echo ================================
echo   Descargando archivo CSV
echo ================================

:: Verificar instalación de Python
echo Verificando Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo Python no esta instalado. Por favor instálalo primero.
    pause
    exit /b
)

:: Verificar instalación de gdown
echo Verificando gdown...
pip show gdown >nul 2>&1
if errorlevel 1 (
    echo Instalando gdown con pip...
    pip install gdown
)

:: Descargar el archivo directamente en el directorio actual
echo Descargando archivo network_data.csv en el directorio actual...
gdown --id 1ntyU09zN_W_vzyG3vAx8cie9P99HHYNk -O network_data.csv

:: Verificar descarga
if exist network_data.csv (
    echo Descarga completada: network_data.csv
) else (
    echo Error en la descarga.
)

pause
