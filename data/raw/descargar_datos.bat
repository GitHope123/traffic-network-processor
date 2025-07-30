@echo off
setlocal

echo Verificando si Python está instalado...
python --version >nul 2>&1
if errorlevel 1 (
    echo Python no está instalado. Por favor instálalo primero.
    pause
    exit /b
)

echo Verificando si gdown está instalado...
pip show gdown >nul 2>&1
if errorlevel 1 (
    echo Instalando gdown con pip...
    pip install gdown
)

echo Creando carpeta data\raw si no existe...
if not exist data\raw (
    mkdir data\raw
)

echo Descargando network_data.csv desde Google Drive...
gdown --id 1ntyU09zN_W_vzyG3vAx8cie9P99HHYNk -O data\raw\network_data.csv

if exist data\raw\network_data.csv (
    echo Descarga completada: data\raw\network_data.csv
) else (
    echo Error en la descarga.
)

pause
