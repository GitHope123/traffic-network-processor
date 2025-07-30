# 🚀 Procesador de Tráfico de Red con Spark + DuckDB

[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-green.svg)](https://www.python.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Compatible-yellow.svg)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-red.svg)](LICENSE)

Sistema avanzado de análisis de tráfico de red que procesa **2.8M+ flujos de red** para generar archivos Parquet optimizados para Power BI, con capacidades de detección de amenazas y análisis de comportamiento en tiempo real.

---

## 📋 Tabla de Contenidos

- [🏗️ Estructura del Proyecto](#️-estructura-del-proyecto)
- [⚙️ Requisitos del Sistema](#️-requisitos-del-sistema)
- [🚀 Instalación y Configuración](#-instalación-y-configuración)
- [💻 Uso del Sistema](#-uso-del-sistema)
- [📊 Características del Dataset](#-características-del-dataset)
- [📈 Análisis de Resultados](#-análisis-de-resultados)
- [🎯 Configuración del Dashboard](#-configuración-del-dashboard)
- [🔧 Mantenimiento](#-mantenimiento)
- [🛠️ Solución de Problemas](#️-solución-de-problemas)

---

## 🏗️ Estructura del Proyecto

```
traffic-network-processor/
│
├── 📁 dashboard/
│   ├── dashboard_red.pbix          # Dashboard principal de Power BI
│   └── Theme.json                  # Tema personalizado para Power BI
│
├── 📁 data/
│   └── raw/
│       ├── descargar_datos.bat     # Script de descarga de datos
│       └── network_data.csv        # Dataset principal (2.8M+ registros)
│
├── 📁 docker/
│   ├── docker-compose.yml          # Configuración de servicios Docker
│   └── Dockerfile                  # Imagen personalizada de procesamiento
│
├── 📁 logs/
│   └── processing.log              # Logs detallados del sistema
│
├── 📁 notebooks/
│   └── Analisis_Exploratorio_BigData.ipynb  # Análisis exploratorio
│
├── 📁 output/
│   └── network_traffic_powerbi.parquet      # Datos procesados y optimizados
│
├── 📁 src/
│   └── main.py                     # Motor principal de procesamiento
│
├── .gitignore                      # Archivos excluidos del control de versiones
├── README.md                       # Documentación del proyecto
└── requirements.txt                # Dependencias de Python
```

---

## ⚙️ Requisitos del Sistema

### 🖥️ Hardware Mínimo
- **SO**: Windows 10/11, macOS 10.15+, o Linux Ubuntu 18.04+
- **RAM**: 4GB mínimo, 8GB recomendado
- **Almacenamiento**: 50GB+ disponible
- **Procesador**: Intel i5 o AMD Ryzen 5 equivalente

### 🐳 Software Requerido
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v4.0+)
- [Git](https://git-scm.com/) para clonación del repositorio
- [Power BI Desktop](https://powerbi.microsoft.com/desktop/) (opcional, para dashboards)

---

## 🚀 Instalación y Configuración

### Método 1: Docker (Recomendado) 🐳

```bash
# 1. Clonar el repositorio
git clone https://github.com/GitHope123/traffic-network-processor.git
cd traffic-network-processor

# 2. Verificar que Docker esté ejecutándose
docker --version

# 3. Construir y ejecutar el sistema
docker-compose up --build

# 4. Verificar el estado de los contenedores
docker-compose ps
```

### Método 2: Instalación Local 🏠

```bash
# 1. Crear entorno virtual (recomendado)
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar el procesador
python src/main.py
```

---

## 💻 Uso del Sistema

### 📥 Descarga del Dataset

Para descargar directamente el archivo `network_data.csv` en `data/raw/` sin crear subcarpetas adicionales, ejecuta el siguiente script desde PowerShell:

```powershell
cd data/raw
.\descargar_datos.bat
```

**¿Qué hace este script?**

Este script realiza las siguientes acciones:

1. ✅ Verifica si Python está instalado en tu sistema
2. 🔍 Verifica si `gdown` está disponible; si no lo está, lo instala automáticamente
3. ⬇️ Utiliza `gdown` para descargar el archivo desde Google Drive
4. 💾 Guarda el archivo directamente en la carpeta `data/raw/` como `network_data.csv`

> **Nota**: Si al ejecutar el script aparece un mensaje de error relacionado con permisos de ejecución, abre PowerShell como administrador y ejecuta:
> ```powershell
> Set-ExecutionPolicy RemoteSigned
> ```

### 🔄 Procesamiento Automático

El sistema ejecuta automáticamente las siguientes tareas:

1. **Carga de datos**: Lee `data/raw/network_data.csv`
2. **Procesamiento**: Aplica transformaciones con Spark + DuckDB
3. **Optimización**: Genera archivo Parquet optimizado
4. **Logging**: Registra métricas y eventos en tiempo real

### 📊 Análisis Exploratorio

Para realizar análisis detallado:

```bash
# Iniciar Jupyter Notebook
jupyter notebook notebooks/Analisis_Exploratorio_BigData.ipynb
```

### 🎯 Archivos de Salida

| Archivo | Descripción | Ubicación |
|---------|-------------|-----------|
| `network_traffic_powerbi.parquet` | Datos procesados para Power BI | `output/` |
| `processing.log` | Logs detallados del sistema | `logs/` |
| `dashboard_red.pbix` | Dashboard interactivo | `dashboard/` |

---

## 📊 Características del Dataset

### 🔢 Dimensiones Principales

| Métrica | Valor | Descripción |
|---------|-------|-------------|
| **Total de Flujos** | 2,830,736 | Flujos de red únicos procesados |
| **Volumen de Datos** | 44.06 GB | Tráfico total analizado |
| **Paquetes Procesados** | 55,920,908 | Paquetes individuales examinados |
| **Variables Originales** | 83 | Columnas en dataset raw |
| **Variables Optimizadas** | 14 | Columnas en salida final |

### 🛡️ Distribución de Seguridad

```
┌─────────────────────────────────────────────────────────┐
│                DISTRIBUCIÓN DE TRÁFICO                  │
├─────────────────────────────────────────────────────────┤
│ 🟢 Tráfico Benigno    │ 2,273,090 flujos │ 80.3%      │
│ 🔴 Tráfico Malicioso  │   557,646 flujos │ 19.7%      │
│ ⚠️  Alto Riesgo       │   140,337 flujos │  4.96%     │
└─────────────────────────────────────────────────────────┘
```

---

## 📈 Análisis de Resultados

### 🌐 Top Protocolos por Volumen

| Protocolo | Puerto | Flujos | % Total | Vel. Promedio | Características |
|-----------|---------|---------|---------|---------------|-----------------|
| **DNS** | 53 | 957,971 | 33.8% | 503 KB/s | Resolución de nombres |
| **HTTP** | 80 | 618,934 | 21.9% | 236 KB/s | Tráfico web no cifrado |
| **HTTPS** | 443 | 505,710 | 17.9% | 300 KB/s | Tráfico web seguro |
| **SSH** | 22 | 89,234 | 3.2% | 45 KB/s | Acceso remoto seguro |
| **FTP** | 21 | 34,567 | 1.2% | 89 KB/s | Transferencia de archivos |

### 🚨 Principales Tipos de Ataque Detectados

| Tipo de Ataque | Flujos | % del Total | Paquetes/Flujo | Tamaño Promedio | Severidad |
|----------------|---------|-------------|----------------|-----------------|-----------|
| **DoS Hulk** | 231,073 | 8.16% | 9.5 | 8 KB | 🔴 Alta |
| **PortScan** | 158,930 | 5.61% | 2.0 | 13 bytes | 🟡 Media |
| **DDoS** | 128,027 | 4.52% | 7.7 | 7.4 KB | 🔴 Alta |
| **DoS GoldenEye** | 10,293 | 0.36% | 9.6 | 7 KB | 🟠 Media-Alta |
| **FTP-Patator** | 7,938 | 0.28% | 13.3 | 154 bytes | 🟡 Media |
| **SSH-Patator** | 5,897 | 0.21% | 15.8 | 298 bytes | 🟡 Media |

### 📡 Patrones de Comunicación

#### Distribución por Tipo de Flujo
```
Bidireccional:      ████████████████████████████████████████ 83.98% (2,377,219)
Unidireccional:     ████████                                16.02% (453,517)
```

#### Distribución por Duración
```
Instantáneo (<1s):  ████████████████████████████████████ 72.2% (2,043,034)
Corto (1-10s):      ████                                  9.9% (279,831)
Medio (10-60s):     ██                                    4.0% (111,888)
Largo (60-300s):    ██████                               14.0% (395,983)
```

### ⚠️ Niveles de Riesgo

| Nivel | Flujos | Porcentaje | Criterios | Acción Recomendada |
|-------|---------|------------|-----------|-------------------|
| 🟢 **BAJO** | 2,398,000 | 84.7% | Tráfico normal, patrones esperados | Monitoreo rutinario |
| 🟡 **MEDIO** | 292,399 | 10.3% | Anomalías menores detectadas | Revisión periódica |
| 🟠 **ALTO** | 98,808 | 3.5% | Patrones sospechosos identificados | Investigación requerida |
| 🔴 **CRÍTICO** | 41,529 | 1.5% | Amenazas confirmadas | Acción inmediata |

---

## 🎯 Configuración del Dashboard

### 📊 Power BI Setup

1. **Abrir Dashboard**
   ```
   Archivo → Abrir → dashboard/dashboard_red.pbix
   ```

2. **Actualizar Fuente de Datos**
   ```
   Transformar datos → Configuración de origen de datos
   → Cambiar origen → output/network_traffic_powerbi.parquet
   ```

3. **Aplicar Tema Personalizado**
   ```
   Ver → Temas → Examinar temas → dashboard/Theme.json
   ```

### 🎨 Métricas Clave Incluidas

- 📈 **Análisis Temporal**: Evolución de amenazas por hora/día
- 🌍 **Geolocalización**: Mapas de origen de ataques
- 🔍 **Detección de Anomalías**: Patrones inusuales en tiempo real
- 📊 **KPIs de Seguridad**: Métricas de riesgo y alertas
- 🚀 **Rendimiento de Red**: Latencia, throughput, packet loss

---

## 🗄️ Estructura de Datos de Salida

El archivo Parquet optimizado contiene las siguientes columnas:

| Campo | Tipo | Descripción | Ejemplo |
|-------|------|-------------|---------|
| `flow_id` | string | ID único del flujo | "FL_2024_001234" |
| `src_ip` | string | IP de origen | "192.168.1.100" |
| `dst_ip` | string | IP de destino | "8.8.8.8" |
| `src_port` | int | Puerto origen | 52341 |
| `dst_port` | int | Puerto destino | 443 |
| `protocol` | string | Protocolo usado | "HTTPS" |
| `timestamp` | datetime | Marca temporal | "2024-01-15 14:30:25" |
| `total_packets` | bigint | Paquetes totales | 127 |
| `total_bytes` | bigint | Bytes totales | 98304 |
| `flow_duration` | float | Duración (segundos) | 15.7 |
| `flow_speed` | float | Velocidad (KB/s) | 6.26 |
| `port_category` | string | Categoría del puerto | "Web_Secure" |
| `communication_type` | string | Tipo de comunicación | "Bidirectional" |
| `duration_category` | string | Categoría temporal | "Medium" |
| `risk_level` | string | Nivel de riesgo | "LOW" |
| `label` | string | Clasificación | "BENIGN" |

---

## 📋 Monitoreo y Logs

### 📄 Sistema de Logging

```bash
# Ver logs en tiempo real
tail -f logs/processing.log

# Buscar errores específicos
grep "ERROR" logs/processing.log

# Analizar rendimiento
grep "Performance" logs/processing.log
```

### 📊 Métricas de Rendimiento

| Métrica | Valor Típico | Umbral Crítico |
|---------|--------------|----------------|
| Tiempo de procesamiento | 45-60 min | >90 min |
| Memoria utilizada | 3-4 GB | >6 GB |
| Registros/segundo | 1,000-1,500 | <500 |
| Tasa de error | <0.1% | >1% |

---

## 🔧 Mantenimiento

### 🔄 Actualización de Datos

```bash
# Método 1: Reemplazo manual
cp nuevo_dataset.csv data/raw/network_data.csv
docker-compose restart

# Método 2: Script automático
cd data/raw
./descargar_datos.bat
```

### 🧹 Limpieza del Sistema

```bash
# Limpiar logs antiguos (>30 días)
find logs/ -name "*.log.old" -mtime +30 -delete

# Limpiar caché de Docker
docker system prune -f

# Limpiar archivos temporales
rm -rf tmp/ .cache/
```

### 📅 Cronograma de Mantenimiento

- **Diario**: Verificación de logs y métricas
- **Semanal**: Actualización de datos y limpieza
- **Mensual**: Revisión de rendimiento y optimización
- **Trimestral**: Actualización de dependencias

---

## 🛠️ Solución de Problemas

### ❌ Errores Comunes

#### 🧠 Error de Memoria Insuficiente
```bash
# Síntoma: "OutOfMemoryError" o container killed
# Solución:
docker-compose down
# Ir a Docker Desktop Settings > Resources > Aumentar memoria
docker-compose up --build
```

#### 💾 Archivo Parquet Corrupto
```bash
# Síntoma: Error al cargar en Power BI
# Solución:
rm output/network_traffic_powerbi.parquet
python src/main.py --force-rebuild
```

#### 🔄 Dashboard No Actualiza Datos
```bash
# Verificar permisos
chmod 644 output/network_traffic_powerbi.parquet

# En Power BI:
# Datos > Configuración de origen de datos > Actualizar
```

#### 🐳 Container No Inicia
```bash
# Verificar puertos en uso
netstat -tulpn | grep :8080

# Reiniciar Docker
docker-compose down
docker system prune -f
docker-compose up --build
```

### 🆘 Diagnóstico Rápido

```bash
# 1. Verificar estado del sistema
docker-compose ps
docker-compose logs app

# 2. Verificar archivos de salida
ls -la output/
file output/network_traffic_powerbi.parquet

# 3. Verificar logs por errores
tail -n 100 logs/processing.log | grep -i error

# 4. Verificar espacio en disco
df -h

# 5. Verificar uso de memoria
free -h
```