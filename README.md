# Procesador de Tráfico de Red con Spark + DuckDB

Sistema de análisis de tráfico de red para generar archivos Parquet optimizados para Power BI, procesando 2.8M+ flujos de red con detección de amenazas y análisis de comportamiento.

## Estructura del Proyecto

```
C:.
│   README.md
│   requirements.txt
│
├───dashboard
│       dashboard_red.pbix
│       Theme.json
│
├───data
│   └───raw
│           network_data.csv
│
├───docker
│       docker-compose.yml
│       Dockerfile
│
├───logs
│       processing.log
│
├───notebooks
│       Analisis_Exploratorio_BigData.ipynb
│
├───output
│       network_traffic_powerbi.parquet
│
└───src
        main.py
```

## Requisitos del Sistema

- Windows 10/11 con Docker Desktop
- Docker Desktop en funcionamiento
- Mínimo 4GB RAM disponible
- Espacio en disco: 50GB+ recomendado

## Instalación y Configuración

### Usando Docker (Recomendado)

```bash
# Clonar el repositorio
git clone <repository-url>
cd traffic-network-processor

# Construir y ejecutar con Docker Compose
docker-compose up --build
```

### Instalación Local

```bash
# Instalar dependencias Python
pip install -r requirements.txt

# Ejecutar el procesador principal
python src/main.py
```

## Uso del Sistema

### Procesamiento de Datos

El sistema procesa automáticamente los datos de red desde `data/raw/network_data.csv` y genera:

- Archivo Parquet optimizado en `output/network_traffic_powerbi.parquet`
- Logs detallados en `logs/processing.log`
- Dashboard Power BI en `dashboard/dashboard_red.pbix`

### Análisis Exploratorio

Utilice el notebook Jupyter para análisis detallado:

```bash
jupyter notebook notebooks/Analisis_Exploratorio_BigData.ipynb
```

## Características del Dataset

### Dimensiones Principales
- **Total de registros**: 2,830,736 flujos de red
- **Volumen procesado**: 44.06 GB de tráfico
- **Paquetes analizados**: 55,920,908 paquetes
- **Variables**: 83 columnas originales, 14 optimizadas para Power BI

### Distribución de Seguridad
- **Tráfico Benigno**: 2,273,090 flujos (80.3%)
- **Tráfico Malicioso**: 557,646 flujos (19.7%)
- **Flujos de Alto Riesgo**: 140,337 flujos (4.96%)

## Análisis de Resultados

### Top Protocolos por Volumen

| Protocolo | Puerto | Flujos | Porcentaje | Velocidad Promedio |
|-----------|---------|---------|------------|-------------------|
| DNS | 53 | 957,971 | 33.8% | 503 KB/s |
| HTTP | 80 | 618,934 | 21.9% | 236 KB/s |
| HTTPS | 443 | 505,710 | 17.9% | 300 KB/s |

### Principales Tipos de Ataque Detectados

| Tipo de Ataque | Flujos | Porcentaje | Características |
|----------------|---------|------------|-----------------|
| DoS Hulk | 231,073 | 8.16% | 9.5 paquetes/flujo, 8KB promedio |
| PortScan | 158,930 | 5.61% | 2.0 paquetes/flujo, 13 bytes promedio |
| DDoS | 128,027 | 4.52% | 7.7 paquetes/flujo, 7.4KB promedio |
| DoS GoldenEye | 10,293 | 0.36% | 9.6 paquetes/flujo, 7KB promedio |
| FTP-Patator | 7,938 | 0.28% | 13.3 paquetes/flujo, 154 bytes promedio |

### Patrones de Comunicación

**Distribución por Tipo de Flujo:**
- Bidireccional: 2,377,219 flujos (83.98%)
- Unidireccional: 453,517 flujos (16.02%)

**Distribución por Duración:**
- Instantáneo (<1s): 2,043,034 flujos (72.2%)
- Corto (1-10s): 279,831 flujos (9.9%)
- Medio (10-60s): 111,888 flujos (4.0%)
- Largo (60-300s): 395,983 flujos (14.0%)

### Niveles de Riesgo

| Nivel | Flujos | Porcentaje | Criterios |
|-------|---------|------------|-----------|
| BAJO | 2,398,000 | 84.7% | Tráfico normal |
| MEDIO | 292,399 | 10.3% | Anomalías menores |
| ALTO | 98,808 | 3.5% | Patrones sospechosos |
| CRÍTICO | 41,529 | 1.5% | Amenazas confirmadas |

## Configuración del Dashboard

### Power BI
1. Abrir `dashboard/dashboard_red.pbix`
2. Actualizar conexión de datos apuntando a `output/network_traffic_powerbi.parquet`
3. Aplicar tema personalizado desde `dashboard/Theme.json`

### Métricas Clave Incluidas
- Distribución de tipos de ataque
- Análisis temporal de amenazas
- Patrones de tráfico por protocolo
- Métricas de rendimiento de red
- Indicadores de riesgo en tiempo real

## Estructura de Datos de Salida

El archivo Parquet generado contiene las siguientes columnas optimizadas:

- `flow_id`: Identificador único del flujo
- `src_ip`, `dst_ip`: IPs de origen y destino
- `src_port`, `dst_port`: Puertos de origen y destino
- `protocol`: Protocolo de comunicación
- `timestamp`: Marca temporal
- `total_packets`, `total_bytes`: Métricas de volumen
- `flow_duration`: Duración del flujo
- `flow_speed`: Velocidad calculada
- `port_category`: Categorización de puerto
- `communication_type`: Tipo de comunicación
- `duration_category`: Categoría temporal
- `risk_level`: Nivel de riesgo asignado
- `label`: Clasificación de seguridad

## Monitoreo y Logs

### Archivos de Log
- `logs/processing.log`: Log detallado del procesamiento
- Rotación automática de logs por tamaño
- Niveles: INFO, WARNING, ERROR, DEBUG

### Métricas de Rendimiento
- Tiempo de procesamiento por lote
- Memoria utilizada
- Registros procesados por segundo
- Errores y excepciones

## Mantenimiento

### Actualización de Datos
```bash
# Reemplazar archivo de datos
cp nuevo_dataset.csv data/raw/network_data.csv

# Re-ejecutar procesamiento
docker-compose restart
```

### Limpieza del Sistema
```bash
# Limpiar logs antiguos
docker-compose exec app rm -rf logs/*.log.old

# Limpiar archivos temporales
docker system prune -f
```

## Solución de Problemas

### Errores Comunes

**Error de memoria insuficiente:**
- Aumentar límites de memoria en Docker Desktop
- Procesar datos en lotes más pequeños

**Archivo Parquet corrupto:**
- Verificar integridad del CSV original
- Re-ejecutar el procesamiento completo

**Dashboard no actualiza:**
- Verificar permisos de archivo Parquet
- Actualizar conexión de datos en Power BI

### Contacto y Soporte

Para reportar problemas o solicitar mejoras, crear un issue en el repositorio del proyecto.

## Licencia

Este proyecto está licenciado bajo los términos especificados en el archivo LICENSE.