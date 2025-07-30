# Dockerfile para el procesador de red con Spark + DuckDB
FROM openjdk:11-jdk-slim

# Variables de entorno para Spark
ENV SPARK_VERSION=3.4.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/local/openjdk-11 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/app/src \
    PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

# Instalar dependencias del sistema y Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    procps \
    ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Crear estructura de carpetas y usuario
RUN useradd -m -u 1000 spark && \
    mkdir -p /app /data /output /app/logs && \
    chown -R spark:spark /app /data /output

# Descargar e instalar Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    chown -R spark:spark $SPARK_HOME

# Copiar e instalar dependencias de Python
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /app/requirements.txt

# Copiar el código fuente
COPY src/ /app/src/
RUN chown -R spark:spark /app

# Cambiar a usuario no root
USER spark

# Establecer directorio de trabajo
WORKDIR /app

# Comando por defecto al iniciar el contenedor
CMD ["python3", "/app/src/main.py"]