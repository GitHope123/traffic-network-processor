#!/usr/bin/env python3
"""
Procesador de tráfico de red OPTIMIZADO para resolver problemas de memoria
VERSIÓN MEJORADA - Maneja datasets grandes sin OutOfMemoryError
"""

import os
import time
import logging
import duckdb
import gc
import glob
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel

# Configuración de logs
# flata limpieza de filas y aplicar todos los procedimientos de optimización para mostrarlo en power bi
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NetworkProcessor:
    def __init__(self):
        logger.info("🚀 Inicializando SparkSession OPTIMIZADA...")
        
        self.spark = SparkSession.builder \
            .appName("NetworkTrafficProcessor-Optimized") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryo.unsafe", "true") \
            .config("spark.kryoserializer.buffer.max", "1024m") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.default.parallelism", "100") \
            .config("spark.sql.files.maxPartitionBytes", "128MB") \
            .config("spark.driver.memoryFraction", "0.8") \
            .config("spark.executor.memoryFraction", "0.8") \
            .config("spark.storage.memoryFraction", "0.3") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ SparkSession OPTIMIZADA inicializada correctamente v.2.0")

    def load_data(self, input_path):
        """Carga datos CSV con optimizaciones de memoria"""
        logger.info(f"📂 Cargando datos desde: {input_path}")
        
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"❌ Archivo no encontrado: {input_path}")
        
        # Cargar CSV con particionado optimizado
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", '"') \
            .option("maxFilesPerTrigger", 1) \
            .csv(input_path)

        # Limpiar nombres de columnas
        logger.info("🧹 Limpiando nombres de columnas...")
        original_columns = df.columns
        cleaned_columns = [col_name.strip() for col_name in df.columns]
        df = df.toDF(*cleaned_columns)
        
        # Reparticionar para optimizar memoria - CORRECCIÓN DEFINITIVA
        num_partitions = df.rdd.getNumPartitions()
        logger.info(f"🔄 Particiones originales: {num_partitions}")
        
        # Calcular particiones óptimas de forma simple y segura
        if num_partitions > 200:
            optimal_partitions = 200
        elif num_partitions < 4:
            optimal_partitions = 4
        else:
            optimal_partitions = num_partitions
            
        df = df.repartition(optimal_partitions)
        
        # Cache con nivel de almacenamiento optimizado
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        
        record_count = df.count()
        column_count = len(df.columns)
        
        logger.info(f"✅ Datos cargados exitosamente:")
        logger.info(f"   📊 Registros: {record_count:,}")
        logger.info(f"   📋 Columnas: {column_count}")
        logger.info(f"   🔄 Particiones finales: {df.rdd.getNumPartitions()}")
        
        return df

    def clean_and_enhance_data(self, df):
        """Limpia y enriquece los datos con optimizaciones de memoria"""
        logger.info("🔧 Iniciando limpieza y enriquecimiento de datos OPTIMIZADO...")
        
        # Mapeo de columnas
        column_mapping = {
            'Destination Port': 'destination_port',
            'Flow Duration': 'flow_duration', 
            'Total Fwd Packets': 'total_fwd_packets',
            'Total Backward Packets': 'total_backward_packets',
            'Total Length of Fwd Packets': 'total_length_of_fwd_packets',
            'Total Length of Bwd Packets': 'total_length_of_bwd_packets',
            'Fwd Packet Length Max': 'fwd_packet_length_max',
            'Bwd Packet Length Max': 'bwd_packet_length_max',
            'Flow Bytes/s': 'flow_bytes_per_sec',
            'Flow Packets/s': 'flow_packets_per_sec',
            'Flow IAT Mean': 'flow_iat_mean',
            'Fwd IAT Mean': 'fwd_iat_mean',
            'Bwd IAT Mean': 'bwd_iat_mean',
            'Fwd Header Length': 'fwd_header_length',
            'Label': 'label'
        }
        
        # Renombrar columnas existentes
        logger.info("📝 Renombrando columnas...")
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
                logger.info(f"   ✓ {old_name} → {new_name}")
        
        # Aplicar limpieza de datos en lotes
        logger.info("🧹 Aplicando limpieza de datos...")
        numeric_columns = ['total_fwd_packets', 'total_backward_packets', 
                          'total_length_of_fwd_packets', 'total_length_of_bwd_packets',
                          'flow_duration', 'fwd_header_length']
        
        for col_name in numeric_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, coalesce(col(col_name), lit(0)))
        
        # Métricas calculadas con optimización
        logger.info("📊 Calculando métricas derivadas...")
        
        # Procesar en grupos para evitar problemas de memoria
        df = df.withColumn("total_packets",
                           coalesce(col("total_fwd_packets"), lit(0)) +
                           coalesce(col("total_backward_packets"), lit(0)))
        
        df = df.withColumn("total_bytes", 
                           coalesce(col("total_length_of_fwd_packets"), lit(0)) + 
                           coalesce(col("total_length_of_bwd_packets"), lit(0)))
        
        # Ratios con manejo seguro de división por cero
        df = df.withColumn("fwd_bwd_packet_ratio",
                           when(col("total_backward_packets") > 0,
                                col("total_fwd_packets") / col("total_backward_packets"))
                           .otherwise(lit(0)))
        
        df = df.withColumn("bytes_per_packet",
                           when(col("total_packets") > 0,
                                col("total_bytes") / col("total_packets"))
                           .otherwise(lit(0)))
        
        # Velocidades si no existen
        if 'flow_bytes_per_sec' not in df.columns:
            df = df.withColumn("flow_bytes_per_sec",
                               when((col("flow_duration") > 0) & (col("flow_duration").isNotNull()),
                                    col("total_bytes") * 1000000 / col("flow_duration"))
                               .otherwise(lit(0)))
        
        if 'flow_packets_per_sec' not in df.columns:
            df = df.withColumn("flow_packets_per_sec",
                               when((col("flow_duration") > 0) & (col("flow_duration").isNotNull()),
                                    col("total_packets") * 1000000 / col("flow_duration"))
                               .otherwise(lit(0)))
        
        # Categorización optimizada
        logger.info("🏷️  Aplicando categorización...")
        
        df = df.withColumn("port_category", 
                           when(col("destination_port") == 80, "HTTP")
                           .when(col("destination_port") == 443, "HTTPS")
                           .when(col("destination_port") == 22, "SSH")
                           .when(col("destination_port") == 21, "FTP")
                           .when(col("destination_port") == 25, "SMTP")
                           .when(col("destination_port") == 53, "DNS")
                           .when((col("destination_port") >= 1) & (col("destination_port") <= 1023), "SYSTEM")
                           .when((col("destination_port") >= 1024) & (col("destination_port") <= 49151), "USER")
                           .when(col("destination_port") >= 49152, "DYNAMIC")
                           .otherwise("UNKNOWN"))
        
        # Clasificación de amenazas
        df = df.withColumn("threat_level", 
                           when(col("label") == "BENIGN", "NONE")
                           .when(col("label").rlike("(?i)dos|ddos"), "CRITICAL")
                           .when(col("label").rlike("(?i)bot|malware"), "HIGH") 
                           .when(col("label").rlike("(?i)brute|force"), "HIGH")
                           .when(col("label").rlike("(?i)infiltration|backdoor"), "CRITICAL")
                           .when(col("label").rlike("(?i)scan|probe"), "MEDIUM")
                           .otherwise("MEDIUM"))
        
        df = df.withColumn("threat_category",
                           when(col("label") == "BENIGN", "NORMAL")
                           .when(col("label").rlike("(?i)dos"), "DENIAL_OF_SERVICE")
                           .when(col("label").rlike("(?i)bot"), "BOTNET")
                           .when(col("label").rlike("(?i)brute"), "BRUTE_FORCE")
                           .when(col("label").rlike("(?i)infiltration"), "INFILTRATION")
                           .when(col("label").rlike("(?i)scan"), "RECONNAISSANCE")
                           .otherwise("OTHER_ATTACK"))
        
        # Flags de análisis
        logger.info("🚩 Generando flags de análisis...")
        
        df = df.withColumn("is_suspicious",
                           when((col("total_packets") > 1000) |
                                (col("flow_duration") > 10000000) |
                                (col("total_bytes") > 1000000) |
                                (col("flow_bytes_per_sec") > 1000000), True)
                           .otherwise(False))
        
        df = df.withColumn("is_high_volume",
                           when(col("total_bytes") > 500000, True).otherwise(False))
        
        df = df.withColumn("is_long_duration",
                           when(col("flow_duration") > 5000000, True).otherwise(False))
        
        df = df.withColumn("is_high_speed",
                           when(col("flow_bytes_per_sec") > 100000, True).otherwise(False))
        
        # Timestamps
        df = df.withColumn("processing_timestamp", current_timestamp())
        df = df.withColumn("processing_date", current_date())
        
        # Repersist con nueva estructura
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        
        # Estadísticas finales
        processed_count = df.count()
        logger.info(f"✅ Procesamiento completado:")
        logger.info(f"   📊 Registros procesados: {processed_count:,}")
        logger.info(f"   🏷️  Columnas finales: {len(df.columns)}")
        
        # Contar amenazas de forma segura
        try:
            threat_counts = df.groupBy("threat_level").count().collect()
            logger.info("   🎯 Distribución de amenazas:")
            for row in threat_counts:
                logger.info(f"     - {row['threat_level']}: {row['count']:,}")
        except Exception as e:
            logger.warning(f"   ⚠️  No se pudo calcular distribución: {str(e)}")
        
        return df

    def save_single_parquet_simple(self, df, output_path, filename="network_traffic_powerbi.parquet"):
        """Guarda Parquet usando método simple y robusto"""
        logger.info("💾 Guardando archivo Parquet (método simplificado)...")
        
        os.makedirs(output_path, exist_ok=True)
        file_path = os.path.join(output_path, filename)
        
        # Columnas seleccionadas para Power BI
        columns_for_powerbi = [
            "destination_port", "flow_duration", "total_packets", "total_bytes",
            "total_fwd_packets", "total_backward_packets", "bytes_per_packet",
            "fwd_bwd_packet_ratio", "flow_bytes_per_sec", "flow_packets_per_sec",
            "label", "threat_level", "threat_category", "port_category",
            "is_suspicious", "is_high_volume", "is_long_duration", "is_high_speed",
            "processing_timestamp", "processing_date"
        ]
        
        # Filtrar columnas existentes
        available_columns = [col for col in columns_for_powerbi if col in df.columns]
        df_final = df.select(*available_columns)
        
        logger.info(f"📋 Columnas seleccionadas: {len(available_columns)}")
        
        try:
            # MÉTODO 1: Pandas + DuckDB (recomendado)
            logger.info("🔄 Método 1: Convertir a Pandas y usar DuckDB...")
            
            # Reducir particiones para optimizar conversión
            df_optimized = df_final.coalesce(2)
            
            # Convertir a Pandas
            pandas_df = df_optimized.toPandas()
            logger.info(f"✅ Convertido a Pandas: {len(pandas_df):,} registros")
            
            # Guardar con DuckDB
            conn = duckdb.connect()
            conn.register('df_data', pandas_df)
            
            conn.execute(f"""
                COPY df_data TO '{file_path}' 
                (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 50000)
            """)
            
            conn.close()
            
            # Estadísticas
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            
            logger.info("✅ Archivo Parquet creado exitosamente:")
            logger.info(f"   📁 Ubicación: {file_path}")
            logger.info(f"   📊 Registros: {len(pandas_df):,}")
            logger.info(f"   📋 Columnas: {len(available_columns)}")
            logger.info(f"   💾 Tamaño: {file_size_mb:.2f} MB")
            logger.info(f"   🗜️  Compresión: ZSTD")
            
            # Limpiar memoria
            del pandas_df
            gc.collect()
            
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Método 1 falló: {str(e)}")
            logger.info("🔄 Intentando Método 2: Spark nativo...")
            
            try:
                # MÉTODO 2: Spark nativo con post-procesamiento
                temp_dir = os.path.join(output_path, "temp_parquet")
                
                # Limpiar directorio temporal si existe
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                
                # Guardar usando Spark (crea múltiples archivos)
                df_final.coalesce(1).write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(temp_dir)
                
                # Buscar archivo Parquet generado
                parquet_files = glob.glob(os.path.join(temp_dir, "part-*.parquet"))
                
                if parquet_files:
                    # Mover el archivo al destino final
                    shutil.move(parquet_files[0], file_path)
                    
                    # Limpiar directorio temporal
                    shutil.rmtree(temp_dir)
                    
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    
                    logger.info("✅ Método 2 exitoso:")
                    logger.info(f"   📁 Ubicación: {file_path}")
                    logger.info(f"   💾 Tamaño: {file_size_mb:.2f} MB")
                    logger.info(f"   🗜️  Compresión: Snappy")
                    
                    return file_path
                else:
                    raise Exception("No se generaron archivos Parquet")
                    
            except Exception as e2:
                logger.error(f"❌ Método 2 también falló: {str(e2)}")
                raise Exception(f"Ambos métodos fallaron. Método 1: {str(e)}, Método 2: {str(e2)}")

    def process_for_powerbi(self, input_path, output_path):
        """Proceso principal OPTIMIZADO para Power BI"""
        start_time = time.time()
        logger.info("="*80)
        logger.info("🚀 INICIANDO PROCESAMIENTO OPTIMIZADO PARA POWER BI")
        logger.info("="*80)
        
        try:
            # 1. Cargar datos
            df_raw = self.load_data(input_path)
            
            # 2. Procesar y enriquecer
            df_processed = self.clean_and_enhance_data(df_raw)
            
            # 3. Guardar archivo Parquet
            parquet_path = self.save_single_parquet_simple(df_processed, output_path)
            
            # 4. Estadísticas finales
            total_time = time.time() - start_time
            
            logger.info("="*80)
            logger.info("✅ PROCESAMIENTO COMPLETADO EXITOSAMENTE")
            logger.info("="*80)
            logger.info(f"⏱️  Tiempo total: {total_time:.2f} segundos")
            logger.info(f"📁 Archivo: {parquet_path}")
            logger.info("="*80)
            
        except Exception as e:
            logger.error(f"❌ Error durante el procesamiento: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        
    def cleanup(self):
        """Limpia recursos de Spark"""
        logger.info("🧹 Cerrando SparkSession...")
        if hasattr(self, 'spark'):
            self.spark.stop()
        logger.info("✅ Recursos liberados")

if __name__ == "__main__":
    processor = NetworkProcessor()
    try:
        processor.process_for_powerbi(
            input_path="/data/raw/network_data.csv",
            output_path="/output"
        )
    except Exception as e:
        logger.error(f"💥 Procesamiento fallido: {str(e)}")
        exit(1)
    finally:
        processor.cleanup()