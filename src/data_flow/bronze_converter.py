#src/data_flow/bronze_converter.py
"""
Bronze Converter - Convierte CSV a Parquet para la capa Bronze
Parte de la arquitectura Medallion del pipeline de datos
"""

import os
import logging
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeConverter:
    """
    Convertidor de datos CSV a Parquet para la capa Bronze
    """
    
    def __init__(self, base_path: str = None):
        """
        Initialize the BronzeConverter
        
        Args:
            base_path: Ruta base del proyecto
        """
        if base_path is None:
            base_path = Path(__file__).parent.parent.parent
        
        self.base_path = Path(base_path)
        self.raw_data_path = self.base_path / "data" / "raw"
        self.bronze_path = self.base_path / "data" / "processed" / "bronze"
        
        # Crear directorio bronze si no existe
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"BronzeConverter inicializado")
        logger.info(f"Datos raw: {self.raw_data_path}")
        logger.info(f"Capa Bronze: {self.bronze_path}")
        
        # ✅ Importar configuración de micro-batches
        try:
            from config.medallion_config import BRONZE_CONFIG
            self.bronze_config = BRONZE_CONFIG
            self.micro_batch_size = BRONZE_CONFIG.get("micro_batch_size", 1000)
            logger.info(f"⚡ Micro-batch size: {self.micro_batch_size:,} filas")
        except ImportError:
            logger.warning("⚠️ No se pudo importar configuración medallion, usando valores por defecto")
            self.micro_batch_size = 1000
            self.bronze_config = {}
        
        # Configuración de Parquet
        self.parquet_config = {
            "compression": self.bronze_config.get("compression", "snappy"),
            "row_group_size": self.bronze_config.get("row_group_size", 50000),
            "page_size": self.bronze_config.get("page_size", 8192),
            "use_dictionary": self.bronze_config.get("use_dictionary", True),
            "write_statistics": self.bronze_config.get("write_statistics", True)
        }
    
    def find_csv_folder(self) -> Optional[Path]:
        """
        Encuentra la carpeta que contiene los archivos CSV
        
        Returns:
            Path de la carpeta con CSVs o None
        """
        logger.info("🔍 Buscando carpeta con archivos CSV...")
        
        # Buscar en subdirectorios de raw
        for item in self.raw_data_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                csv_files = list(item.glob("*.csv"))
                if len(csv_files) >= 5:
                    logger.info(f"📁 Carpeta encontrada: {item.name} ({len(csv_files)} archivos CSV)")
                    return item
        
        # Si no hay subdirectorios, buscar directamente en raw
        csv_files = list(self.raw_data_path.glob("*.csv"))
        if len(csv_files) >= 5:
            logger.info(f"📁 Archivos CSV encontrados directamente en raw ({len(csv_files)} archivos)")
            return self.raw_data_path
        
        logger.error("❌ No se encontró carpeta con archivos CSV")
        return None
    
    def get_csv_files(self) -> List[Path]:
        """
        Obtiene lista de archivos CSV a procesar
        
        Returns:
            Lista de rutas de archivos CSV
        """
        csv_folder = self.find_csv_folder()
        if not csv_folder:
            return []
        
        # Filtrar solo los archivos que necesitamos
        expected_patterns = ["2012-*.csv", "validation.csv"]
        csv_files = []
        
        for pattern in expected_patterns:
            csv_files.extend(csv_folder.glob(pattern))
        
        # Ordenar archivos para procesamiento consistente
        csv_files.sort(key=lambda x: x.name)
        
        logger.info(f"📋 Archivos CSV encontrados: {[f.name for f in csv_files]}")
        return csv_files
    
    def validate_csv_schema(self, df: pd.DataFrame, file_name: str) -> Dict[str, Any]:
        """
        Valida el esquema del CSV
        
        Args:
            df: DataFrame a validar
            file_name: Nombre del archivo para logs
            
        Returns:
            Dict con resultado de validación
        """
        validation_result = {
            "valid": True,
            "warnings": [],
            "errors": [],
            "row_count": len(df),
            "column_count": len(df.columns)
        }
        
        # Verificar columnas requeridas
        required_columns = ["timestamp", "price", "user_id"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            validation_result["valid"] = False
            validation_result["errors"].append(f"Faltan columnas: {missing_columns}")
        
        # Verificar tipos de datos
        if "price" in df.columns:
            non_numeric_prices = df[~pd.to_numeric(df["price"], errors="coerce").notna()]
            if len(non_numeric_prices) > 0:
                validation_result["warnings"].append(f"{len(non_numeric_prices)} precios no numéricos")
        
        # Verificar valores nulos
        null_counts = df.isnull().sum()
        for col, null_count in null_counts.items():
            if null_count > 0:
                percentage = (null_count / len(df)) * 100
                validation_result["warnings"].append(f"{col}: {null_count} nulos ({percentage:.2f}%)")
        
        # Log resultados
        if validation_result["valid"]:
            logger.info(f"✅ {file_name}: Esquema válido ({validation_result['row_count']:,} filas)")
        else:
            logger.error(f"❌ {file_name}: Esquema inválido - {validation_result['errors']}")
        
        if validation_result["warnings"]:
            for warning in validation_result["warnings"]:
                logger.warning(f"⚠️ {file_name}: {warning}")
        
        return validation_result
    
    def convert_csv_to_parquet_microbatch(self, csv_path: Path, batch_size: int = 1000) -> Tuple[bool, Optional[Path]]:
        """
        Convierte un archivo CSV a Parquet usando micro-batches
        ✅ CUMPLE REQUERIMIENTO: No carga archivo completo en memoria
        
        Args:
            csv_path: Ruta del archivo CSV
            batch_size: Tamaño del micro-batch (filas por batch)
            
        Returns:
            Tuple[bool, Optional[Path]]: (éxito, ruta del archivo parquet)
        """
        try:
            file_name = csv_path.stem
            parquet_path = self.bronze_path / f"{file_name}.parquet"
            
            logger.info(f"🔄 Convirtiendo {csv_path.name} → {parquet_path.name} (micro-batches de {batch_size})")
            
            # Definir esquema PyArrow para optimización
            schema = pa.schema([
                ("timestamp", pa.string()),
                ("price", pa.float64()),
                ("user_id", pa.string()),
                ("source_file", pa.string()),
                ("bronze_created_at", pa.string()),
                ("bronze_created_by", pa.string())
            ])
            
            # Variables para tracking
            total_rows_processed = 0
            batch_count = 0
            first_batch = True
            validation_passed = True
            
            # ✅ PROCESAMIENTO EN MICRO-BATCHES - NO CARGA TODO EN MEMORIA
            csv_chunks = pd.read_csv(
                csv_path,
                chunksize=batch_size,
                dtype={"user_id": "string"},
                parse_dates=False
            )
            
            # Crear writer de Parquet para escritura incremental
            parquet_writer = None
            
            try:
                for chunk_df in csv_chunks:
                    batch_count += 1
                    batch_rows = len(chunk_df)
                    
                    logger.info(f"  📦 Procesando micro-batch {batch_count}: {batch_rows} filas")
                    
                    # Validar esquema solo en el primer batch
                    if first_batch:
                        validation = self.validate_csv_schema(chunk_df, csv_path.name)
                        if not validation["valid"]:
                            logger.error(f"❌ Esquema inválido en {csv_path.name}")
                            validation_passed = False
                            break
                        first_batch = False
                    
                    # Agregar metadatos al chunk
                    chunk_df["source_file"] = csv_path.name
                    chunk_df["bronze_created_at"] = datetime.now().isoformat()
                    chunk_df["bronze_created_by"] = "bronze_converter"
                    
                    # Convertir chunk a PyArrow Table
                    table = pa.Table.from_pandas(chunk_df, schema=schema)
                    
                    # Escribir de forma incremental
                    if parquet_writer is None:
                        # Primer batch: crear writer
                        parquet_writer = pq.ParquetWriter(
                            parquet_path,
                            schema,
                            compression=self.parquet_config["compression"],
                            use_dictionary=self.parquet_config["use_dictionary"],
                            write_statistics=self.parquet_config["write_statistics"]
                        )
                    
                    # Escribir tabla del batch
                    parquet_writer.write_table(table)
                    total_rows_processed += batch_rows
                    
                    # Log progreso cada 5 batches
                    if batch_count % 5 == 0:
                        logger.info(f"    📊 Progreso: {total_rows_processed:,} filas procesadas en {batch_count} batches")
                    
                    # ✅ IMPORTANTE: Limpiar memoria del chunk
                    del chunk_df, table
                
            finally:
                # Cerrar writer
                if parquet_writer is not None:
                    parquet_writer.close()
            
            if not validation_passed:
                return False, None
            
            # Verificar archivo creado
            if parquet_path.exists():
                parquet_size = parquet_path.stat().st_size
                csv_size = csv_path.stat().st_size
                compression_ratio = (1 - parquet_size / csv_size) * 100
                
                logger.info(f"✅ {parquet_path.name} creado exitosamente")
                logger.info(f"   📊 Total procesado: {total_rows_processed:,} filas en {batch_count} micro-batches")
                logger.info(f"   💾 CSV: {self.format_size(csv_size)} → Parquet: {self.format_size(parquet_size)}")
                logger.info(f"   🗜️ Compresión: {compression_ratio:.1f}%")
                logger.info(f"   ⚡ Tamaño promedio batch: {total_rows_processed // batch_count if batch_count > 0 else 0} filas")
                
                return True, parquet_path
            else:
                logger.error(f"❌ No se pudo crear {parquet_path.name}")
                return False, None
                
        except Exception as e:
            logger.error(f"❌ Error convirtiendo {csv_path.name}: {str(e)}")
            return False, None
    
    def convert_csv_to_parquet(self, csv_path: Path) -> Tuple[bool, Optional[Path]]:
        """
        Wrapper que usa micro-batches para cumplir requerimientos de memoria
        """
        # Usar micro-batches de 1000 filas por defecto
        return self.convert_csv_to_parquet_microbatch(csv_path, batch_size=1000)
    
    def format_size(self, size_bytes: int) -> str:
        """Formatea tamaño de archivo"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"
    
    def get_parquet_info(self, parquet_path: Path) -> Dict[str, Any]:
        """
        Obtiene información de un archivo Parquet
        
        Args:
            parquet_path: Ruta del archivo Parquet
            
        Returns:
            Dict con información del archivo
        """
        try:
            # Leer metadatos sin cargar datos
            parquet_file = pq.ParquetFile(parquet_path)
            metadata = parquet_file.metadata
            
            return {
                "file_name": parquet_path.name,
                "file_size": parquet_path.stat().st_size,
                "file_size_formatted": self.format_size(parquet_path.stat().st_size),
                "row_count": metadata.num_rows,
                "column_count": len(parquet_file.schema),
                "row_groups": metadata.num_row_groups,
                "compression": str(metadata.row_group(0).column(0).compression),
                "schema": [field.name for field in parquet_file.schema],
                "created": datetime.fromtimestamp(parquet_path.stat().st_mtime).isoformat()
            }
        except Exception as e:
            return {"error": str(e)}
    
    def convert_all_csv_to_bronze(self) -> Dict[str, Any]:
        """
        Convierte todos los archivos CSV a Parquet en la capa Bronze usando micro-batches
        ✅ CUMPLE REQUERIMIENTO: Procesa archivos de uno en uno, cada uno en micro-batches
        
        Returns:
            Dict con resumen de la conversión
        """
        logger.info("🥉 INICIANDO CONVERSIÓN A CAPA BRONZE CON MICRO-BATCHES")
        logger.info("=" * 60)
        logger.info(f"⚡ Tamaño de micro-batch: {self.micro_batch_size:,} filas")
        logger.info(f"💾 Modo de memoria: Optimizado (no carga archivos completos)")
        
        csv_files = self.get_csv_files()
        if not csv_files:
            logger.error("❌ No se encontraron archivos CSV para procesar")
            return {"success": False, "message": "No CSV files found"}
        
        results = {
            "success": True,
            "total_files": len(csv_files),
            "converted_files": 0,
            "failed_files": 0,
            "total_rows": 0,
            "total_batches": 0,
            "total_size_original": 0,
            "total_size_compressed": 0,
            "files_processed": [],
            "errors": [],
            "micro_batch_stats": {
                "batch_size_used": self.micro_batch_size,
                "memory_optimized": True,
                "incremental_write": True
            }
        }
        
        # ✅ PROCESAMIENTO SECUENCIAL - UN ARCHIVO A LA VEZ (no todos en memoria)
        for i, csv_path in enumerate(csv_files, 1):
            logger.info(f"\n📄 Procesando archivo {i}/{len(csv_files)}: {csv_path.name}")
            logger.info(f"📊 Memoria: Solo este archivo será procesado en micro-batches")
            
            # Usar micro-batches configurables
            success, parquet_path = self.convert_csv_to_parquet_microbatch(
                csv_path, 
                batch_size=self.micro_batch_size
            )
            
            if success and parquet_path:
                results["converted_files"] += 1
                
                # Obtener información del archivo
                parquet_info = self.get_parquet_info(parquet_path)
                csv_size = csv_path.stat().st_size
                
                # Calcular número de batches procesados
                file_rows = parquet_info.get("row_count", 0)
                file_batches = (file_rows + self.micro_batch_size - 1) // self.micro_batch_size
                
                results["total_rows"] += file_rows
                results["total_batches"] += file_batches
                results["total_size_original"] += csv_size
                results["total_size_compressed"] += parquet_info.get("file_size", 0)
                
                results["files_processed"].append({
                    "csv_file": csv_path.name,
                    "parquet_file": parquet_path.name,
                    "rows": file_rows,
                    "batches_processed": file_batches,
                    "csv_size": csv_size,
                    "parquet_size": parquet_info.get("file_size", 0)
                })
                
                logger.info(f"✅ {csv_path.name} → {file_rows:,} filas en {file_batches} micro-batches")
            else:
                results["failed_files"] += 1
                results["errors"].append(f"Error procesando {csv_path.name}")
                logger.error(f"❌ Error procesando {csv_path.name}")
        
        # Resumen final con estadísticas de micro-batches
        if results["converted_files"] > 0:
            compression_ratio = (1 - results["total_size_compressed"] / results["total_size_original"]) * 100
            avg_batch_size = results["total_rows"] / results["total_batches"] if results["total_batches"] > 0 else 0
            
            logger.info("\n🎉 CONVERSIÓN A BRONZE COMPLETADA")
            logger.info("=" * 50)
            logger.info(f"📊 Resumen de procesamiento:")
            logger.info(f"   Archivos procesados: {results['converted_files']}/{results['total_files']}")
            logger.info(f"   Total de filas: {results['total_rows']:,}")
            logger.info(f"   Total micro-batches: {results['total_batches']:,}")
            logger.info(f"   Tamaño promedio batch: {avg_batch_size:.0f} filas")
            logger.info(f"   Tamaño original: {self.format_size(results['total_size_original'])}")
            logger.info(f"   Tamaño comprimido: {self.format_size(results['total_size_compressed'])}")
            logger.info(f"   Compresión total: {compression_ratio:.1f}%")
            
            logger.info(f"\n💾 Optimización de memoria:")
            logger.info(f"   ✅ Micro-batch size: {self.micro_batch_size:,} filas")
            logger.info(f"   ✅ Archivos procesados uno a la vez")
            logger.info(f"   ✅ Memoria liberada entre batches")
            logger.info(f"   ✅ Escritura incremental de Parquet")
            
            if results["failed_files"] > 0:
                logger.warning(f"⚠️ {results['failed_files']} archivos fallaron")
                for error in results["errors"]:
                    logger.error(f"   {error}")
        else:
            logger.error("❌ No se pudo convertir ningún archivo")
            results["success"] = False
        
        return results
    
    def verify_bronze_layer(self) -> bool:
        """
        Verifica la integridad de la capa Bronze
        
        Returns:
            bool: True si la verificación es exitosa
        """
        logger.info("\n🔍 VERIFICANDO CAPA BRONZE")
        logger.info("-" * 30)
        
        expected_files = ["2012-1", "2012-2", "2012-3", "2012-4", "2012-5", "validation"]
        all_valid = True
        
        for file_stem in expected_files:
            parquet_path = self.bronze_path / f"{file_stem}.parquet"
            
            if parquet_path.exists():
                info = self.get_parquet_info(parquet_path)
                if "error" in info:
                    logger.error(f"❌ {file_stem}.parquet: {info['error']}")
                    all_valid = False
                else:
                    logger.info(f"✅ {file_stem}.parquet: {info['row_count']:,} filas, {info['file_size_formatted']}")
            else:
                logger.error(f"❌ Falta: {file_stem}.parquet")
                all_valid = False
        
        if all_valid:
            logger.info("🎉 Capa Bronze verificada correctamente")
        else:
            logger.error("❌ Hay problemas en la capa Bronze")
        
        return all_valid


def main():
    """
    Función principal para ejecutar la conversión a Bronze
    """
    try:
        # Inicializar el convertidor
        converter = BronzeConverter()
        
        # Convertir archivos CSV a Parquet
        results = converter.convert_all_csv_to_bronze()
        
        if results["success"]:
            # Verificar capa Bronze
            verification_success = converter.verify_bronze_layer()
            
            if verification_success:
                logger.info("✅ Proceso de conversión a Bronze completado exitosamente")
                return 0
            else:
                logger.error("❌ Error en la verificación de la capa Bronze")
                return 1
        else:
            logger.error("❌ Error durante la conversión a Bronze")
            return 1
            
    except KeyboardInterrupt:
        logger.info("❌ Conversión cancelada por el usuario")
        return 1
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)