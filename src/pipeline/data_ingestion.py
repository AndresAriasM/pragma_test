# src/pipeline/data_ingestion.py - VERSIÓN CORREGIDA
"""
Data Ingestion Pipeline - Núcleo del Reto
✅ Bronze (Parquet) → Base de Datos + Estadísticas Incrementales
✅ Procesamiento en micro-batches (cumple requerimiento de memoria)
✅ Estadísticas O(1) sin tocar datos ya cargados
✅ Verificación final comparando stats incrementales vs consulta directa
✅ FIXED: Filtrado de valores NaN para consistencia BD ↔ Stats
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
import sys

# Configurar path para imports
current_dir = Path(__file__).parent
src_dir = current_dir.parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

try:
    from pipeline.statistics_engine import IncrementalStatisticsEngine
    from pipeline.database_setup import DatabaseManager
    from config.medallion_config import BRONZE_PATH, EXPECTED_FILE_STEMS
except ImportError as e:
    logging.error(f"Error importando módulos: {e}")
    logging.error("Asegúrate de ejecutar desde la raíz del proyecto")
    sys.exit(1)

logger = logging.getLogger(__name__)

class DataIngestionPipeline:
    """
    Pipeline principal que implementa los requerimientos del reto:
    
    ✅ Cargar archivos CSV → BD (desde Bronze Parquet optimizado)
    ✅ Estadísticas incrementales en tiempo real (count, avg, min, max)
    ✅ NO recalcular desde BD (O(1) vs O(n))
    ✅ Procesamiento en micro-batches (cumple memoria)
    ✅ Verificación final stats incrementales vs consulta directa
    ✅ Procesar validation.csv y mostrar cambios
    ✅ FIXED: Filtrar NaN para mantener consistencia BD ↔ Stats
    """
    
    def __init__(self, 
                 database_config: Optional[Dict[str, Any]] = None,
                 batch_size: int = 1000,
                 enable_persistence: bool = True,
                 project_root: Optional[str] = None):
        """
        Inicializa el pipeline de ingesta de datos
        
        Args:
            database_config: Configuración de BD (None para SQLite por defecto)
            batch_size: Tamaño de micro-batch para procesamiento
            enable_persistence: Si persistir estadísticas en archivo
            project_root: Ruta raíz del proyecto
        """
        self.project_root = Path(project_root) if project_root else Path(__file__).parent.parent.parent
        self.batch_size = batch_size
        
        # Inicializar componentes
        self.db_manager = DatabaseManager(database_config)
        
        # Configurar persistencia de estadísticas
        persistence_file = None
        if enable_persistence:
            persistence_file = str(self.project_root / "data" / "processed" / "pipeline_statistics.json")
        
        self.stats_engine = IncrementalStatisticsEngine(persistence_file=persistence_file)
        
        # Configurar rutas
        self.bronze_path = self.project_root / "data" / "processed" / "bronze"
        
        # Contadores y metadata
        self.files_processed = []
        self.total_files_to_process = 0
        self.total_batches_processed = 0
        self.pipeline_start_time = None
        self.pipeline_end_time = None
        
        logger.info(f"🔄 DataIngestionPipeline inicializado")
        logger.info(f"   Micro-batch size: {self.batch_size:,} filas")
        logger.info(f"   Bronze path: {self.bronze_path}")
        logger.info(f"   BD tipo: {self.db_manager.config['type']}")
        logger.info(f"   Persistencia stats: {enable_persistence}")
    
    def process_parquet_file_to_database(self, parquet_file: Path) -> Dict[str, Any]:
        """
        Procesa un archivo Parquet desde Bronze hacia la base de datos
        ✅ CORE IMPLEMENTATION: Micro-batches + Stats incrementales + BD
        ✅ FIXED: Filtrar NaN antes de procesar para mantener consistencia
        
        Args:
            parquet_file: Ruta al archivo Parquet en Bronze
            
        Returns:
            Dict con resultado del procesamiento
        """
        file_name = parquet_file.name
        logger.info(f"\n🔄 PROCESANDO {file_name} → BASE DE DATOS")
        logger.info(f"📊 Modo: Micro-batches de {self.batch_size:,} filas + Estadísticas O(1)")
        
        if not parquet_file.exists():
            logger.error(f"❌ Archivo no encontrado: {parquet_file}")
            return {'success': False, 'error': f'File not found: {parquet_file}'}
        
        processing_result = {
            'file_name': file_name,
            'success': False,
            'total_rows': 0,
            'batches_processed': 0,
            'processing_start': datetime.now(),
            'processing_end': None,
            'batch_ids': [],
            'stats_before': self.stats_engine.get_current_stats(),
            'stats_after': None,
            'error': None,
            'rows_filtered': 0  # Nuevo: contar filas filtradas
        }
        
        try:
            # ✅ LEER ARCHIVO PARQUET COMPLETO (ya está comprimido ~70% vs CSV)
            logger.info(f"  📖 Leyendo archivo Parquet comprimido...")
            df = pd.read_parquet(parquet_file)
            original_rows = len(df)
            
            logger.info(f"  📊 Archivo cargado: {original_rows:,} filas")
            
            # ✅ PROCESAR EN MICRO-BATCHES DESDE EL DATAFRAME
            batch_count = 0
            total_processed_rows = 0
            
            # Dividir DataFrame en chunks del tamaño especificado
            for start_idx in range(0, original_rows, self.batch_size):
                end_idx = min(start_idx + self.batch_size, original_rows)
                chunk_df = df.iloc[start_idx:end_idx].copy()
                
                batch_count += 1
                initial_chunk_size = len(chunk_df)
                
                logger.info(f"  📦 Micro-batch {batch_count}: {initial_chunk_size} filas")
                
                # ✅ VALIDAR Y FILTRAR DATOS DEL CHUNK
                if not self._validate_chunk_data(chunk_df, file_name, batch_count):
                    logger.warning(f"⚠️ Saltando batch {batch_count} (validación fallida)")
                    continue
                
                final_chunk_size = len(chunk_df)
                rows_filtered_this_batch = initial_chunk_size - final_chunk_size
                processing_result['rows_filtered'] += rows_filtered_this_batch
                
                if final_chunk_size == 0:
                    logger.warning(f"⚠️ Batch {batch_count} vacío después del filtrado")
                    continue
                
                # 1. ✅ INSERTAR EN BASE DE DATOS
                batch_info = {
                    'source_file': file_name,
                    'batch_number': batch_count,
                    'rows_processed': final_chunk_size,
                    'stats_snapshot': json.dumps(self.stats_engine.get_current_stats())
                }
                
                batch_id = self.db_manager.insert_batch(chunk_df, batch_info)
                processing_result['batch_ids'].append(batch_id)
                
                # 2. ✅ ACTUALIZAR ESTADÍSTICAS INCREMENTALES (O(1) - SIN CONSULTAR BD)
                # IMPORTANTE: Solo procesar precios válidos (ya filtrados)
                prices = chunk_df['price'].tolist()
                batch_info_for_stats = {
                    'source_file': file_name,
                    'batch_number': batch_count,
                    'batch_id': batch_id
                }
                
                self.stats_engine.update_batch(prices, batch_info_for_stats)
                current_stats = self.stats_engine.get_current_stats()
                
                # 3. ✅ MOSTRAR PROGRESO EN TIEMPO REAL (REQUERIMIENTO DEL RETO)
                logger.info(f"     💾 BD: ✅ Insertado (batch_id: {batch_id[:8]}...)")
                logger.info(f"     📊 Stats: {self.stats_engine.format_stats()}")
                
                total_processed_rows += final_chunk_size
                
                # Limpiar memoria del chunk
                del chunk_df
                
                # Log progreso cada 5 batches
                if batch_count % 5 == 0:
                    logger.info(f"    📈 Progreso: {total_processed_rows:,} filas válidas procesadas en {batch_count} micro-batches")
            
            # Limpiar DataFrame completo de la memoria
            del df
            
            # Finalizar procesamiento del archivo
            processing_result.update({
                'success': True,
                'total_rows': total_processed_rows,  # Solo filas válidas procesadas
                'batches_processed': batch_count,
                'processing_end': datetime.now(),
                'stats_after': self.stats_engine.get_current_stats()
            })
            
            self.total_batches_processed += batch_count
            
            # ✅ RESUMEN FINAL CON INFO DE FILTRADO
            logger.info(f"✅ {file_name} procesado completamente:")
            logger.info(f"   📊 Filas originales: {original_rows:,}")
            if processing_result['rows_filtered'] > 0:
                logger.info(f"   🧹 Filas filtradas (NaN, etc.): {processing_result['rows_filtered']:,}")
            logger.info(f"   ✅ Filas válidas procesadas: {total_processed_rows:,}")
            logger.info(f"   📦 Micro-batches: {batch_count}")
            logger.info(f"   📈 Stats actuales: {self.stats_engine.format_stats()}")
            
            return processing_result
            
        except Exception as e:
            logger.error(f"❌ Error procesando {file_name}: {e}")
            processing_result.update({
                'success': False,
                'error': str(e),
                'processing_end': datetime.now()
            })
            return processing_result
    
    def _validate_chunk_data(self, chunk_df: pd.DataFrame, file_name: str, batch_number: int) -> bool:
        """
        Valida los datos de un chunk antes de procesarlo
        ✅ FIXED: Filtrar y limpiar datos problemáticos antes de procesar
        
        Args:
            chunk_df: DataFrame del chunk (SE MODIFICA IN-PLACE)
            file_name: Nombre del archivo fuente
            batch_number: Número del batch
            
        Returns:
            bool: True si los datos son válidos
        """
        import pandas as pd
        import numpy as np
        
        required_columns = ['timestamp', 'price', 'user_id', 'source_file']
        missing_columns = [col for col in required_columns if col not in chunk_df.columns]
        
        if missing_columns:
            logger.error(f"❌ {file_name} batch {batch_number}: Faltan columnas {missing_columns}")
            return False
        
        # ✅ CRÍTICO: Contar y filtrar valores NaN en price ANTES de procesar
        initial_count = len(chunk_df)
        nan_prices = chunk_df['price'].isna().sum()
        
        if nan_prices > 0:
            logger.warning(f"⚠️ {file_name} batch {batch_number}: {nan_prices} precios NaN encontrados - FILTRANDO")
            # FILTRAR filas con precios NaN del DataFrame (IN-PLACE)
            chunk_df.dropna(subset=['price'], inplace=True)
            chunk_df.reset_index(drop=True, inplace=True)
            final_count = len(chunk_df)
            logger.info(f"🧹 {file_name} batch {batch_number}: Filtrado {initial_count - final_count} filas con NaN")
        
        # Filtrar precios <= 0 también
        invalid_prices = chunk_df[chunk_df['price'] <= 0]
        if len(invalid_prices) > 0:
            logger.warning(f"⚠️ {file_name} batch {batch_number}: {len(invalid_prices)} precios ≤0 encontrados - FILTRANDO")
            chunk_df = chunk_df[chunk_df['price'] > 0]
            chunk_df.reset_index(drop=True, inplace=True)
        
        # Validar nulos en otros campos críticos
        null_counts = chunk_df[required_columns].isnull().sum()
        total_nulls = null_counts.sum()
        if total_nulls > 0:
            logger.warning(f"⚠️ {file_name} batch {batch_number}: {total_nulls} valores nulos en otros campos")
        
        # Verificar que el chunk aún tiene datos después del filtrado
        if len(chunk_df) == 0:
            logger.warning(f"⚠️ {file_name} batch {batch_number}: Chunk vacío después del filtrado")
            return False
        
        return True
    
    def process_all_bronze_files(self, exclude_validation: bool = True) -> Dict[str, Any]:
        """
        Procesa todos los archivos Parquet de Bronze hacia la base de datos
        ✅ IMPLEMENTA REQUERIMIENTO: Cargar todos los CSV (desde Bronze optimizado)
        
        Args:
            exclude_validation: Si excluir validation.csv (procesar por separado)
            
        Returns:
            Dict con resultado del procesamiento completo
        """
        self.pipeline_start_time = datetime.now()
        
        logger.info("🚀 INICIANDO PIPELINE DE INGESTA DE DATOS")
        logger.info("=" * 60)
        logger.info(f"⚡ Configuración: Micro-batches de {self.batch_size:,} filas")
        logger.info(f"🗄️ Base de datos: {self.db_manager.config['type']}")
        logger.info(f"📊 Estadísticas: Incrementales O(1) (NO recalcula desde BD)")
        
        # Obtener archivos a procesar
        files_to_process = []
        for file_stem in EXPECTED_FILE_STEMS:
            if exclude_validation and file_stem == 'validation':
                continue
            
            parquet_file = self.bronze_path / f"{file_stem}.parquet"
            if parquet_file.exists():
                files_to_process.append(parquet_file)
            else:
                logger.warning(f"⚠️ Archivo Bronze faltante: {parquet_file.name}")
        
        if not files_to_process:
            logger.error("❌ No se encontraron archivos Bronze para procesar")
            return {'success': False, 'error': 'No Bronze files found'}
        
        self.total_files_to_process = len(files_to_process)
        logger.info(f"📋 Archivos a procesar: {self.total_files_to_process}")
        for file in files_to_process:
            logger.info(f"   📄 {file.name}")
        
        # Mostrar estadísticas iniciales
        initial_stats = self.stats_engine.get_current_stats()
        logger.info(f"\n📊 ESTADÍSTICAS INICIALES:")
        logger.info(f"   {self.stats_engine.format_stats()}")
        
        # Procesar archivos secuencialmente (cumple requerimiento de memoria)
        pipeline_result = {
            'success': True,
            'files_processed': [],
            'files_failed': [],
            'total_files': len(files_to_process),
            'total_rows_processed': 0,
            'total_batches_processed': 0,
            'total_rows_filtered': 0,  # Nuevo: total de filas filtradas
            'processing_start': self.pipeline_start_time,
            'processing_end': None,
            'initial_stats': initial_stats,
            'final_stats': None,
            'verification_result': None
        }
        
        # ✅ PROCESAMIENTO SECUENCIAL - UN ARCHIVO A LA VEZ
        for i, parquet_file in enumerate(files_to_process, 1):
            logger.info(f"\n📄 ARCHIVO {i}/{len(files_to_process)}: {parquet_file.name}")
            logger.info("-" * 40)
            
            file_result = self.process_parquet_file_to_database(parquet_file)
            
            if file_result['success']:
                pipeline_result['files_processed'].append(file_result)
                pipeline_result['total_rows_processed'] += file_result['total_rows']
                pipeline_result['total_batches_processed'] += file_result['batches_processed']
                pipeline_result['total_rows_filtered'] += file_result.get('rows_filtered', 0)
                
                logger.info(f"✅ {parquet_file.name}: {file_result['total_rows']:,} filas en {file_result['batches_processed']} batches")
            else:
                pipeline_result['files_failed'].append(file_result)
                logger.error(f"❌ {parquet_file.name}: {file_result.get('error', 'Unknown error')}")
        
        # Finalizar pipeline
        self.pipeline_end_time = datetime.now()
        pipeline_result['processing_end'] = self.pipeline_end_time
        pipeline_result['final_stats'] = self.stats_engine.get_current_stats()
        
        # ✅ VERIFICACIÓN CRÍTICA: Comparar estadísticas incrementales vs consulta directa
        logger.info(f"\n🔍 VERIFICACIÓN FINAL: ESTADÍSTICAS INCREMENTALES VS BASE DE DATOS")
        logger.info("=" * 70)
        
        try:
            db_stats = self.db_manager.get_database_statistics()
            comparison_result = self.stats_engine.compare_with_database_stats(db_stats)
            pipeline_result['verification_result'] = comparison_result
            
            # Guardar verificación en BD para auditoría
            self.db_manager.save_verification_result(
                pipeline_result['final_stats'],
                db_stats,
                comparison_result
            )
            
            if comparison_result['overall_match']:
                logger.info("🎉 VERIFICACIÓN EXITOSA: Las estadísticas incrementales coinciden exactamente con la BD")
            else:
                logger.error("❌ VERIFICACIÓN FALLIDA: Hay diferencias entre estadísticas incrementales y BD")
                pipeline_result['success'] = False
        
        except Exception as e:
            logger.error(f"❌ Error en verificación final: {e}")
            pipeline_result['verification_result'] = {'error': str(e)}
        
        # Resumen final
        self._print_pipeline_summary(pipeline_result)
        
        return pipeline_result
    
    def process_validation_file(self) -> Dict[str, Any]:
        """
        Procesa validation.csv y muestra cómo cambian las estadísticas
        ✅ IMPLEMENTA REQUERIMIENTO: Ejecutar validation.csv y mostrar cambios
        
        Returns:
            Dict con resultado del procesamiento de validación
        """
        logger.info("\n🧪 PROCESANDO ARCHIVO DE VALIDACIÓN")
        logger.info("=" * 45)
        
        validation_file = self.bronze_path / "validation.parquet"
        
        if not validation_file.exists():
            logger.error(f"❌ Archivo de validación no encontrado: {validation_file}")
            return {'success': False, 'error': 'Validation file not found'}
        
        # Capturar estadísticas ANTES de validation
        stats_before = self.stats_engine.get_current_stats()
        logger.info(f"📊 ESTADÍSTICAS ANTES DE VALIDATION:")
        logger.info(f"   {self.stats_engine.format_stats()}")
        
        # Procesar validation.csv
        validation_result = self.process_parquet_file_to_database(validation_file)
        
        if not validation_result['success']:
            logger.error(f"❌ Error procesando validation: {validation_result.get('error')}")
            return validation_result
        
        # Capturar estadísticas DESPUÉS de validation
        stats_after = self.stats_engine.get_current_stats()
        logger.info(f"\n📊 ESTADÍSTICAS DESPUÉS DE VALIDATION:")
        logger.info(f"   {self.stats_engine.format_stats()}")
        
        # ✅ MOSTRAR CAMBIOS (REQUERIMIENTO DEL RETO)
        logger.info(f"\n📈 CAMBIOS DETECTADOS:")
        logger.info("-" * 25)
        
        count_change = stats_after['count'] - stats_before['count']
        avg_change = stats_after['avg'] - stats_before['avg'] if stats_before['count'] > 0 else stats_after['avg']
        
        logger.info(f"   Filas agregadas: +{count_change:,}")
        logger.info(f"   Cambio en promedio: {avg_change:+.2f} (${stats_before['avg']:.2f} → ${stats_after['avg']:.2f})")
        
        if stats_after['min'] < stats_before.get('min', float('inf')):
            logger.info(f"   🔽 Nuevo mínimo: ${stats_after['min']:.2f} (antes: ${stats_before.get('min', 0):.2f})")
        
        if stats_after['max'] > stats_before.get('max', float('-inf')):
            logger.info(f"   🔼 Nuevo máximo: ${stats_after['max']:.2f} (antes: ${stats_before.get('max', 0):.2f})")
        
        # Verificación final después de validation
        logger.info(f"\n🔍 VERIFICACIÓN FINAL DESPUÉS DE VALIDATION:")
        db_stats = self.db_manager.get_database_statistics()
        comparison_result = self.stats_engine.compare_with_database_stats(db_stats)
        
        validation_result.update({
            'stats_before': stats_before,
            'stats_after': stats_after,
            'changes': {
                'count_added': count_change,
                'avg_change': avg_change,
                'new_min': stats_after['min'] < stats_before.get('min', float('inf')),
                'new_max': stats_after['max'] > stats_before.get('max', float('-inf'))
            },
            'final_verification': comparison_result
        })
        
        if comparison_result['overall_match']:
            logger.info("✅ Verificación final exitosa después de validation")
        else:
            logger.error("❌ Verificación final fallida después de validation")
        
        return validation_result
    
    def _print_pipeline_summary(self, pipeline_result: Dict[str, Any]):
        """
        Imprime resumen completo del pipeline
        """
        duration = (self.pipeline_end_time - self.pipeline_start_time).total_seconds()
        
        logger.info(f"\n🎯 RESUMEN FINAL DEL PIPELINE")
        logger.info("=" * 40)
        logger.info(f"⏱️ Duración total: {duration:.2f} segundos")
        logger.info(f"📄 Archivos procesados: {len(pipeline_result['files_processed'])}/{pipeline_result['total_files']}")
        logger.info(f"📊 Total filas procesadas: {pipeline_result['total_rows_processed']:,}")
        
        # ✅ NUEVO: Mostrar información de filtrado
        if pipeline_result.get('total_rows_filtered', 0) > 0:
            logger.info(f"🧹 Total filas filtradas (NaN/inválidas): {pipeline_result['total_rows_filtered']:,}")
        
        logger.info(f"📦 Total micro-batches: {pipeline_result['total_batches_processed']:,}")
        logger.info(f"⚡ Rendimiento: {pipeline_result['total_rows_processed']/duration:.0f} filas/segundo")
        
        # Estadísticas finales
        logger.info(f"\n{self.stats_engine.format_detailed_stats()}")
        
        # Resumen de BD
        try:
            batch_summary = self.db_manager.get_batch_summary()
            logger.info(f"\n🗄️ RESUMEN DE BASE DE DATOS:")
            logger.info(f"   Total batches en BD: {batch_summary.get('total_batches', 0)}")
            logger.info(f"   Batches exitosos: {batch_summary.get('completed_batches', 0)}")
            logger.info(f"   Tasa de éxito: {batch_summary.get('success_rate', 0):.1f}%")
        except Exception as e:
            logger.warning(f"⚠️ No se pudo obtener resumen de BD: {e}")
        
        # Performance del motor de estadísticas
        perf_metrics = self.stats_engine.get_performance_metrics()
        logger.info(f"\n⚡ PERFORMANCE DE ESTADÍSTICAS:")
        logger.info(f"   Eficiencia: {perf_metrics.get('processing_efficiency', 'N/A')}")
        logger.info(f"   Uso de memoria: {perf_metrics.get('memory_usage', 'N/A')}")
        
        # Estado final
        verification = pipeline_result.get('verification_result', {})
        if verification.get('overall_match'):
            logger.info(f"\n🎉 PIPELINE COMPLETADO EXITOSAMENTE")
            logger.info(f"✅ Estadísticas incrementales verificadas correctamente")
        else:
            logger.error(f"\n❌ PIPELINE COMPLETADO CON ERRORES")
            logger.error(f"❌ Verificación de estadísticas fallida")
    
    def run_complete_pipeline(self) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo: archivos principales + validation
        ✅ IMPLEMENTA TODOS LOS REQUERIMIENTOS DEL RETO
        
        Returns:
            Dict con resultado completo
        """
        logger.info("🚀 EJECUTANDO PIPELINE COMPLETO - RETO DE INGENIERÍA DE DATOS")
        logger.info("=" * 70)
        
        complete_result = {
            'main_pipeline': None,
            'validation_pipeline': None,
            'overall_success': False,
            'execution_start': datetime.now(),
            'execution_end': None
        }
        
        try:
            # 1. Procesar archivos principales (2012-1 a 2012-5)
            logger.info("📋 FASE 1: PROCESANDO ARCHIVOS PRINCIPALES (2012-1 a 2012-5)")
            main_result = self.process_all_bronze_files(exclude_validation=True)
            complete_result['main_pipeline'] = main_result
            
            if not main_result['success']:
                logger.error("❌ Error en procesamiento de archivos principales")
                return complete_result
            
            # 2. Procesar validation.csv
            logger.info("\n📋 FASE 2: PROCESANDO ARCHIVO DE VALIDACIÓN")
            validation_result = self.process_validation_file()
            complete_result['validation_pipeline'] = validation_result
            
            if not validation_result['success']:
                logger.error("❌ Error en procesamiento de validation")
                return complete_result
            
            complete_result['overall_success'] = True
            
            # Resumen final completo
            logger.info(f"\n🎉 PIPELINE COMPLETO EJECUTADO EXITOSAMENTE")
            logger.info("=" * 50)
            
            total_rows = main_result['total_rows_processed'] + validation_result['total_rows']
            total_batches = main_result['total_batches_processed'] + validation_result['batches_processed']
            
            logger.info(f"📊 TOTALES FINALES:")
            logger.info(f"   Archivos procesados: {main_result['total_files'] + 1}")
            logger.info(f"   Filas totales: {total_rows:,}")
            logger.info(f"   Micro-batches totales: {total_batches:,}")
            logger.info(f"   Verificación final: {'✅ EXITOSA' if validation_result.get('final_verification', {}).get('overall_match') else '❌ FALLIDA'}")
            
            return complete_result
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline completo: {e}")
            complete_result['error'] = str(e)
            return complete_result
        
        finally:
            complete_result['execution_end'] = datetime.now()
    
    def cleanup(self):
        """
        Limpia recursos del pipeline
        """
        try:
            self.db_manager.close()
            logger.info("🧹 Recursos del pipeline liberados")
        except Exception as e:
            logger.error(f"❌ Error en cleanup: {e}")


def main():
    """
    Función principal para ejecutar el pipeline completo
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("🚀 INICIANDO DATA INGESTION PIPELINE")
    logger.info("Implementando requerimientos completos del reto de ingeniería de datos")
    
    pipeline = None
    try:
        # Crear y ejecutar pipeline
        pipeline = DataIngestionPipeline(
            batch_size=1000,  # Micro-batches de 1000 filas
            enable_persistence=True
        )
        
        # Ejecutar pipeline completo
        result = pipeline.run_complete_pipeline()
        
        if result['overall_success']:
            logger.info("🎉 RETO COMPLETADO EXITOSAMENTE")
            return 0
        else:
            logger.error("❌ RETO COMPLETADO CON ERRORES")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}")
        return 1
    
    finally:
        if pipeline:
            pipeline.cleanup()


if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)