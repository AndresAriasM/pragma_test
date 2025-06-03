# src/pipeline/master_pipeline.py
"""
🚀 MASTER PIPELINE - COORDINADOR COMPLETO
=========================================
Ejecuta todos los pasos del pipeline en secuencia:
1. Descarga de datos desde Google Drive
2. Conversión CSV → Bronze (Parquet)
3. Ingesta Bronze → Base de Datos
4. Procesamiento de validation
5. Generación de reportes

✅ Pipeline completo orquestado desde Streamlit
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import json

# Configurar path
current_dir = Path(__file__).parent
src_dir = current_dir.parent
project_root = src_dir.parent

if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(project_root / "logs" / f"master_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)

class MasterPipeline:
    """
    Pipeline maestro que coordina todos los pasos del proceso
    """
    
    def __init__(self, batch_size: int = 1000, enable_stats: bool = True):
        """
        Inicializa el pipeline maestro
        
        Args:
            batch_size: Tamaño de micro-batch para procesamiento
            enable_stats: Si habilitar estadísticas incrementales
        """
        self.batch_size = batch_size
        self.enable_stats = enable_stats
        self.project_root = project_root
        self.execution_start = None
        self.execution_end = None
        
        # Crear directorios necesarios
        self._ensure_directories()
        
        logger.info("🚀 MasterPipeline inicializado")
        logger.info(f"📁 Proyecto: {self.project_root}")
        logger.info(f"⚡ Batch size: {self.batch_size}")
        logger.info(f"📊 Estadísticas: {'Habilitadas' if self.enable_stats else 'Deshabilitadas'}")
    
    def _ensure_directories(self):
        """Crea directorios necesarios"""
        dirs_to_create = [
            self.project_root / "data" / "raw",
            self.project_root / "data" / "processed" / "bronze",
            self.project_root / "data" / "processed" / "silver",
            self.project_root / "data" / "processed" / "gold",
            self.project_root / "logs"
        ]
        
        for directory in dirs_to_create:
            directory.mkdir(parents=True, exist_ok=True)
    
    def run_complete_pipeline(self) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo paso a paso
        
        Returns:
            Dict con resultado de la ejecución completa
        """
        self.execution_start = datetime.now()
        
        logger.info("🚀 INICIANDO PIPELINE MAESTRO COMPLETO")
        logger.info("=" * 60)
        
        pipeline_result = {
            'overall_success': False,
            'execution_start': self.execution_start.isoformat(),
            'execution_end': None,
            'steps_completed': [],
            'steps_failed': [],
            'total_duration': None,
            'steps': {}
        }
        
        try:
            # PASO 1: Descarga de datos
            logger.info("\n📥 PASO 1: DESCARGA DE DATOS")
            logger.info("-" * 40)
            
            download_result = self._step_download_data()
            pipeline_result['steps']['download'] = download_result
            
            if download_result['success']:
                pipeline_result['steps_completed'].append('download')
                logger.info("✅ Descarga completada exitosamente")
            else:
                pipeline_result['steps_failed'].append('download')
                logger.error("❌ Error en descarga de datos")
                # No continuar si la descarga falla
                return self._finalize_result(pipeline_result)
            
            # PASO 2: Conversión a Bronze
            logger.info("\n🥉 PASO 2: CONVERSIÓN A BRONZE")
            logger.info("-" * 40)
            
            bronze_result = self._step_convert_to_bronze()
            pipeline_result['steps']['bronze'] = bronze_result
            
            if bronze_result['success']:
                pipeline_result['steps_completed'].append('bronze')
                logger.info("✅ Conversión Bronze completada exitosamente")
            else:
                pipeline_result['steps_failed'].append('bronze')
                logger.error("❌ Error en conversión Bronze")
                return self._finalize_result(pipeline_result)
            
            # PASO 3: Ingesta a Base de Datos
            logger.info("\n🗄️ PASO 3: INGESTA A BASE DE DATOS")
            logger.info("-" * 40)
            
            ingestion_result = self._step_data_ingestion()
            pipeline_result['steps']['ingestion'] = ingestion_result
            
            if ingestion_result['success']:
                pipeline_result['steps_completed'].append('ingestion')
                logger.info("✅ Ingesta completada exitosamente")
            else:
                pipeline_result['steps_failed'].append('ingestion')
                logger.error("❌ Error en ingesta de datos")
                return self._finalize_result(pipeline_result)
            
            # PASO 4: Procesamiento de Validation
            logger.info("\n🧪 PASO 4: PROCESAMIENTO DE VALIDATION")
            logger.info("-" * 40)
            
            validation_result = self._step_process_validation()
            pipeline_result['steps']['validation'] = validation_result
            
            if validation_result['success']:
                pipeline_result['steps_completed'].append('validation')
                logger.info("✅ Validation completado exitosamente")
            else:
                pipeline_result['steps_failed'].append('validation')
                logger.warning("⚠️ Warning en validation (no crítico)")
            
            # PASO 5: Generación de reportes
            logger.info("\n📊 PASO 5: GENERACIÓN DE REPORTES")
            logger.info("-" * 40)
            
            report_result = self._step_generate_reports()
            pipeline_result['steps']['reports'] = report_result
            
            if report_result['success']:
                pipeline_result['steps_completed'].append('reports')
                logger.info("✅ Reportes generados exitosamente")
            else:
                pipeline_result['steps_failed'].append('reports')
                logger.warning("⚠️ Warning en reportes (no crítico)")
            
            # Verificar éxito general
            critical_steps = ['download', 'bronze', 'ingestion']
            critical_success = all(step in pipeline_result['steps_completed'] for step in critical_steps)
            
            if critical_success:
                pipeline_result['overall_success'] = True
                logger.info("🎉 PIPELINE MAESTRO COMPLETADO EXITOSAMENTE")
            else:
                logger.error("❌ PIPELINE MAESTRO FALLÓ EN PASOS CRÍTICOS")
            
        except Exception as e:
            logger.error(f"❌ Error crítico en pipeline maestro: {e}")
            pipeline_result['critical_error'] = str(e)
        
        return self._finalize_result(pipeline_result)
    
    def _step_download_data(self) -> Dict[str, Any]:
        """Ejecuta descarga de datos"""
        try:
            from data_flow.download_data import DataDownloader
            
            downloader = DataDownloader(base_path=str(self.project_root))
            success, zip_path = downloader.download_challenge_data(force_download=False)
            
            if success:
                verification = downloader.verify_downloaded_data()
                return {
                    'success': True,
                    'zip_path': zip_path,
                    'verification_passed': verification,
                    'message': 'Datos descargados y verificados correctamente'
                }
            else:
                return {
                    'success': False,
                    'error': 'Error en descarga de datos',
                    'zip_path': zip_path
                }
        
        except Exception as e:
            logger.error(f"Error en descarga: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _step_convert_to_bronze(self) -> Dict[str, Any]:
        """Ejecuta conversión a Bronze"""
        try:
            from data_flow.bronze_converter import BronzeConverter
            
            converter = BronzeConverter(base_path=str(self.project_root))
            result = converter.convert_all_csv_to_bronze()
            
            if result['success']:
                verification = converter.verify_bronze_layer()
                return {
                    'success': True,
                    'files_converted': result.get('converted_files', 0),
                    'total_rows': result.get('total_rows', 0),
                    'verification_passed': verification,
                    'message': f"Conversión Bronze exitosa: {result.get('converted_files', 0)} archivos"
                }
            else:
                return {
                    'success': False,
                    'error': result.get('message', 'Error en conversión Bronze'),
                    'details': result
                }
        
        except Exception as e:
            logger.error(f"Error en conversión Bronze: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _step_data_ingestion(self) -> Dict[str, Any]:
        """Ejecuta ingesta de datos a BD"""
        try:
            from pipeline.data_ingestion import DataIngestionPipeline
            
            # Configurar pipeline de ingesta
            ingestion_pipeline = DataIngestionPipeline(
                batch_size=self.batch_size,
                enable_persistence=self.enable_stats,
                project_root=str(self.project_root)
            )
            
            # Ejecutar solo archivos principales (sin validation)
            main_result = ingestion_pipeline.process_all_bronze_files(exclude_validation=True)
            
            if main_result['success']:
                return {
                    'success': True,
                    'files_processed': main_result.get('total_files', 0),
                    'rows_processed': main_result.get('total_rows_processed', 0),
                    'batches_processed': main_result.get('total_batches_processed', 0),
                    'verification_passed': main_result.get('verification_result', {}).get('overall_match', False),
                    'message': f"Ingesta exitosa: {main_result.get('total_rows_processed', 0):,} filas procesadas"
                }
            else:
                return {
                    'success': False,
                    'error': 'Error en ingesta de datos',
                    'details': main_result
                }
        
        except Exception as e:
            logger.error(f"Error en ingesta: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _step_process_validation(self) -> Dict[str, Any]:
        """Ejecuta procesamiento de validation"""
        try:
            from pipeline.data_ingestion import DataIngestionPipeline
            
            # Reutilizar pipeline de ingesta para validation
            ingestion_pipeline = DataIngestionPipeline(
                batch_size=self.batch_size,
                enable_persistence=self.enable_stats,
                project_root=str(self.project_root)
            )
            
            validation_result = ingestion_pipeline.process_validation_file()
            
            if validation_result['success']:
                changes = validation_result.get('changes', {})
                return {
                    'success': True,
                    'rows_added': changes.get('count_added', 0),
                    'avg_change': changes.get('avg_change', 0),
                    'final_verification': validation_result.get('final_verification', {}).get('overall_match', False),
                    'message': f"Validation procesado: +{changes.get('count_added', 0)} filas"
                }
            else:
                return {
                    'success': False,
                    'error': validation_result.get('error', 'Error procesando validation'),
                    'details': validation_result
                }
        
        except Exception as e:
            logger.error(f"Error en validation: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _step_generate_reports(self) -> Dict[str, Any]:
        """Genera reportes finales"""
        try:
            report_data = self._generate_pipeline_report()
            
            # Guardar reporte en archivo
            report_file = self.project_root / "logs" / f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(report_file, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            # Generar reporte de texto también
            text_report = self._generate_text_report(report_data)
            text_file = self.project_root / "logs" / f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            with open(text_file, 'w') as f:
                f.write(text_report)
            
            return {
                'success': True,
                'report_file': str(report_file),
                'text_file': str(text_file),
                'report_data': report_data,
                'message': 'Reportes generados correctamente'
            }
        
        except Exception as e:
            logger.error(f"Error generando reportes: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _generate_pipeline_report(self) -> Dict[str, Any]:
        """Genera datos para el reporte"""
        report = {
            'pipeline_execution': {
                'start_time': self.execution_start.isoformat() if self.execution_start else None,
                'end_time': datetime.now().isoformat(),
                'batch_size': self.batch_size,
                'stats_enabled': self.enable_stats
            },
            'data_files': {},
            'database': {},
            'statistics': {}
        }
        
        try:
            # Información de archivos Bronze
            bronze_path = self.project_root / "data" / "processed" / "bronze"
            if bronze_path.exists():
                bronze_files = list(bronze_path.glob("*.parquet"))
                report['data_files']['bronze_count'] = len(bronze_files)
                report['data_files']['bronze_files'] = [f.name for f in bronze_files]
            
            # Información de base de datos
            db_path = self.project_root / "data" / "pipeline.db"
            if db_path.exists():
                import sqlite3
                conn = sqlite3.connect(str(db_path))
                
                # Contar registros
                cursor = conn.execute("SELECT COUNT(*) FROM transactions")
                total_records = cursor.fetchone()[0]
                report['database']['total_records'] = total_records
                
                # Estadísticas básicas
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as count,
                        AVG(price) as avg_price,
                        MIN(price) as min_price,
                        MAX(price) as max_price,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(DISTINCT source_file) as unique_files
                    FROM transactions
                """)
                stats = cursor.fetchone()
                report['database']['statistics'] = {
                    'count': stats[0],
                    'avg_price': round(stats[1], 2) if stats[1] else 0,
                    'min_price': round(stats[2], 2) if stats[2] else 0,
                    'max_price': round(stats[3], 2) if stats[3] else 0,
                    'unique_users': stats[4],
                    'unique_files': stats[5]
                }
                
                conn.close()
            
            # Información de estadísticas incrementales
            stats_path = self.project_root / "data" / "processed" / "pipeline_statistics.json"
            if stats_path.exists():
                with open(stats_path, 'r') as f:
                    stats_data = json.load(f)
                    report['statistics'] = stats_data.get('stats', {})
        
        except Exception as e:
            logger.warning(f"Error generando datos de reporte: {e}")
        
        return report
    
    def _generate_text_report(self, report_data: Dict[str, Any]) -> str:
        """Genera reporte en formato texto"""
        lines = [
            "🚀 REPORTE DEL PIPELINE MAESTRO",
            "=" * 50,
            "",
            f"📅 Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"⚡ Batch size utilizado: {self.batch_size:,}",
            f"📊 Estadísticas incrementales: {'Habilitadas' if self.enable_stats else 'Deshabilitadas'}",
            "",
            "📋 RESUMEN DE DATOS:",
            "-" * 25
        ]
        
        # Información de archivos
        if 'data_files' in report_data:
            bronze_count = report_data['data_files'].get('bronze_count', 0)
            lines.append(f"🥉 Archivos Bronze generados: {bronze_count}")
        
        # Información de BD
        if 'database' in report_data and 'total_records' in report_data['database']:
            db_stats = report_data['database']
            lines.extend([
                f"🗄️ Registros en base de datos: {db_stats['total_records']:,}",
                "",
                "📊 ESTADÍSTICAS DE PRECIOS:",
                "-" * 30
            ])
            
            if 'statistics' in db_stats:
                stats = db_stats['statistics']
                lines.extend([
                    f"📈 Precio promedio: ${stats.get('avg_price', 0):.2f}",
                    f"📉 Precio mínimo: ${stats.get('min_price', 0):.2f}",
                    f"📈 Precio máximo: ${stats.get('max_price', 0):.2f}",
                    f"👥 Usuarios únicos: {stats.get('unique_users', 0):,}",
                    f"📄 Archivos procesados: {stats.get('unique_files', 0)}"
                ])
        
        lines.extend([
            "",
            "✅ Pipeline ejecutado exitosamente",
            f"🕐 Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        return "\n".join(lines)
    
    def _finalize_result(self, pipeline_result: Dict[str, Any]) -> Dict[str, Any]:
        """Finaliza y retorna resultado del pipeline"""
        self.execution_end = datetime.now()
        pipeline_result['execution_end'] = self.execution_end.isoformat()
        
        if self.execution_start:
            duration = (self.execution_end - self.execution_start).total_seconds()
            pipeline_result['total_duration'] = duration
            
            logger.info(f"\n⏱️ DURACIÓN TOTAL: {duration:.2f} segundos")
        
        # Log resumen final
        logger.info(f"\n📊 RESUMEN FINAL:")
        logger.info(f"   ✅ Pasos completados: {len(pipeline_result['steps_completed'])}")
        logger.info(f"   ❌ Pasos fallidos: {len(pipeline_result['steps_failed'])}")
        logger.info(f"   🎯 Éxito general: {'SÍ' if pipeline_result['overall_success'] else 'NO'}")
        
        return pipeline_result


def main():
    """Función principal para ejecutar el pipeline maestro"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline Maestro de Datos")
    parser.add_argument("--batch-size", type=int, default=1000, help="Tamaño de micro-batch")
    parser.add_argument("--no-stats", action="store_true", help="Deshabilitar estadísticas incrementales")
    
    args = parser.parse_args()
    
    try:
        # Crear y ejecutar pipeline maestro
        master_pipeline = MasterPipeline(
            batch_size=args.batch_size,
            enable_stats=not args.no_stats
        )
        
        result = master_pipeline.run_complete_pipeline()
        
        if result['overall_success']:
            logger.info("🎉 PIPELINE MAESTRO COMPLETADO EXITOSAMENTE")
            return 0
        else:
            logger.error("❌ PIPELINE MAESTRO FALLÓ")
            return 1
    
    except Exception as e:
        logger.error(f"❌ Error crítico: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)