# src/config/pipeline_config.py
"""
Configuración general del pipeline de datos
✅ Configuraciones centralizadas para todo el proyecto
"""

import os
from pathlib import Path
from typing import Dict, Any, List

# ==========================================
# RUTAS DEL PROYECTO
# ==========================================

# Rutas base del proyecto
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_RAW_PATH = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_PATH = PROJECT_ROOT / "data" / "processed"
LOGS_PATH = PROJECT_ROOT / "logs"

# Crear directorios si no existen
DATA_RAW_PATH.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

# ==========================================
# CONFIGURACIÓN DE GOOGLE DRIVE
# ==========================================

GOOGLE_DRIVE_CONFIG = {
    "file_id": "1ejZpGTvZa81ZGD7IRWjObFeVuYbsSvuB",  # ID del archivo del reto
    "chunk_size": 32768,          # Tamaño de chunk para descarga (32KB)
    "timeout": 300,               # Timeout en segundos
    "max_retries": 3,             # Máximo número de reintentos
    "retry_delay": 5,             # Delay entre reintentos (segundos)
    "verify_ssl": True,           # Verificar certificados SSL
    "user_agent": "DataPipelineBot/1.0"  # User agent para requests
}

# ==========================================
# ARCHIVOS ESPERADOS DEL CHALLENGE
# ==========================================

# Archivos CSV esperados del reto
EXPECTED_CSV_FILES = [
    "2012-1.csv",
    "2012-2.csv", 
    "2012-3.csv",
    "2012-4.csv",
    "2012-5.csv",
    "validation.csv"
]

# Orden de procesamiento (validation al final)
PROCESSING_ORDER = [
    "2012-1.csv",
    "2012-2.csv", 
    "2012-3.csv",
    "2012-4.csv",
    "2012-5.csv"
]

# Archivo de validación (se procesa por separado)
VALIDATION_FILE = "validation.csv"

# ==========================================
# CONFIGURACIÓN DEL PIPELINE PRINCIPAL
# ==========================================

PIPELINE_CONFIG = {
    # Configuración de micro-batches
    "batch_size": 1000,                    # ✅ Tamaño de micro-batch (ajustable según memoria)
    "max_retries": 3,                      # Reintentos por batch fallido
    "retry_delay": 5,                      # Delay entre reintentos (segundos)
    "chunk_size_csv": 1000,                # Filas por chunk al leer CSV
    
    # Configuración de logging
    "log_level": "INFO",                   # DEBUG, INFO, WARNING, ERROR
    "log_to_file": True,                   # Si guardar logs en archivo
    "log_file_pattern": "pipeline_{timestamp}.log",  # Patrón nombre archivo log
    "log_rotation": True,                  # Rotar logs por fecha
    "max_log_files": 10,                   # Máximo archivos de log a mantener
    
    # Configuración de calidad de datos
    "enable_data_quality_checks": True,    # Habilitar validaciones de calidad
    "fail_on_quality_issues": False,       # Si fallar el pipeline por problemas de calidad
    "quality_report": True,                # Generar reporte de calidad
    
    # Configuración de estadísticas
    "enable_statistics_persistence": True, # Persistir estadísticas en archivo
    "statistics_file": "pipeline_statistics.json",  # Archivo de estadísticas
    "statistics_backup": True,             # Hacer backup de estadísticas
    
    # Configuración de performance
    "enable_performance_monitoring": True, # Monitorear performance
    "memory_monitoring": True,             # Monitorear uso de memoria
    "show_progress": True,                 # Mostrar progreso en tiempo real
    "progress_update_frequency": 5,        # Actualizar progreso cada N batches
    
    # Configuración de recovery
    "enable_checkpoint": False,            # Habilitar checkpoints (para pipelines largos)
    "checkpoint_frequency": 10,            # Crear checkpoint cada N batches
    "auto_recovery": False,                # Auto-recovery desde checkpoints
    
    # Configuración de paralelización (futuro)
    "enable_parallel_processing": False,   # Procesamiento paralelo (experimental)
    "max_workers": 4,                      # Máximo workers paralelos
    "parallel_batch_size": 500             # Tamaño de batch para paralelización
}

# ==========================================
# CONFIGURACIÓN DE CALIDAD DE DATOS
# ==========================================

DATA_QUALITY_CONFIG = {
    # Columnas requeridas en los CSV
    "required_columns": ["timestamp", "price", "user_id"],
    
    # Validaciones de precios
    "price_min_threshold": 0.01,          # Precio mínimo válido ($0.01)
    "price_max_threshold": 50000.0,       # Precio máximo válido ($50,000)
    "price_validation": True,              # Validar rangos de precios
    "price_outlier_detection": True,       # Detectar outliers en precios
    "price_outlier_std_multiplier": 3,     # Múltiplo de desviación estándar para outliers
    
    # Validaciones de duplicados
    "duplicate_check": True,               # Verificar duplicados
    "duplicate_tolerance": 0.0,            # Tolerancia de duplicados (0% = no duplicados)
    "duplicate_action": "warn",            # "warn", "error", "remove"
    
    # Validaciones de nulos
    "null_tolerance": 0.01,                # 1% máximo de valores nulos permitidos
    "null_action": "warn",                 # "warn", "error", "fill"
    "null_fill_strategy": {               # Estrategia para llenar nulos
        "price": "median",                 # median, mean, forward_fill, backward_fill
        "user_id": "unknown",              # Valor por defecto
        "timestamp": "interpolate"         # Interpolar timestamps faltantes
    },
    
    # Validaciones de timestamp
    "timestamp_validation": True,          # Validar formato de timestamps
    "timestamp_format": "auto",            # "auto" o formato específico como "%Y-%m-%d %H:%M:%S"
    "timestamp_range_check": True,         # Verificar que timestamps estén en rango válido
    "timestamp_min": "2012-01-01",         # Timestamp mínimo esperado
    "timestamp_max": "2012-12-31",         # Timestamp máximo esperado
    
    # Validaciones de user_id
    "user_id_pattern": r"^user_\d+$",     # Patrón regex para user_id válido
    "user_id_validation": True,            # Validar patrón de user_id
    "user_id_case_sensitive": False,       # Si user_id es case-sensitive
    
    # Configuración de reportes
    "generate_quality_report": True,       # Generar reporte detallado de calidad
    "quality_report_format": "json",       # "json", "html", "csv"
    "save_invalid_records": True,          # Guardar registros inválidos para análisis
    "invalid_records_file": "invalid_records.csv"  # Archivo para registros inválidos
}

# ==========================================
# CONFIGURACIÓN DE ESTADÍSTICAS
# ==========================================

STATISTICS_CONFIG = {
    # Métricas a calcular
    "metrics": ["count", "sum", "min", "max", "avg", "std", "median"],
    
    # Configuración de actualización
    "update_frequency": "per_batch",       # "per_batch", "per_file", "per_chunk"
    "real_time_display": True,             # Mostrar estadísticas en tiempo real
    "display_precision": 2,                # Decimales para mostrar en logs
    
    # Configuración de persistencia
    "persistence_mode": "database",        # "database", "file", "memory", "hybrid"
    "persistence_file": "statistics_engine.json",  # Archivo para persistir stats
    "backup_frequency": 10,                # Backup cada N actualizaciones
    "backup_retention": 5,                 # Número de backups a mantener
    
    # Configuración de verificación
    "auto_verification": True,             # Verificar automáticamente vs BD
    "verification_frequency": "end",       # "per_file", "end", "periodic"
    "verification_tolerance": 1e-6,        # Tolerancia para comparaciones float
    "verification_log_details": True,      # Log detalles de verificación
    
    # Configuración de alertas
    "enable_alerts": True,                 # Habilitar alertas de anomalías
    "price_spike_threshold": 2.0,          # Multiplicador para detectar spikes de precio
    "count_anomaly_threshold": 0.1,        # Threshold para anomalías en count (10%)
    "alert_channels": ["log", "file"],     # "log", "file", "email" (futuro)
    
    # Configuración de histórico
    "keep_batch_history": True,            # Mantener historial de batches
    "max_history_entries": 1000,           # Máximo entradas en historial
    "history_compression": False,          # Comprimir historial antiguo
    
    # Configuración avanzada
    "incremental_validation": True,        # Validar estadísticas incrementales
    "cross_file_validation": True,         # Validar consistencia entre archivos
    "performance_tracking": True           # Trackear performance del motor
}

# ==========================================
# CONFIGURACIÓN DE LOGGING
# ==========================================

LOGGING_CONFIG = {
    # Configuración básica
    "level": "INFO",                       # Nivel de logging global
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    
    # Configuración de archivos
    "log_to_console": True,                # Log a consola
    "log_to_file": True,                   # Log a archivo
    "log_file_prefix": "pipeline",         # Prefijo para archivos de log
    "log_rotation": "daily",               # "daily", "hourly", "size"
    "max_log_size_mb": 50,                 # Tamaño máximo por archivo (MB)
    "backup_count": 7,                     # Número de archivos de backup
    
    # Niveles específicos por módulo
    "module_levels": {
        "data_flow": "INFO",
        "pipeline": "INFO",
        "statistics": "INFO",
        "database": "WARNING",
        "requests": "WARNING",              # Reducir logs de HTTP requests
        "urllib3": "WARNING"
    },
    
    # Configuración de formatos específicos
    "console_format": "%(asctime)s - %(levelname)s - %(message)s",
    "file_format": "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
    
    # Filtros de logging
    "exclude_patterns": [                  # Patrones a excluir de logs
        "urllib3.connectionpool",
        "requests.packages.urllib3"
    ],
    "include_thread_info": False,          # Incluir información de threads
    "include_process_info": False          # Incluir información de procesos
}

# ==========================================
# CONFIGURACIÓN DE ENTORNOS
# ==========================================

# Configuración específica por entorno
ENVIRONMENT_CONFIGS = {
    "development": {
        "batch_size": 500,                 # Batches más pequeños para development
        "log_level": "DEBUG",              # Más verbose en desarrollo
        "enable_data_quality_checks": True,
        "fail_on_quality_issues": False,
        "enable_performance_monitoring": True,
        "database_echo": True              # Mostrar queries SQL
    },
    
    "testing": {
        "batch_size": 100,                 # Batches muy pequeños para tests
        "log_level": "WARNING",            # Menos verbose en tests
        "enable_data_quality_checks": True,
        "fail_on_quality_issues": True,   # Fallar en tests si hay problemas
        "enable_performance_monitoring": False,
        "database_echo": False,
        "quick_mode": True                 # Modo rápido para tests
    },
    
    "production": {
        "batch_size": 2000,                # Batches más grandes en producción
        "log_level": "INFO",
        "enable_data_quality_checks": True,
        "fail_on_quality_issues": True,   # Fallar en producción si hay problemas críticos
        "enable_performance_monitoring": True,
        "database_echo": False,
        "enable_checkpoint": True,          # Checkpoints en producción
        "auto_recovery": True
    }
}

# ==========================================
# CONFIGURACIÓN DINÁMICA
# ==========================================

def get_environment() -> str:
    """
    Detecta el entorno actual basado en variables de entorno
    """
    return os.getenv("PIPELINE_ENV", "development").lower()

def get_config_for_environment(env: str = None) -> Dict[str, Any]:
    """
    Obtiene configuración específica para un entorno
    
    Args:
        env: Entorno específico (si no se proporciona, detecta automáticamente)
        
    Returns:
        Dict con configuración del entorno
    """
    if env is None:
        env = get_environment()
    
    base_config = PIPELINE_CONFIG.copy()
    env_config = ENVIRONMENT_CONFIGS.get(env, {})
    
    # Mergear configuraciones
    base_config.update(env_config)
    return base_config

def get_database_config_for_environment(env: str = None) -> Dict[str, Any]:
    """
    Obtiene configuración de BD específica para un entorno
    """
    if env is None:
        env = get_environment()
    
    if env == "production":
        return {
            "type": "postgresql",
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 5432)),
            "database": os.getenv("DB_NAME", "pipeline_prod"),
            "username": os.getenv("DB_USER", "pipeline_user"),
            "password": os.getenv("DB_PASSWORD", ""),
            "echo": False,
            "pool_size": 10,
            "max_overflow": 20
        }
    else:
        # Development y testing usan SQLite
        db_path = PROJECT_ROOT / "data" / f"pipeline_{env}.db"
        return {
            "type": "sqlite",
            "path": str(db_path),
            "echo": env == "development"
        }

# ==========================================
# FUNCIONES DE UTILIDAD
# ==========================================

def validate_config() -> bool:
    """
    Valida que la configuración sea correcta
    
    Returns:
        bool: True si la configuración es válida
    """
    # Verificar que las rutas existan
    required_paths = [PROJECT_ROOT, DATA_RAW_PATH, DATA_PROCESSED_PATH, LOGS_PATH]
    for path in required_paths:
        if not path.exists():
            print(f"❌ Ruta requerida no existe: {path}")
            return False
    
    # Verificar configuraciones críticas
    if PIPELINE_CONFIG["batch_size"] <= 0:
        print("❌ batch_size debe ser mayor a 0")
        return False
    
    if DATA_QUALITY_CONFIG["price_min_threshold"] >= DATA_QUALITY_CONFIG["price_max_threshold"]:
        print("❌ price_min_threshold debe ser menor que price_max_threshold")
        return False
    
    return True

def print_current_config():
    """
    Imprime la configuración actual para debugging
    """
    env = get_environment()
    config = get_config_for_environment(env)
    
    print(f"🔧 CONFIGURACIÓN ACTUAL DEL PIPELINE")
    print(f"=" * 40)
    print(f"Entorno: {env}")
    print(f"Proyecto: {PROJECT_ROOT}")
    print(f"Batch size: {config['batch_size']:,}")
    print(f"Log level: {config['log_level']}")
    print(f"Calidad de datos: {'✅' if config['enable_data_quality_checks'] else '❌'}")
    print(f"Performance monitoring: {'✅' if config['enable_performance_monitoring'] else '❌'}")

def get_file_paths() -> Dict[str, Path]:
    """
    Obtiene todas las rutas de archivos importantes
    
    Returns:
        Dict con rutas organizadas
    """
    return {
        "project_root": PROJECT_ROOT,
        "data_raw": DATA_RAW_PATH,
        "data_processed": DATA_PROCESSED_PATH,
        "logs": LOGS_PATH,
        "bronze": DATA_PROCESSED_PATH / "bronze",
        "silver": DATA_PROCESSED_PATH / "silver", 
        "gold": DATA_PROCESSED_PATH / "gold",
        "database": PROJECT_ROOT / "data" / f"pipeline_{get_environment()}.db",
        "statistics": DATA_PROCESSED_PATH / STATISTICS_CONFIG["persistence_file"]
    }

# ==========================================
# VALIDACIÓN AL IMPORTAR
# ==========================================

# Validar configuración al importar el módulo
if __name__ != "__main__":
    if not validate_config():
        raise ValueError("❌ Configuración del pipeline inválida")

# ==========================================
# DEMO/TEST DE CONFIGURACIÓN
# ==========================================

if __name__ == "__main__":
    print("🔧 TESTING CONFIGURACIÓN DEL PIPELINE")
    print("=" * 40)
    
    # Mostrar configuración actual
    print_current_config()
    
    # Validar configuración
    if validate_config():
        print("\n✅ Configuración válida")
    else:
        print("\n❌ Configuración inválida")
    
    # Mostrar rutas importantes
    print(f"\n📁 RUTAS IMPORTANTES:")
    paths = get_file_paths()
    for name, path in paths.items():
        exists = "✅" if path.exists() else "❌"
        print(f"   {name}: {path} {exists}")
    
    # Mostrar configuración por entornos
    print(f"\n🌍 CONFIGURACIONES POR ENTORNO:")
    for env_name in ["development", "testing", "production"]:
        config = get_config_for_environment(env_name)
        print(f"   {env_name}: batch_size={config['batch_size']}, log_level={config['log_level']}")