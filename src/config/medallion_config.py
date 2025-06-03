# src/config/medallion_config.py
"""
Configuración para la arquitectura Medallion
"""

from pathlib import Path
from typing import Dict, Any, List

# Rutas de la arquitectura Medallion
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_RAW_PATH = PROJECT_ROOT / "data" / "raw"
MEDALLION_BASE_PATH = PROJECT_ROOT / "data" / "processed"

# Capas de la arquitectura Medallion
BRONZE_PATH = MEDALLION_BASE_PATH / "bronze"
SILVER_PATH = MEDALLION_BASE_PATH / "silver" 
GOLD_PATH = MEDALLION_BASE_PATH / "gold"

# Configuración de Bronze Layer
BRONZE_CONFIG = {
    "input_format": "csv",
    "output_format": "parquet",
    "compression": "snappy",  # snappy, gzip, lz4, brotli
    "row_group_size": 50000,
    "page_size": 8192,
    "use_dictionary": True,
    "write_statistics": True,
    "preserve_index": False,
    "schema_validation": True,
    "add_metadata": True,  # Agregar metadatos de origen
    "partitioning": None,  
    "micro_batch_size": 1000,  # Filas por micro-batch 
    "memory_optimization": True,  # Limpiar memoria entre batches
    "incremental_write": True,  # Escritura incremental de Parquet
    "progress_logging": 5  # Log progreso cada N batches
}

# Configuración de Silver Layer
SILVER_CONFIG = {
    "input_format": "parquet",
    "output_format": "parquet", 
    "compression": "snappy",
    "deduplication": True,
    "data_quality_checks": True,
    "schema_evolution": True,
    "partitioning": {
        "enabled": True,
        "columns": ["source_file"],  # Particionar por archivo fuente
        "partition_size_mb": 128
    },
    "transformations": {
        "timestamp_parsing": True,
        "price_validation": True,
        "user_id_standardization": True,
        "outlier_detection": True
    }
}

# Configuración de Gold Layer
GOLD_CONFIG = {
    "input_format": "parquet",
    "output_format": "parquet",
    "compression": "snappy",
    "aggregation_windows": ["daily", "weekly", "monthly"],
    "metrics": [
        "transaction_count",
        "price_statistics",
        "user_analytics", 
        "temporal_patterns"
    ],
    "partitioning": {
        "enabled": True,
        "columns": ["metric_type", "date_partition"],
        "partition_size_mb": 64
    },
    "materialization": {
        "user_analytics": "daily",
        "price_statistics": "hourly",
        "temporal_patterns": "daily"
    }
}

# Esquemas de datos por capa
DATA_SCHEMAS = {
    "bronze": {
        "timestamp": "string",  # Mantener como string en bronze
        "price": "float64",
        "user_id": "string",
        "source_file": "string",
        "bronze_created_at": "string",
        "bronze_created_by": "string"
    },
    "silver": {
        "transaction_id": "string",  # ID único generado
        "timestamp": "timestamp[ns]",  # Parseado en silver
        "price": "float64",
        "user_id": "string",
        "source_file": "string",
        "is_valid": "bool",
        "quality_score": "float64",
        "silver_created_at": "timestamp[ns]",
        "silver_updated_at": "timestamp[ns]"
    },
    "gold": {
        "metric_name": "string",
        "metric_value": "float64",
        "metric_type": "string",
        "date_partition": "date32",
        "aggregation_level": "string",
        "gold_created_at": "timestamp[ns]"
    }
}

# Validaciones de calidad por capa
QUALITY_RULES = {
    "bronze": {
        "required_columns": ["timestamp", "price", "user_id"],
        "null_tolerance": 0.0,  # No nulos permitidos en bronze
        "duplicate_tolerance": 1.0,  # Duplicados permitidos en bronze
        "price_range": (0.01, 50000.0)
    },
    "silver": {
        "required_columns": ["transaction_id", "timestamp", "price", "user_id", "is_valid"],
        "null_tolerance": 0.0,
        "duplicate_tolerance": 0.0,  # No duplicados en silver
        "price_range": (0.01, 10000.0),  # Rango más restrictivo
        "timestamp_format": "ISO8601",
        "user_id_pattern": r"^user_\d+$"  # Patrón esperado
    },
    "gold": {
        "required_columns": ["metric_name", "metric_value", "date_partition"],
        "null_tolerance": 0.0,
        "duplicate_tolerance": 0.0,
        "metric_value_range": (-1e6, 1e6)
    }
}

# Configuración de procesamiento por batches
BATCH_CONFIG = {
    "bronze": {
        "batch_size": None,  # Procesar archivo completo
        "parallel_processing": False,
        "memory_optimization": True
    },
    "silver": {
        "batch_size": 10000,  # Procesar en chunks
        "parallel_processing": True,
        "max_workers": 4,
        "memory_optimization": True
    },
    "gold": {
        "batch_size": 50000,
        "parallel_processing": True,
        "max_workers": 2,
        "memory_optimization": False  # Más memoria para agregaciones
    }
}

# Configuración de retención de datos
RETENTION_CONFIG = {
    "bronze": {
        "retention_days": 365,  # 1 año
        "archival_enabled": True,
        "archival_storage": "cold"
    },
    "silver": {
        "retention_days": 730,  # 2 años  
        "archival_enabled": True,
        "archival_storage": "warm"
    },
    "gold": {
        "retention_days": -1,  # Indefinido
        "archival_enabled": False,
        "archival_storage": "hot"
    }
}

# Configuración de monitoreo
MONITORING_CONFIG = {
    "metrics_collection": True,
    "performance_tracking": True,
    "data_lineage": True,
    "alerts": {
        "processing_failures": True,
        "quality_degradation": True,
        "performance_degradation": True,
        "schema_changes": True
    },
    "dashboards": {
        "data_quality": True,
        "processing_performance": True,
        "data_lineage": True
    }
}

# Metadatos por defecto
DEFAULT_METADATA = {
    "bronze": {
        "created_by": "bronze_converter",
        "data_source": "challenge_csv_files",
        "processing_version": "1.0",
        "quality_tier": "raw"
    },
    "silver": {
        "created_by": "silver_processor", 
        "data_source": "bronze_layer",
        "processing_version": "1.0",
        "quality_tier": "clean"
    },
    "gold": {
        "created_by": "gold_aggregator",
        "data_source": "silver_layer", 
        "processing_version": "1.0",
        "quality_tier": "analytics_ready"
    }
}

# Funciones auxiliares
def get_layer_path(layer: str) -> Path:
    """
    Obtiene la ruta de una capa específica
    
    Args:
        layer: Nombre de la capa (bronze, silver, gold)
        
    Returns:
        Path de la capa
    """
    layer_paths = {
        "bronze": BRONZE_PATH,
        "silver": SILVER_PATH,
        "gold": GOLD_PATH
    }
    
    if layer not in layer_paths:
        raise ValueError(f"Capa no válida: {layer}. Opciones: {list(layer_paths.keys())}")
    
    # Crear directorio si no existe
    layer_path = layer_paths[layer]
    layer_path.mkdir(parents=True, exist_ok=True)
    
    return layer_path

def get_layer_config(layer: str) -> Dict[str, Any]:
    """
    Obtiene la configuración de una capa específica
    
    Args:
        layer: Nombre de la capa
        
    Returns:
        Diccionario con la configuración
    """
    layer_configs = {
        "bronze": BRONZE_CONFIG,
        "silver": SILVER_CONFIG, 
        "gold": GOLD_CONFIG
    }
    
    if layer not in layer_configs:
        raise ValueError(f"Capa no válida: {layer}")
    
    return layer_configs[layer].copy()

def get_layer_schema(layer: str) -> Dict[str, str]:
    """
    Obtiene el esquema de una capa específica
    
    Args:
        layer: Nombre de la capa
        
    Returns:
        Diccionario con el esquema
    """
    if layer not in DATA_SCHEMAS:
        raise ValueError(f"Esquema no definido para capa: {layer}")
    
    return DATA_SCHEMAS[layer].copy()

def get_quality_rules(layer: str) -> Dict[str, Any]:
    """
    Obtiene las reglas de calidad de una capa
    
    Args:
        layer: Nombre de la capa
        
    Returns:
        Diccionario con las reglas de calidad
    """
    if layer not in QUALITY_RULES:
        raise ValueError(f"Reglas de calidad no definidas para capa: {layer}")
    
    return QUALITY_RULES[layer].copy()

# Lista de archivos esperados (sin extensión)
EXPECTED_FILE_STEMS = [
    "2012-1",
    "2012-2", 
    "2012-3",
    "2012-4", 
    "2012-5",
    "validation"
]

# Configuración de archivos por capa
FILE_PATTERNS = {
    "bronze": {
        "input_pattern": "*.csv",
        "output_pattern": "{stem}.parquet",
        "metadata_pattern": "{stem}_metadata.json"
    },
    "silver": {
        "input_pattern": "*.parquet", 
        "output_pattern": "{stem}_clean.parquet",
        "metadata_pattern": "{stem}_silver_metadata.json"
    },
    "gold": {
        "input_pattern": "*_clean.parquet",
        "output_pattern": "{metric_type}_{period}.parquet", 
        "metadata_pattern": "{metric_type}_gold_metadata.json"
    }
}


# src/config/__init__.py (actualizar imports)
"""
Módulo de configuración del pipeline
"""

from .pipeline_config import (
    PROJECT_ROOT,
    DATA_RAW_PATH,
    DATA_PROCESSED_PATH,
    LOGS_PATH,
    GOOGLE_DRIVE_CONFIG,
    EXPECTED_CSV_FILES,
    PIPELINE_CONFIG,
    DATA_QUALITY_CONFIG,
    STATISTICS_CONFIG
)

from .database_config import (
    DEFAULT_DB_CONFIG,
    POSTGRES_CONFIG,
    TABLE_SCHEMAS,
    get_database_config,
    get_connection_string
)

from .medallion_config import (
    MEDALLION_BASE_PATH,
    BRONZE_PATH,
    SILVER_PATH,
    GOLD_PATH,
    BRONZE_CONFIG,
    SILVER_CONFIG,
    GOLD_CONFIG,
    DATA_SCHEMAS,
    QUALITY_RULES,
    BATCH_CONFIG,
    MONITORING_CONFIG,
    EXPECTED_FILE_STEMS,
    get_layer_path,
    get_layer_config,
    get_layer_schema,
    get_quality_rules
)

__all__ = [
    # Pipeline config
    "PROJECT_ROOT",
    "DATA_RAW_PATH", 
    "DATA_PROCESSED_PATH",
    "LOGS_PATH",
    "GOOGLE_DRIVE_CONFIG",
    "EXPECTED_CSV_FILES",
    "PIPELINE_CONFIG",
    "DATA_QUALITY_CONFIG", 
    "STATISTICS_CONFIG",
    
    # Database config
    "DEFAULT_DB_CONFIG",
    "POSTGRES_CONFIG",
    "TABLE_SCHEMAS",
    "get_database_config",
    "get_connection_string",
    
    # Medallion config
    "MEDALLION_BASE_PATH",
    "BRONZE_PATH",
    "SILVER_PATH", 
    "GOLD_PATH",
    "BRONZE_CONFIG",
    "SILVER_CONFIG",
    "GOLD_CONFIG",
    "DATA_SCHEMAS",
    "QUALITY_RULES",
    "BATCH_CONFIG",
    "MONITORING_CONFIG",
    "EXPECTED_FILE_STEMS",
    "get_layer_path",
    "get_layer_config", 
    "get_layer_schema",
    "get_quality_rules"
]