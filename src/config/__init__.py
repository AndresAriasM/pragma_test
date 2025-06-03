# src/config/__init__.py
"""
Módulo de configuración del pipeline - VERSIÓN CORREGIDA
"""

try:
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
except ImportError:
    # Fallback values si no se puede importar
    from pathlib import Path
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    DATA_RAW_PATH = PROJECT_ROOT / "data" / "raw"
    DATA_PROCESSED_PATH = PROJECT_ROOT / "data" / "processed"
    LOGS_PATH = PROJECT_ROOT / "logs"
    
    EXPECTED_CSV_FILES = [
        "2012-1.csv", "2012-2.csv", "2012-3.csv", 
        "2012-4.csv", "2012-5.csv", "validation.csv"
    ]
    
    PIPELINE_CONFIG = {"batch_size": 1000}
    DATA_QUALITY_CONFIG = {}
    STATISTICS_CONFIG = {}
    GOOGLE_DRIVE_CONFIG = {}

try:
    from .database_config import (
        DEFAULT_DB_CONFIG,
        POSTGRES_CONFIG,
        TABLE_SCHEMAS,
        get_database_config,
        get_connection_string
    )
except ImportError:
    # Fallback values
    DEFAULT_DB_CONFIG = {"type": "sqlite", "path": "data/pipeline.db"}
    POSTGRES_CONFIG = {}
    TABLE_SCHEMAS = {}
    
    def get_database_config():
        return DEFAULT_DB_CONFIG
    
    def get_connection_string():
        return "sqlite:///data/pipeline.db"

try:
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
except ImportError:
    # Fallback values
    from pathlib import Path
    MEDALLION_BASE_PATH = Path(__file__).parent.parent.parent / "data" / "processed"
    BRONZE_PATH = MEDALLION_BASE_PATH / "bronze"
    SILVER_PATH = MEDALLION_BASE_PATH / "silver"
    GOLD_PATH = MEDALLION_BASE_PATH / "gold"
    
    EXPECTED_FILE_STEMS = ["2012-1", "2012-2", "2012-3", "2012-4", "2012-5", "validation"]
    
    BRONZE_CONFIG = {"micro_batch_size": 1000}
    SILVER_CONFIG = {}
    GOLD_CONFIG = {}
    DATA_SCHEMAS = {}
    QUALITY_RULES = {}
    BATCH_CONFIG = {}
    MONITORING_CONFIG = {}
    
    def get_layer_path(layer):
        layers = {"bronze": BRONZE_PATH, "silver": SILVER_PATH, "gold": GOLD_PATH}
        return layers.get(layer, BRONZE_PATH)
    
    def get_layer_config(layer):
        return BRONZE_CONFIG if layer == "bronze" else {}
    
    def get_layer_schema(layer):
        return {}
    
    def get_quality_rules(layer):
        return {}

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