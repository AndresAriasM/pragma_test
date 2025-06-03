# src/config/database_config.py
"""
Configuración de base de datos - VERSIÓN CORREGIDA
"""

import os
from typing import Dict, Any
from pathlib import Path

# Obtener PROJECT_ROOT de forma segura
try:
    PROJECT_ROOT = Path(__file__).parent.parent.parent
except:
    PROJECT_ROOT = Path.cwd()

# Base de datos por defecto (SQLite)
DEFAULT_DB_CONFIG = {
    "type": "sqlite",
    "path": PROJECT_ROOT / "data" / "pipeline.db",
    "echo": False,  # Log SQL queries
    "pool_size": 5,
    "max_overflow": 10
}

# Configuración para PostgreSQL (producción)
POSTGRES_CONFIG = {
    "type": "postgresql",
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "data_pipeline"),
    "username": os.getenv("POSTGRES_USER", "pipeline_user"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "schema": "public",
    "echo": False,
    "pool_size": 10,
    "max_overflow": 20
}

# Esquemas de tablas
TABLE_SCHEMAS = {
    "transactions": {
        "columns": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "timestamp": "TEXT NOT NULL",
            "price": "REAL NOT NULL",
            "user_id": "TEXT NOT NULL",
            "source_file": "TEXT NOT NULL",
            "batch_id": "TEXT",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        },
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_user_id ON transactions(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_source_file ON transactions(source_file)",
            "CREATE INDEX IF NOT EXISTS idx_price ON transactions(price)"
        ]
    },
    "statistics": {
        "columns": {
            "id": "INTEGER PRIMARY KEY",
            "count": "INTEGER NOT NULL DEFAULT 0",
            "sum": "REAL NOT NULL DEFAULT 0.0",
            "min_price": "REAL",
            "max_price": "REAL",
            "avg_price": "REAL",
            "last_updated": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        }
    },
    "batch_metadata": {
        "columns": {
            "batch_id": "TEXT PRIMARY KEY",
            "source_file": "TEXT NOT NULL",
            "batch_number": "INTEGER NOT NULL",
            "rows_processed": "INTEGER NOT NULL",
            "processing_start": "TIMESTAMP",
            "processing_end": "TIMESTAMP",
            "status": "TEXT DEFAULT 'pending'",  # pending, processing, completed, failed
            "error_message": "TEXT",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        }
    },
    "data_quality_checks": {
        "columns": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "batch_id": "TEXT NOT NULL",
            "check_type": "TEXT NOT NULL",
            "check_result": "TEXT NOT NULL",  # passed, failed, warning
            "details": "TEXT",
            "checked_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        }
    }
}

def get_database_config() -> Dict[str, Any]:
    """
    Retorna la configuración de base de datos basada en el entorno
    """
    env = os.getenv("ENVIRONMENT", "development").lower()
    
    if env == "production":
        return POSTGRES_CONFIG
    else:
        return DEFAULT_DB_CONFIG

def get_connection_string() -> str:
    """
    Genera string de conexión basado en la configuración
    """
    config = get_database_config()
    
    if config["type"] == "sqlite":
        return "sqlite:///" + str(config["path"])
    elif config["type"] == "postgresql":
        # Construir string sin f-string para evitar problemas con backslashes
        connection_str = "postgresql://{username}:{password}@{host}:{port}/{database}".format(
            username=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"]
        )
        return connection_str
    else:
        raise ValueError("Tipo de base de datos no soportado: {}".format(config["type"]))