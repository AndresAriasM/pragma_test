# src/data_flow/utils.py
"""
Utilidades comunes para el flujo de datos
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime

# Configurar logging
def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """
    Configura el sistema de logging
    
    Args:
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR)
        log_file: Archivo de log opcional
        
    Returns:
        Logger configurado
    """
    # Crear formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configurar logger root
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler si se especifica
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def validate_csv_structure(file_path: str, required_columns: list) -> Dict[str, Any]:
    """
    Valida la estructura de un archivo CSV
    
    Args:
        file_path: Ruta al archivo CSV
        required_columns: Lista de columnas requeridas
        
    Returns:
        Dict con resultado de la validación
    """
    result = {
        "valid": False,
        "message": "",
        "row_count": 0,
        "columns": [],
        "missing_columns": [],
        "file_size": 0
    }
    
    try:
        if not Path(file_path).exists():
            result["message"] = f"Archivo no encontrado: {file_path}"
            return result
        
        # Obtener tamaño del archivo
        result["file_size"] = Path(file_path).stat().st_size
        
        # Leer solo las primeras filas para validar estructura
        sample_df = pd.read_csv(file_path, nrows=5)
        result["columns"] = list(sample_df.columns)
        
        # Verificar columnas requeridas
        missing_columns = [col for col in required_columns if col not in sample_df.columns]
        result["missing_columns"] = missing_columns
        
        if missing_columns:
            result["message"] = f"Faltan columnas: {missing_columns}"
            return result
        
        # Contar filas totales
        row_count = sum(1 for _ in open(file_path)) - 1  # -1 por el header
        result["row_count"] = row_count
        
        result["valid"] = True
        result["message"] = "Archivo válido"
        
    except Exception as e:
        result["message"] = f"Error validando archivo: {str(e)}"
    
    return result

def create_batch_id(source_file: str, batch_number: int) -> str:
    """
    Crea un ID único para un batch
    
    Args:
        source_file: Nombre del archivo fuente
        batch_number: Número del batch
        
    Returns:
        ID único del batch
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = Path(source_file).stem
    return f"{file_name}_batch_{batch_number:04d}_{timestamp}"

def ensure_directories_exist(paths: list):
    """
    Asegura que los directorios existan
    
    Args:
        paths: Lista de rutas a crear
    """
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)

def format_file_size(size_bytes: int) -> str:
    """
    Formatea el tamaño de archivo en unidades legibles
    
    Args:
        size_bytes: Tamaño en bytes
        
    Returns:
        String formateado (ej: "1.5 MB")
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"

def safe_float_conversion(value, default: float = 0.0) -> float:
    """
    Convierte un valor a float de forma segura
    
    Args:
        value: Valor a convertir
        default: Valor por defecto si la conversión falla
        
    Returns:
        Valor como float
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def get_csv_info(file_path: str) -> Dict[str, Any]:
    """
    Obtiene información básica de un archivo CSV
    
    Args:
        file_path: Ruta al archivo CSV
        
    Returns:
        Dict con información del archivo
    """
    info = {
        "file_name": Path(file_path).name,
        "file_size": 0,
        "file_size_formatted": "0 B",
        "row_count": 0,
        "columns": [],
        "sample_data": None,
        "exists": False
    }
    
    try:
        if not Path(file_path).exists():
            return info
        
        info["exists"] = True
        info["file_size"] = Path(file_path).stat().st_size
        info["file_size_formatted"] = format_file_size(info["file_size"])
        
        # Leer muestra para obtener información
        sample_df = pd.read_csv(file_path, nrows=3)
        info["columns"] = list(sample_df.columns)
        info["sample_data"] = sample_df.to_dict('records')
        
        # Contar filas (método eficiente)
        with open(file_path, 'r') as f:
            info["row_count"] = sum(1 for _ in f) - 1  # -1 por header
        
    except Exception as e:
        info["error"] = str(e)
    
    return info


# Funciones adicionales para trabajar con Parquet

def get_parquet_info(file_path: str) -> Dict[str, Any]:
    """
    Obtiene información de un archivo Parquet
    
    Args:
        file_path: Ruta al archivo Parquet
        
    Returns:
        Dict con información del archivo
    """
    import pyarrow.parquet as pq
    
    info = {
        "file_name": Path(file_path).name,
        "file_size": 0,
        "file_size_formatted": "0 B",
        "row_count": 0,
        "columns": [],
        "schema": {},
        "compression": "unknown",
        "row_groups": 0,
        "exists": False
    }
    
    try:
        if not Path(file_path).exists():
            return info
        
        info["exists"] = True
        info["file_size"] = Path(file_path).stat().st_size
        info["file_size_formatted"] = format_file_size(info["file_size"])
        
        # Leer metadatos Parquet
        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata
        
        info["row_count"] = metadata.num_rows
        info["row_groups"] = metadata.num_row_groups
        info["columns"] = [field.name for field in parquet_file.schema]
        info["schema"] = {field.name: str(field.type) for field in parquet_file.schema}
        
        # Obtener compresión del primer row group
        if metadata.num_row_groups > 0:
            info["compression"] = str(metadata.row_group(0).column(0).compression)
        
    except Exception as e:
        info["error"] = str(e)
    
    return info

def compare_csv_parquet_sizes(csv_path: str, parquet_path: str) -> Dict[str, Any]:
    """
    Compara tamaños entre CSV y Parquet
    
    Args:
        csv_path: Ruta del archivo CSV
        parquet_path: Ruta del archivo Parquet
        
    Returns:
        Dict con comparación de tamaños
    """
    comparison = {
        "csv_exists": Path(csv_path).exists(),
        "parquet_exists": Path(parquet_path).exists(),
        "csv_size": 0,
        "parquet_size": 0,
        "compression_ratio": 0.0,
        "space_saved": 0
    }
    
    try:
        if comparison["csv_exists"]:
            comparison["csv_size"] = Path(csv_path).stat().st_size
        
        if comparison["parquet_exists"]:
            comparison["parquet_size"] = Path(parquet_path).stat().st_size
        
        if comparison["csv_size"] > 0 and comparison["parquet_size"] > 0:
            comparison["compression_ratio"] = (1 - comparison["parquet_size"] / comparison["csv_size"]) * 100
            comparison["space_saved"] = comparison["csv_size"] - comparison["parquet_size"]
        
    except Exception as e:
        comparison["error"] = str(e)
    
    return comparison

def validate_medallion_layer(layer_path: str, expected_files: list) -> Dict[str, Any]:
    """
    Valida una capa de la arquitectura medallion
    
    Args:
        layer_path: Ruta de la capa
        expected_files: Lista de archivos esperados (sin extensión)
        
    Returns:
        Dict con resultado de validación
    """
    validation = {
        "layer_path": layer_path,
        "valid": True,
        "files_found": 0,
        "files_expected": len(expected_files),
        "missing_files": [],
        "corrupted_files": [],
        "total_rows": 0,
        "total_size": 0,
        "file_details": []
    }
    
    layer_path_obj = Path(layer_path)
    if not layer_path_obj.exists():
        validation["valid"] = False
        validation["error"] = f"Capa no existe: {layer_path}"
        return validation
    
    for file_stem in expected_files:
        parquet_file = layer_path_obj / f"{file_stem}.parquet"
        
        if parquet_file.exists():
            validation["files_found"] += 1
            
            # Obtener información del archivo
            file_info = get_parquet_info(str(parquet_file))
            
            if "error" in file_info:
                validation["corrupted_files"].append(file_stem)
                validation["valid"] = False
            else:
                validation["total_rows"] += file_info["row_count"]
                validation["total_size"] += file_info["file_size"]
                validation["file_details"].append(file_info)
        else:
            validation["missing_files"].append(file_stem)
            validation["valid"] = False
    
    return validation


# src/data_flow/__init__.py
"""
Módulo de flujo de datos
"""

from .utils import (
    setup_logging,
    validate_csv_structure,
    create_batch_id,
    ensure_directories_exist,
    format_file_size,
    safe_float_conversion,
    get_csv_info,
    get_parquet_info,
    compare_csv_parquet_sizes,
    validate_medallion_layer
)

__all__ = [
    "setup_logging",
    "validate_csv_structure", 
    "create_batch_id",
    "ensure_directories_exist",
    "format_file_size",
    "safe_float_conversion",
    "get_csv_info",
    "get_parquet_info",
    "compare_csv_parquet_sizes",
    "validate_medallion_layer"
]


# test_download.py (archivo en la raíz del proyecto)
"""
Script de prueba para verificar la descarga de datos
"""

import sys
from pathlib import Path

# Agregar src al path para importar módulos
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

try:
    from data_flow.download_data import DataDownloader
    from data_flow.utils import setup_logging, get_csv_info
    # Importar configuración de forma segura
    try:
        from config.pipeline_config import EXPECTED_CSV_FILES, DATA_RAW_PATH
    except ImportError:
        # Fallback si no se puede importar
        EXPECTED_CSV_FILES = [
            "2012-1.csv", "2012-2.csv", "2012-3.csv", 
            "2012-4.csv", "2012-5.csv", "validation.csv"
        ]
        DATA_RAW_PATH = project_root / "data" / "raw"
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure you're running from the project root directory")
    sys.exit(1)

def test_download():
    """
    Prueba la funcionalidad de descarga
    """
    # Configurar logging
    logger = setup_logging(log_level="INFO")
    
    logger.info("🧪 INICIANDO PRUEBA DE DESCARGA")
    logger.info("=" * 50)
    
    try:
        # Inicializar descargador
        downloader = DataDownloader()
        
        # Realizar descarga
        success, zip_path = downloader.download_challenge_data()
        
        if not success:
            logger.error("❌ Error en la descarga")
            return False
        
        # Verificar archivos
        verification_success = downloader.verify_downloaded_data()
        
        if not verification_success:
            logger.error("❌ Error en la verificación")
            return False
        
        # Mostrar información detallada de cada archivo
        logger.info("\n📊 INFORMACIÓN DETALLADA DE ARCHIVOS:")
        logger.info("-" * 40)
        
        for csv_file in EXPECTED_CSV_FILES:
            file_path = DATA_RAW_PATH / csv_file
            if not file_path.exists():
                # Buscar en subdirectorios
                for item in DATA_RAW_PATH.iterdir():
                    if item.is_dir():
                        alt_path = item / csv_file
                        if alt_path.exists():
                            file_path = alt_path
                            break
            
            info = get_csv_info(str(file_path))
            
            if info["exists"]:
                logger.info(f"📄 {info['file_name']}:")
                logger.info(f"   Tamaño: {info['file_size_formatted']}")
                logger.info(f"   Filas: {info['row_count']:,}")
                logger.info(f"   Columnas: {info['columns']}")
                
                if info.get("sample_data"):
                    logger.info(f"   Muestra: {info['sample_data'][0]}")
                
                logger.info("")
            else:
                logger.warning(f"❌ {csv_file}: No encontrado")
        
        logger.info("✅ Prueba de descarga completada exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")
        return False

if __name__ == "__main__":
    success = test_download()
    exit_code = 0 if success else 1
    sys.exit(exit_code)