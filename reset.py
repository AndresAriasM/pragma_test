#!/usr/bin/env python3
"""
Script para limpiar completamente el estado y empezar fresh
Resuelve problemas de sincronización entre estadísticas y BD
"""

import os
import shutil
from pathlib import Path

def clean_all_state():
    """
    Limpia completamente el estado del pipeline para empezar fresh
    """
    project_root = Path(__file__).parent
    
    files_to_clean = [
        # Base de datos
        project_root / "data" / "pipeline.db",
        project_root / "data" / "pipeline_development.db", 
        project_root / "data" / "pipeline_production.db",
        
        # Estadísticas persistidas
        project_root / "data" / "processed" / "pipeline_statistics.json",
        project_root / "data" / "processed" / "statistics_engine.json",
        
        # Logs
        project_root / "logs",
    ]
    
    print("🧹 LIMPIANDO ESTADO COMPLETO DEL PIPELINE")
    print("=" * 50)
    
    for file_path in files_to_clean:
        if file_path.exists():
            if file_path.is_file():
                file_path.unlink()
                print(f"✅ Eliminado: {file_path}")
            elif file_path.is_dir():
                shutil.rmtree(file_path)
                print(f"✅ Eliminado directorio: {file_path}")
        else:
            print(f"⏭️  No existe: {file_path}")
    
    # Recrear directorios necesarios
    dirs_to_create = [
        project_root / "data",
        project_root / "data" / "processed",
        project_root / "logs"
    ]
    
    for dir_path in dirs_to_create:
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"📁 Creado directorio: {dir_path}")
    
    print("\n🎉 LIMPIEZA COMPLETADA")
    print("✅ Ahora puedes ejecutar el pipeline fresh sin conflictos de estado")

if __name__ == "__main__":
    clean_all_state()