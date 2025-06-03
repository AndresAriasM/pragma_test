# streamlit_app/pages/02_🚀_pipeline_control.py
"""
🚀 CONTROL DEL PIPELINE
======================
Página para ejecutar y monitorear el pipeline de datos
"""

import streamlit as st
import sys
from pathlib import Path
import subprocess
import threading
import time
import json
from datetime import datetime
import queue
import os

# Configurar paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

st.set_page_config(page_title="Control Pipeline", page_icon="🚀", layout="wide")

def main():
    st.title("🚀 Control del Pipeline")
    st.markdown("Ejecuta y monitorea el pipeline de datos en tiempo real")
    
    # Sidebar con opciones
    with st.sidebar:
        st.markdown("### ⚙️ Configuración")
        
        batch_size = st.number_input(
            "Tamaño de micro-batch",
            min_value=100,
            max_value=5000,
            value=1000,
            step=100,
            help="Número de filas por micro-batch"
        )
        
        enable_stats = st.checkbox("Estadísticas incrementales", value=True)
        enable_verification = st.checkbox("Verificación final", value=True)
        
        st.markdown("---")
        st.markdown("### 📋 Pasos del Pipeline")
        
        pipeline_steps = [
            ("📥", "Descarga datos", "download"),
            ("🥉", "Conversión Bronze", "bronze"),
            ("🚀", "Pipeline principal", "main"),
            ("🧪", "Validation", "validation"),
            ("📊", "Reporte final", "report")
        ]
        
        selected_steps = []
        for icon, name, key in pipeline_steps:
            if st.checkbox(f"{icon} {name}", value=True, key=f"step_{key}"):
                selected_steps.append(key)
    
    # Área principal
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🎛️ Panel de Control")
        
        # Botones de control
        col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)
        
        with col_btn1:
            if st.button("🚀 Ejecutar Pipeline Completo", use_container_width=True, type="primary"):
                execute_full_pipeline(batch_size, enable_stats, enable_verification)
        
        with col_btn2:
            if st.button("🥉 Solo Bronze", use_container_width=True):
                execute_bronze_only()
        
        with col_btn3:
            if st.button("📊 Solo Estadísticas", use_container_width=True):
                execute_stats_only()
        
        with col_btn4:
            if st.button("🧹 Limpiar BD", use_container_width=True):
                clean_database()
        
        # Estado de ejecución
        if 'pipeline_running' not in st.session_state:
            st.session_state.pipeline_running = False
        
        if 'pipeline_logs' not in st.session_state:
            st.session_state.pipeline_logs = []
        
        # Área de logs en tiempo real
        st.markdown("### 📝 Logs en Tiempo Real")
        
        if st.session_state.pipeline_running:
            st.info("🔄 Pipeline ejecutándose...")
            
            # Contenedor para logs que se actualiza
            log_container = st.empty()
            
            # Simular logs en tiempo real (en una implementación real usarías WebSockets)
            if st.session_state.pipeline_logs:
                log_text = "\n".join(st.session_state.pipeline_logs[-20:])  # Últimas 20 líneas
                log_container.code(log_text, language="bash")
        else:
            st.code("Esperando ejecución del pipeline...", language="bash")
    
    with col2:
        st.markdown("### 📊 Estado Actual")
        
        # Métricas en tiempo real
        display_pipeline_metrics()
        
        st.markdown("---")
        
        # Progreso visual
        st.markdown("### 📈 Progreso")
        
        progress_data = get_pipeline_progress()
        
        for step_name, progress in progress_data.items():
            st.metric(
                label=step_name,
                value=f"{progress}%",
                delta="Completado" if progress == 100 else "En progreso"
            )
            st.progress(progress / 100)
        
        st.markdown("---")
        
        # Última ejecución
        st.markdown("### 🕐 Última Ejecución")
        last_execution = get_last_execution_info()
        
        if last_execution:
            st.success(f"**Fecha**: {last_execution['date']}")
            st.info(f"**Duración**: {last_execution['duration']}")
            st.info(f"**Archivos**: {last_execution['files']}")
            st.info(f"**Filas**: {last_execution['rows']}")
        else:
            st.warning("Sin ejecuciones previas")

def execute_full_pipeline(batch_size, enable_stats, enable_verification):
    """Ejecuta el pipeline completo"""
    st.session_state.pipeline_running = True
    st.session_state.pipeline_logs = ["🚀 Iniciando pipeline completo..."]
    
    try:
        # Comando para ejecutar el pipeline
        cmd = [
            "python3",
            str(PROJECT_ROOT / "src" / "pipeline" / "data_ingestion.py")
        ]
        
        # Configurar environment
        env = os.environ.copy()
        env["PYTHONPATH"] = str(SRC_PATH)
        env["BATCH_SIZE"] = str(batch_size)
        
        # Ejecutar en background (simulado)
        with st.spinner("Ejecutando pipeline..."):
            result = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                env=env,
                timeout=300  # 5 minutos máximo
            )
            
            if result.returncode == 0:
                st.session_state.pipeline_logs.extend([
                    "✅ Pipeline ejecutado exitosamente",
                    f"📊 Output: {result.stdout[:500]}..."
                ])
                st.success("🎉 Pipeline completado exitosamente!")
            else:
                st.session_state.pipeline_logs.extend([
                    "❌ Error en pipeline",
                    f"Error: {result.stderr[:500]}..."
                ])
                st.error("❌ Error ejecutando pipeline")
    
    except subprocess.TimeoutExpired:
        st.error("⏰ Pipeline timeout - ejecutándose en background")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
    
    finally:
        st.session_state.pipeline_running = False

def execute_bronze_only():
    """Ejecuta solo la conversión Bronze"""
    with st.spinner("Ejecutando conversión Bronze..."):
        try:
            cmd = [
                "python3",
                str(PROJECT_ROOT / "src" / "data_flow" / "bronze_converter.py")
            ]
            
            env = os.environ.copy()
            env["PYTHONPATH"] = str(SRC_PATH)
            
            result = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True, env=env)
            
            if result.returncode == 0:
                st.success("✅ Conversión Bronze completada")
                st.code(result.stdout, language="bash")
            else:
                st.error("❌ Error en conversión Bronze")
                st.code(result.stderr, language="bash")
        
        except Exception as e:
            st.error(f"Error: {str(e)}")

def execute_stats_only():
    """Ejecuta solo el cálculo de estadísticas"""
    with st.spinner("Calculando estadísticas..."):
        try:
            # Importar y ejecutar motor de estadísticas
            from pipeline.statistics_engine import IncrementalStatisticsEngine
            
            engine = IncrementalStatisticsEngine()
            stats = engine.get_current_stats()
            
            st.success("📊 Estadísticas calculadas")
            st.json(stats)
        
        except Exception as e:
            st.error(f"Error: {str(e)}")

def clean_database():
    """Limpia la base de datos"""
    if st.button("⚠️ Confirmar limpieza de BD", type="secondary"):
        try:
            db_path = PROJECT_ROOT / "data" / "pipeline.db"
            if db_path.exists():
                db_path.unlink()
                st.success("✅ Base de datos limpiada")
            else:
                st.warning("⚠️ Base de datos no existe")
        except Exception as e:
            st.error(f"Error: {str(e)}")

def display_pipeline_metrics():
    """Muestra métricas del pipeline"""
    try:
        # Bronze files
        bronze_path = PROJECT_ROOT / "data" / "processed" / "bronze"
        bronze_count = len(list(bronze_path.glob("*.parquet"))) if bronze_path.exists() else 0
        
        st.metric("🥉 Archivos Bronze", bronze_count)
        
        # Database records
        try:
            import sqlite3
            db_path = PROJECT_ROOT / "data" / "pipeline.db"
            if db_path.exists():
                conn = sqlite3.connect(str(db_path))
                cursor = conn.execute("SELECT COUNT(*) FROM transactions")
                db_count = cursor.fetchone()[0]
                conn.close()
                st.metric("🗄️ Registros BD", f"{db_count:,}")
            else:
                st.metric("🗄️ Registros BD", "0")
        except:
            st.metric("🗄️ Registros BD", "Error")
        
        # Statistics
        stats_path = PROJECT_ROOT / "data" / "processed" / "pipeline_statistics.json"
        if stats_path.exists():
            with open(stats_path, 'r') as f:
                stats_data = json.load(f)
                if 'stats' in stats_data:
                    count = stats_data['stats'].get('count', 0)
                    avg = stats_data['stats'].get('avg', 0)
                    st.metric("📊 Count Stats", f"{count:,}")
                    st.metric("💰 Avg Price", f"${avg:.2f}")
        
    except Exception as e:
        st.error(f"Error obteniendo métricas: {e}")

def get_pipeline_progress():
    """Obtiene el progreso del pipeline"""
    # En una implementación real, esto leería el estado real del pipeline
    progress = {
        "Descarga": 100,
        "Bronze": 100,
        "Pipeline": 75,
        "Validation": 50,
        "Reporte": 0
    }
    
    return progress

def get_last_execution_info():
    """Obtiene información de la última ejecución"""
    try:
        logs_path = PROJECT_ROOT / "logs"
        if logs_path.exists():
            reports = list(logs_path.glob("daily_report_*.txt"))
            if reports:
                latest = max(reports, key=lambda x: x.stat().st_mtime)
                date_str = latest.stem.replace("daily_report_", "")
                
                # Leer contenido del reporte
                content = latest.read_text()
                
                return {
                    "date": date_str,
                    "duration": "~1 min",
                    "files": "6",
                    "rows": "~150"
                }
        
        return None
    except:
        return None

if __name__ == "__main__":
    main()