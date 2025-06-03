# streamlit_app/main.py
"""
🚀 PIPELINE DE DATOS - INTERFAZ INTERACTIVA
==========================================
Dashboard principal para el pipeline de ingeniería de datos de Pragma
"""

import streamlit as st
import sys
from pathlib import Path
import os
from datetime import datetime
import time

# Configurar rutas
PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
STREAMLIT_PATH = PROJECT_ROOT / "streamlit_app"

# Agregar paths al sistema
sys.path.insert(0, str(SRC_PATH))
sys.path.insert(0, str(STREAMLIT_PATH))

# Configuración de la página
st.set_page_config(
    page_title="Pipeline de Datos - Pragma",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/tu-usuario/pragma-test',
        'Report a bug': 'https://github.com/tu-usuario/pragma-test/issues',
        'About': "# Pipeline de Datos\nDesarrollado para el reto técnico de Pragma"
    }
)

# CSS personalizado
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
    }
    .status-success {
        background: #d4edda;
        color: #155724;
        padding: 0.5rem;
        border-radius: 5px;
        border-left: 4px solid #28a745;
    }
    .status-error {
        background: #f8d7da;
        color: #721c24;
        padding: 0.5rem;
        border-radius: 5px;
        border-left: 4px solid #dc3545;
    }
    .status-warning {
        background: #fff3cd;
        color: #856404;
        padding: 0.5rem;
        border-radius: 5px;
        border-left: 4px solid #ffc107;
    }
</style>
""", unsafe_allow_html=True)

def main():
    """Función principal de la aplicación"""
    
    # Header principal
    st.markdown("""
    <div class="main-header">
        <h1>🚀 Pipeline de Datos - Pragma</h1>
        <p>Interfaz interactiva para el pipeline de ingeniería de datos</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar con navegación
    with st.sidebar:
        st.image("https://via.placeholder.com/200x100/1f77b4/white?text=PRAGMA", width=200)
        
        st.markdown("### 🧭 Navegación")
        st.markdown("""
        - 🏠 **Dashboard**: Vista general del sistema
        - 🚀 **Control Pipeline**: Ejecutar y monitorear pipeline
        - 📊 **Explorador**: Explorar datos procesados
        - 📈 **Analytics**: Análisis y visualizaciones
        - 🗄️ **Base de Datos**: Consultar BD directamente
        - ⚙️ **Configuración**: Ajustes del sistema
        """)
        
        st.markdown("---")
        
        # Estado del sistema
        st.markdown("### 📊 Estado del Sistema")
        
        # Verificar estado de componentes
        status_data = check_system_status()
        
        for component, status in status_data.items():
            if status["status"] == "success":
                st.markdown(f"✅ **{component}**: {status['message']}")
            elif status["status"] == "warning":
                st.markdown(f"⚠️ **{component}**: {status['message']}")
            else:
                st.markdown(f"❌ **{component}**: {status['message']}")
        
        st.markdown("---")
        st.markdown("### ℹ️ Información")
        st.info(f"""
        **Proyecto**: {PROJECT_ROOT.name}
        **Última actualización**: {datetime.now().strftime('%H:%M:%S')}
        **Python**: {sys.version.split()[0]}
        """)

    # Contenido principal - Dashboard básico
    display_main_dashboard()

def check_system_status():
    """Verifica el estado de los componentes del sistema"""
    status = {}
    
    # Verificar archivos Bronze
    bronze_path = PROJECT_ROOT / "data" / "processed" / "bronze"
    if bronze_path.exists():
        parquet_files = list(bronze_path.glob("*.parquet"))
        if len(parquet_files) >= 5:
            status["Bronze Layer"] = {
                "status": "success",
                "message": f"{len(parquet_files)} archivos"
            }
        else:
            status["Bronze Layer"] = {
                "status": "warning", 
                "message": f"Solo {len(parquet_files)} archivos"
            }
    else:
        status["Bronze Layer"] = {
            "status": "error",
            "message": "No encontrado"
        }
    
    # Verificar base de datos
    db_path = PROJECT_ROOT / "data" / "pipeline.db"
    if db_path.exists():
        size_mb = db_path.stat().st_size / 1024 / 1024
        status["Base de Datos"] = {
            "status": "success",
            "message": f"{size_mb:.1f} MB"
        }
    else:
        status["Base de Datos"] = {
            "status": "error",
            "message": "No encontrada"
        }
    
    # Verificar Airflow
    airflow_path = PROJECT_ROOT / "airflow_config"
    if airflow_path.exists():
        status["Airflow"] = {
            "status": "success",
            "message": "Configurado"
        }
    else:
        status["Airflow"] = {
            "status": "warning",
            "message": "No configurado"
        }
    
    return status

def display_main_dashboard():
    """Muestra el dashboard principal"""
    
    # Métricas principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📄 Archivos Bronze",
            value=get_bronze_files_count(),
            delta="Parquet optimizado"
        )
    
    with col2:
        st.metric(
            label="🗄️ Registros en BD",
            value=get_database_records_count(),
            delta="SQLite"
        )
    
    with col3:
        st.metric(
            label="📊 Estadísticas", 
            value="Activas",
            delta="Incrementales O(1)"
        )
    
    with col4:
        st.metric(
            label="⚡ Pipeline",
            value=get_pipeline_status(),
            delta="Micro-batches"
        )
    
    st.markdown("---")
    
    # Quick Actions
    st.markdown("### 🎯 Acciones Rápidas")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🚀 Ejecutar Pipeline Completo", use_container_width=True):
            st.info("Funcionalidad disponible en la página 'Control Pipeline'")
    
    with col2:
        if st.button("📊 Ver Datos Procesados", use_container_width=True):
            st.info("Funcionalidad disponible en la página 'Explorador'")
    
    with col3:
        if st.button("📈 Generar Análisis", use_container_width=True):
            st.info("Funcionalidad disponible en la página 'Analytics'")
    
    # Información del proyecto
    st.markdown("---")
    st.markdown("### 📋 Resumen del Proyecto")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        **Pipeline de Ingeniería de Datos - Reto Pragma**
        
        Este sistema implementa un pipeline completo de datos que incluye:
        
        - **🥉 Bronze Layer**: Conversión CSV → Parquet con compresión
        - **⚡ Micro-batches**: Procesamiento eficiente de 1,000 filas por batch
        - **📊 Estadísticas O(1)**: Cálculo incremental sin recalcular desde BD
        - **🗄️ Base de Datos**: SQLite con esquemas optimizados
        - **🌪️ Orquestación**: Apache Airflow para automatización
        - **📱 Interfaz**: Streamlit para visualización interactiva
        
        **Tecnologías utilizadas:**
        - Python 3.11, Pandas, PyArrow, SQLAlchemy
        - Apache Airflow, Streamlit
        - SQLite, Parquet, Micro-batches
        """)
    
    with col2:
        st.markdown("#### 🏗️ Arquitectura")
        st.markdown("""
        ```
        CSV Files
            ↓
        Bronze (Parquet)
            ↓
        Pipeline Engine
            ↓
        SQLite + Stats
            ↓
        Analytics Dashboard
        ```
        """)
        
        # Mostrar última actividad
        st.markdown("#### 📅 Última Actividad")
        try:
            last_report = get_last_report_date()
            if last_report:
                st.success(f"Reporte: {last_report}")
            else:
                st.warning("Sin reportes recientes")
        except:
            st.error("Error verificando reportes")

def get_bronze_files_count():
    """Obtiene el número de archivos Bronze"""
    try:
        bronze_path = PROJECT_ROOT / "data" / "processed" / "bronze"
        if bronze_path.exists():
            return len(list(bronze_path.glob("*.parquet")))
        return 0
    except:
        return "Error"

def get_database_records_count():
    """Obtiene el número de registros en la base de datos"""
    try:
        import sqlite3
        db_path = PROJECT_ROOT / "data" / "pipeline.db"
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            cursor = conn.execute("SELECT COUNT(*) FROM transactions")
            count = cursor.fetchone()[0]
            conn.close()
            return f"{count:,}"
        return "0"
    except:
        return "Error"

def get_pipeline_status():
    """Obtiene el estado del pipeline"""
    try:
        # Verificar si hay estadísticas recientes
        stats_path = PROJECT_ROOT / "data" / "processed" / "pipeline_statistics.json"
        if stats_path.exists():
            return "Listo"
        return "Pendiente"
    except:
        return "Error"

def get_last_report_date():
    """Obtiene la fecha del último reporte"""
    try:
        logs_path = PROJECT_ROOT / "logs"
        if logs_path.exists():
            reports = list(logs_path.glob("daily_report_*.txt"))
            if reports:
                latest = max(reports, key=lambda x: x.stat().st_mtime)
                date_str = latest.stem.replace("daily_report_", "")
                return date_str
        return None
    except:
        return None

if __name__ == "__main__":
    main()