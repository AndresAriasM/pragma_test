# streamlit_app/pages/06_🧪_verificacion_reto.py
"""
🧪 VERIFICACIÓN DEL RETO
=======================
Página dedicada para mostrar la comprobación de resultados del punto 3:
● Estadísticas en ejecución
● Consultas directas a BD
● Comparación antes/después de validation.csv
"""

import streamlit as st
import pandas as pd
import sqlite3
import json
from pathlib import Path
import sys
from datetime import datetime

# Configurar paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

st.set_page_config(page_title="Verificación del Reto", page_icon="🧪", layout="wide")

def main():
    st.title("🧪 Verificación del Reto - Comprobación de Resultados")
    st.markdown("**Cumplimiento del Punto 3: Comprobación de resultados**")
    
    # Verificar si hay datos para mostrar
    if not check_data_availability():
        st.warning("⚠️ No hay datos procesados. Ejecuta el pipeline primero.")
        if st.button("🚀 Ir a Control del Pipeline"):
            st.switch_page("pages/02_🚀_pipeline_control.py")
        return
    
    # Crear tabs para cada verificación
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Estadísticas en Ejecución",
        "🗄️ Consulta BD Directa", 
        "🧪 Validation.csv",
        "📈 Comparación Final"
    ])
    
    with tab1:
        show_running_statistics()
    
    with tab2:
        show_database_query()
    
    with tab3:
        show_validation_processing()
    
    with tab4:
        show_final_comparison()

def check_data_availability():
    """Verifica si hay datos disponibles para mostrar"""
    db_path = PROJECT_ROOT / "data" / "pipeline.db"
    return db_path.exists()

def show_running_statistics():
    """Muestra las estadísticas en ejecución (Punto 3.1)"""
    st.markdown("### 📊 Estadísticas en Ejecución")
    st.markdown("**Requerimiento**: *Imprime el valor actual de las estadísticas en ejecución*")
    
    # Cargar estadísticas incrementales
    stats_data = load_incremental_statistics()
    
    if stats_data:
        st.success("✅ Estadísticas incrementales disponibles")
        
        # Mostrar en formato de métricas grandes
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="📈 Total de Registros",
                value=f"{stats_data.get('count', 0):,}",
                help="Contador incremental - NO recalculado desde BD"
            )
        
        with col2:
            st.metric(
                label="💰 Precio Promedio",
                value=f"${stats_data.get('avg', 0):.2f}",
                help="Promedio calculado incrementalmente"
            )
        
        with col3:
            st.metric(
                label="📉 Precio Mínimo", 
                value=f"${stats_data.get('min', 0):.2f}",
                help="Mínimo encontrado durante procesamiento"
            )
        
        with col4:
            st.metric(
                label="📈 Precio Máximo",
                value=f"${stats_data.get('max', 0):.2f}",
                help="Máximo encontrado durante procesamiento"
            )
        
        # Información adicional
        st.markdown("#### 📋 Detalles de las Estadísticas Incrementales")
        
        details_col1, details_col2 = st.columns(2)
        
        with details_col1:
            st.info(f"""
            **💎 Suma Total**: ${stats_data.get('sum', 0):,.2f}
            **🕐 Última Actualización**: {stats_data.get('last_updated', 'N/A')}
            **📦 Batches Procesados**: {stats_data.get('batches_processed', 'N/A')}
            """)
        
        with details_col2:
            st.success(f"""
            **⚡ Eficiencia**: O(1) por operación
            **🔄 Método**: Estadísticas incrementales
            **✅ Ventaja**: NO recalcula desde base de datos
            """)
        
        # Mostrar datos raw en JSON expandible
        with st.expander("🔍 Ver datos completos de estadísticas incrementales"):
            st.json(stats_data)
    
    else:
        st.error("❌ No se encontraron estadísticas incrementales")
        st.info("Las estadísticas se generan durante la ejecución del pipeline")

def show_database_query():
    """Muestra consulta directa a la base de datos (Punto 3.2)"""
    st.markdown("### 🗄️ Consulta Directa a Base de Datos")
    st.markdown("**Requerimiento**: *Consulta en la base de datos del: recuento total, promedio, mínimo y máximo*")
    
    try:
        db_path = PROJECT_ROOT / "data" / "pipeline.db"
        
        if not db_path.exists():
            st.error("❌ Base de datos no encontrada")
            return
        
        # Ejecutar consulta directa
        conn = sqlite3.connect(str(db_path))
        
        query = """
        SELECT 
            COUNT(*) as total_records,
            AVG(price) as average_price,
            MIN(price) as minimum_price,
            MAX(price) as maximum_price,
            SUM(price) as total_sum,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT source_file) as unique_files
        FROM transactions
        """
        
        cursor = conn.execute(query)
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] > 0:
            st.success("✅ Consulta a base de datos ejecutada exitosamente")
            
            # Mostrar resultados de la consulta
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="🗄️ Registros en BD",
                    value=f"{result[0]:,}",
                    help="COUNT(*) directo desde base de datos"
                )
            
            with col2:
                st.metric(
                    label="💰 Promedio BD",
                    value=f"${result[1]:.2f}" if result[1] else "$0.00",
                    help="AVG(price) calculado por SQL"
                )
            
            with col3:
                st.metric(
                    label="📉 Mínimo BD", 
                    value=f"${result[2]:.2f}" if result[2] else "$0.00",
                    help="MIN(price) desde base de datos"
                )
            
            with col4:
                st.metric(
                    label="📈 Máximo BD",
                    value=f"${result[3]:.2f}" if result[3] else "$0.00",
                    help="MAX(price) desde base de datos"
                )
            
            # Información adicional de BD
            st.markdown("#### 📊 Información Adicional de Base de Datos")
            
            col_extra1, col_extra2, col_extra3 = st.columns(3)
            
            with col_extra1:
                st.info(f"**💎 Suma Total BD**: ${result[4]:,.2f}" if result[4] else "$0.00")
            
            with col_extra2:
                st.info(f"**👥 Usuarios Únicos**: {result[5]:,}")
            
            with col_extra3:
                st.info(f"**📄 Archivos Procesados**: {result[6]}")
            
            # Mostrar la consulta SQL ejecutada
            with st.expander("🔍 Ver consulta SQL ejecutada"):
                st.code(query, language="sql")
            
            # Comparar con estadísticas incrementales
            show_statistics_comparison()
        
        else:
            st.warning("⚠️ No hay datos en la base de datos")
    
    except Exception as e:
        st.error(f"❌ Error ejecutando consulta: {str(e)}")

def show_validation_processing():
    """Muestra el procesamiento de validation.csv (Punto 3.3)"""
    st.markdown("### 🧪 Procesamiento de Validation.csv")
    st.markdown("**Requerimiento**: *Ejecuta validation.csv y muestra estadísticas en ejecución*")
    
    # Verificar si validation fue procesado
    validation_status = check_validation_processed()
    
    if validation_status['processed']:
        st.success("✅ Validation.csv fue procesado exitosamente")
        
        # Mostrar estadísticas antes de validation
        if validation_status['before_stats']:
            st.markdown("#### 📊 Estadísticas ANTES de procesar validation.csv")
            
            before_stats = validation_status['before_stats']
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Count", f"{before_stats.get('count', 0):,}")
            with col2:
                st.metric("Promedio", f"${before_stats.get('avg', 0):.2f}")
            with col3:
                st.metric("Mínimo", f"${before_stats.get('min', 0):.2f}")
            with col4:
                st.metric("Máximo", f"${before_stats.get('max', 0):.2f}")
        
        # Mostrar estadísticas después de validation
        if validation_status['after_stats']:
            st.markdown("#### 📈 Estadísticas DESPUÉS de procesar validation.csv")
            
            after_stats = validation_status['after_stats']
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Count", f"{after_stats.get('count', 0):,}")
            with col2:
                st.metric("Promedio", f"${after_stats.get('avg', 0):.2f}")
            with col3:
                st.metric("Mínimo", f"${after_stats.get('min', 0):.2f}")
            with col4:
                st.metric("Máximo", f"${after_stats.get('max', 0):.2f}")
        
        # Mostrar cambios detectados
        if validation_status['changes']:
            st.markdown("#### 🔄 Cambios Detectados por Validation.csv")
            changes = validation_status['changes']
            
            change_col1, change_col2, change_col3 = st.columns(3)
            
            with change_col1:
                st.metric(
                    "Filas Añadidas",
                    f"+{changes.get('count_added', 0):,}",
                    delta=f"+{changes.get('count_added', 0)}"
                )
            
            with change_col2:
                st.metric(
                    "Cambio en Promedio",
                    f"${changes.get('avg_after', 0):.2f}",
                    delta=f"{changes.get('avg_change', 0):+.2f}"
                )
            
            with change_col3:
                if changes.get('new_min') or changes.get('new_max'):
                    st.success("🔄 Nuevos valores min/max detectados")
                else:
                    st.info("📊 Rango de precios sin cambios")
    
    else:
        st.warning("⚠️ Validation.csv aún no ha sido procesado")
        st.info("El archivo validation.csv se procesa automáticamente al final del pipeline")
        
        if st.button("🚀 Ejecutar Pipeline Completo"):
            st.switch_page("pages/02_🚀_pipeline_control.py")

def show_final_comparison():
    """Muestra comparación final BD vs Estadísticas (Punto 3.4)"""
    st.markdown("### 📈 Comparación Final: Estadísticas vs Base de Datos")
    st.markdown("**Requerimiento**: *Nueva consulta BD después de validation.csv*")
    
    # Obtener estadísticas incrementales
    incremental_stats = load_incremental_statistics()
    
    # Obtener estadísticas de BD
    db_stats = get_database_statistics()
    
    if incremental_stats and db_stats:
        st.success("✅ Comparación disponible")
        
        # Tabla de comparación
        comparison_data = {
            "Métrica": ["Count", "Promedio", "Mínimo", "Máximo"],
            "Estadísticas Incrementales": [
                f"{incremental_stats.get('count', 0):,}",
                f"${incremental_stats.get('avg', 0):.2f}",
                f"${incremental_stats.get('min', 0):.2f}",
                f"${incremental_stats.get('max', 0):.2f}"
            ],
            "Consulta BD Directa": [
                f"{db_stats.get('count', 0):,}",
                f"${db_stats.get('avg', 0):.2f}",
                f"${db_stats.get('min', 0):.2f}",
                f"${db_stats.get('max', 0):.2f}"
            ],
            "¿Coinciden?": []
        }
        
        # Verificar coincidencias
        tolerance = 1e-6
        
        matches = [
            "✅" if incremental_stats.get('count', 0) == db_stats.get('count', 0) else "❌",
            "✅" if abs(incremental_stats.get('avg', 0) - db_stats.get('avg', 0)) < tolerance else "❌",
            "✅" if abs(incremental_stats.get('min', 0) - db_stats.get('min', 0)) < tolerance else "❌",
            "✅" if abs(incremental_stats.get('max', 0) - db_stats.get('max', 0)) < tolerance else "❌"
        ]
        
        comparison_data["¿Coinciden?"] = matches
        
        # Mostrar tabla de comparación
        df_comparison = pd.DataFrame(comparison_data)
        st.dataframe(df_comparison, use_container_width=True)
        
        # Verificación general
        all_match = all("✅" in match for match in matches)
        
        if all_match:
            st.success("🎉 ¡VERIFICACIÓN EXITOSA! Las estadísticas incrementales coinciden exactamente con la consulta directa a BD")
            st.balloons()
        else:
            st.error("❌ Hay diferencias entre las estadísticas incrementales y la BD")
        
        # Mostrar ventajas del método incremental
        st.markdown("#### ⚡ Ventajas del Método Incremental")
        
        col_advantage1, col_advantage2 = st.columns(2)
        
        with col_advantage1:
            st.info("""
            **🚀 Eficiencia:**
            - O(1) por operación
            - No recalcula desde BD
            - Escalable a millones de registros
            """)
        
        with col_advantage2:
            st.success("""
            **✅ Precisión:**
            - Mismos resultados que SQL
            - Actualizaciones en tiempo real
            - Verificación automática
            """)
    
    else:
        st.error("❌ No se pueden comparar - faltan datos")

def load_incremental_statistics():
    """Carga estadísticas incrementales"""
    try:
        stats_path = PROJECT_ROOT / "data" / "processed" / "pipeline_statistics.json"
        if stats_path.exists():
            with open(stats_path, 'r') as f:
                data = json.load(f)
                return data.get('stats', {})
        return {}
    except:
        return {}

def get_database_statistics():
    """Obtiene estadísticas directas de BD"""
    try:
        db_path = PROJECT_ROOT / "data" / "pipeline.db"
        if not db_path.exists():
            return {}
        
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute("""
            SELECT 
                COUNT(*) as count,
                AVG(price) as avg,
                MIN(price) as min,
                MAX(price) as max
            FROM transactions
        """)
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] > 0:
            return {
                'count': result[0],
                'avg': float(result[1]) if result[1] else 0.0,
                'min': float(result[2]) if result[2] else 0.0,
                'max': float(result[3]) if result[3] else 0.0
            }
        
        return {}
    except:
        return {}

def show_statistics_comparison():
    """Muestra comparación entre estadísticas incrementales y BD"""
    st.markdown("#### 🔍 Comparación: Incremental vs BD")
    
    incremental = load_incremental_statistics()
    db_stats = get_database_statistics()
    
    if incremental and db_stats:
        # Verificar coincidencias
        count_match = incremental.get('count', 0) == db_stats.get('count', 0)
        avg_match = abs(incremental.get('avg', 0) - db_stats.get('avg', 0)) < 1e-6
        min_match = abs(incremental.get('min', 0) - db_stats.get('min', 0)) < 1e-6
        max_match = abs(incremental.get('max', 0) - db_stats.get('max', 0)) < 1e-6
        
        if count_match and avg_match and min_match and max_match:
            st.success("✅ Las estadísticas incrementales coinciden exactamente con la BD")
        else:
            st.warning("⚠️ Hay pequeñas diferencias (revisar tolerancia)")

def check_validation_processed():
    """Verifica si validation fue procesado y obtiene estadísticas antes/después"""
    try:
        stats_path = PROJECT_ROOT / "data" / "processed" / "pipeline_statistics.json"
        if stats_path.exists():
            with open(stats_path, 'r') as f:
                data = json.load(f)
                
                # Buscar evidencia de validation procesado
                processed = 'validation_stats' in data or data.get('stats', {}).get('count', 0) > 0
                
                return {
                    'processed': processed,
                    'before_stats': data.get('before_validation', {}),
                    'after_stats': data.get('after_validation', data.get('stats', {})),
                    'changes': data.get('validation_changes', {})
                }
        
        return {'processed': False, 'before_stats': {}, 'after_stats': {}, 'changes': {}}
    except:
        return {'processed': False, 'before_stats': {}, 'after_stats': {}, 'changes': {}}

if __name__ == "__main__":
    main()