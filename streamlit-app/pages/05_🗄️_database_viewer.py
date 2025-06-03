# streamlit_app/pages/05_🗄️_database_viewer.py
"""
🗄️ VISOR DE BASE DE DATOS
========================
Página para consultar y explorar la base de datos SQLite
"""

import streamlit as st
import sqlite3
import pandas as pd
from pathlib import Path
import sys

# Configurar paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

st.set_page_config(page_title="Database Viewer", page_icon="🗄️", layout="wide")

def main():
    st.title("🗄️ Visor de Base de Datos")
    st.markdown("Consulta directa a la base de datos SQLite del pipeline")
    
    # Verificar si existe la base de datos
    db_path = PROJECT_ROOT / "data" / "pipeline.db"
    
    if not db_path.exists():
        st.error("❌ Base de datos no encontrada")
        st.info("Ejecuta el pipeline primero para crear la base de datos")
        
        if st.button("🚀 Ir a Control del Pipeline"):
            st.switch_page("pages/02_🚀_pipeline_control.py")
        return
    
    # Sidebar con información de la BD
    with st.sidebar:
        st.markdown("### 🗄️ Información de BD")
        
        try:
            # Información del archivo
            file_size = db_path.stat().st_size / 1024 / 1024
            st.metric("📁 Tamaño", f"{file_size:.2f} MB")
            
            # Información de tablas
            tables_info = get_tables_info(db_path)
            
            st.markdown("### 📋 Tablas")
            for table_name, count in tables_info.items():
                st.metric(f"📊 {table_name}", f"{count:,} registros")
            
            st.markdown("---")
            
            # Selector de tabla para consultas predefinidas
            st.markdown("### 🔍 Consultas Rápidas")
            selected_table = st.selectbox("Tabla", list(tables_info.keys()))
            
            if st.button("📊 Ver muestra", use_container_width=True):
                st.session_state.quick_query = f"SELECT * FROM {selected_table} LIMIT 100"
            
            if st.button("📈 Estadísticas", use_container_width=True):
                if selected_table == "transactions":
                    st.session_state.quick_query = """
                    SELECT 
                        COUNT(*) as total_records,
                        AVG(price) as avg_price,
                        MIN(price) as min_price,
                        MAX(price) as max_price,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(DISTINCT source_file) as unique_files
                    FROM transactions
                    """
            
            if st.button("🏷️ Por usuario", use_container_width=True):
                if selected_table == "transactions":
                    st.session_state.quick_query = """
                    SELECT 
                        user_id,
                        COUNT(*) as transactions,
                        AVG(price) as avg_price,
                        MIN(price) as min_price,
                        MAX(price) as max_price
                    FROM transactions 
                    GROUP BY user_id 
                    ORDER BY transactions DESC 
                    LIMIT 20
                    """
        
        except Exception as e:
            st.error(f"Error: {e}")
    
    # Área principal
    tab1, tab2, tab3 = st.tabs(["🔍 Consulta SQL", "📊 Explorador", "📋 Esquema"])
    
    with tab1:
        display_sql_interface(db_path)
    
    with tab2:
        display_table_explorer(db_path)
    
    with tab3:
        display_schema_info(db_path)

def get_tables_info(db_path):
    """Obtiene información de las tablas"""
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Obtener lista de tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        tables_info = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            tables_info[table] = count
        
        conn.close()
        return tables_info
    
    except Exception as e:
        st.error(f"Error obteniendo info de tablas: {e}")
        return {}

def display_sql_interface(db_path):
    """Muestra interfaz para consultas SQL"""
    st.markdown("### 🔍 Ejecutor de Consultas SQL")
    
    # Editor de SQL
    default_query = getattr(st.session_state, 'quick_query', "SELECT * FROM transactions LIMIT 10")
    
    sql_query = st.text_area(
        "Escribe tu consulta SQL:",
        value=default_query,
        height=150,
        help="Escribe cualquier consulta SQL válida para SQLite"
    )
    
    col1, col2, col3 = st.columns([1, 1, 4])
    
    with col1:
        execute_button = st.button("▶️ Ejecutar", type="primary", use_container_width=True)
    
    with col2:
        if st.button("🧹 Limpiar", use_container_width=True):
            st.session_state.quick_query = ""
            st.rerun()
    
    # Ejemplos de consultas
    with st.expander("💡 Ejemplos de Consultas"):
        st.code("""
-- Estadísticas básicas
SELECT COUNT(*) as total, AVG(price) as avg_price, MIN(price) as min_price, MAX(price) as max_price 
FROM transactions;

-- Top usuarios por transacciones
SELECT user_id, COUNT(*) as transactions, AVG(price) as avg_price 
FROM transactions 
GROUP BY user_id 
ORDER BY transactions DESC 
LIMIT 10;

-- Transacciones por archivo fuente
SELECT source_file, COUNT(*) as count, AVG(price) as avg_price 
FROM transactions 
GROUP BY source_file;

-- Precios por rango
SELECT 
    CASE 
        WHEN price < 20 THEN 'Bajo (< $20)'
        WHEN price < 50 THEN 'Medio ($20-$50)'
        WHEN price < 80 THEN 'Alto ($50-$80)'
        ELSE 'Muy Alto (> $80)'
    END as price_range,
    COUNT(*) as count
FROM transactions 
GROUP BY price_range;

-- Verificación de batches
SELECT batch_id, COUNT(*) as records, source_file 
FROM transactions 
WHERE batch_id IS NOT NULL 
GROUP BY batch_id 
ORDER BY records DESC;
        """, language="sql")
    
    # Ejecutar consulta
    if execute_button and sql_query.strip():
        execute_sql_query(db_path, sql_query)

def execute_sql_query(db_path, query):
    """Ejecuta una consulta SQL y muestra resultados"""
    try:
        conn = sqlite3.connect(str(db_path))
        
        # Verificar que la consulta sea segura (solo SELECT)
        query_upper = query.strip().upper()
        if not query_upper.startswith('SELECT'):
            st.error("❌ Solo se permiten consultas SELECT por seguridad")
            return
        
        # Ejecutar consulta
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Mostrar resultados
        if len(df) > 0:
            st.success(f"✅ Consulta ejecutada: {len(df)} filas retornadas")
            
            # Métricas rápidas
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📊 Filas", len(df))
            with col2:
                st.metric("📊 Columnas", len(df.columns))
            with col3:
                memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                st.metric("💾 Memoria", f"{memory_mb:.2f} MB")
            
            # Tabla de resultados
            st.dataframe(df, use_container_width=True, height=400)
            
            # Opción de descargar
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Descargar CSV",
                data=csv,
                file_name=f"query_result_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("⚠️ La consulta no retornó resultados")
    
    except Exception as e:
        st.error(f"❌ Error ejecutando consulta: {str(e)}")
        st.code(str(e), language="text")

def display_table_explorer(db_path):
    """Muestra explorador visual de tablas"""
    st.markdown("### 📊 Explorador de Tablas")
    
    try:
        tables_info = get_tables_info(db_path)
        
        if not tables_info:
            st.warning("No se encontraron tablas")
            return
        
        # Selector de tabla
        selected_table = st.selectbox("Selecciona una tabla", list(tables_info.keys()))
        
        if selected_table:
            conn = sqlite3.connect(str(db_path))
            
            # Información de la tabla
            st.markdown(f"#### 📋 Tabla: `{selected_table}`")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("📊 Total registros", f"{tables_info[selected_table]:,}")
            
            with col2:
                # Obtener info de columnas
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({selected_table})")
                columns_info = cursor.fetchall()
                st.metric("📊 Total columnas", len(columns_info))
            
            # Mostrar esquema de la tabla
            st.markdown("##### 🏗️ Esquema")
            schema_df = pd.DataFrame(columns_info, columns=['Index', 'Nombre', 'Tipo', 'NotNull', 'Default', 'PK'])
            st.dataframe(schema_df, use_container_width=True)
            
            # Muestra de datos
            st.markdown("##### 📋 Muestra de Datos")
            
            # Controles para la muestra
            col1, col2 = st.columns(2)
            with col1:
                limit = st.number_input("Número de filas", 1, 1000, 100)
            with col2:
                offset = st.number_input("Saltar filas", 0, tables_info[selected_table], 0)
            
            # Cargar y mostrar datos
            query = f"SELECT * FROM {selected_table} LIMIT {limit} OFFSET {offset}"
            df = pd.read_sql_query(query, conn)
            
            if len(df) > 0:
                st.dataframe(df, use_container_width=True, height=400)
                
                # Estadísticas rápidas para columnas numéricas
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    st.markdown("##### 📊 Estadísticas Rápidas")
                    stats_df = df[numeric_cols].describe()
                    st.dataframe(stats_df, use_container_width=True)
            
            conn.close()
    
    except Exception as e:
        st.error(f"Error en explorador: {e}")

def display_schema_info(db_path):
    """Muestra información del esquema de la base de datos"""
    st.markdown("### 📋 Esquema de Base de Datos")
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Obtener todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            with st.expander(f"📊 Tabla: {table}"):
                # Información de columnas
                cursor.execute(f"PRAGMA table_info({table})")
                columns_info = cursor.fetchall()
                
                if columns_info:
                    columns_df = pd.DataFrame(
                        columns_info, 
                        columns=['Index', 'Nombre', 'Tipo', 'NotNull', 'Default', 'PrimaryKey']
                    )
                    st.dataframe(columns_df, use_container_width=True)
                
                # Índices de la tabla
                cursor.execute(f"PRAGMA index_list({table})")
                indexes = cursor.fetchall()
                
                if indexes:
                    st.markdown("**Índices:**")
                    for idx in indexes:
                        st.write(f"- {idx[1]} ({'UNIQUE' if idx[2] else 'NON-UNIQUE'})")
                
                # Conteo de registros
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                st.metric("Total registros", f"{count:,}")
        
        # Información adicional
        st.markdown("---")
        st.markdown("### 🔧 Información Técnica")
        
        # Tamaño de la base de datos
        file_size = db_path.stat().st_size
        st.metric("Tamaño archivo", f"{file_size / 1024 / 1024:.2f} MB")
        
        # Versión de SQLite
        cursor.execute("SELECT sqlite_version()")
        sqlite_version = cursor.fetchone()[0]
        st.info(f"SQLite versión: {sqlite_version}")
        
        # Configuración de la BD
        st.markdown("#### ⚙️ Configuración")
        
        pragma_settings = [
            "page_size", "cache_size", "temp_store", "journal_mode", 
            "synchronous", "foreign_keys", "auto_vacuum"
        ]
        
        config_data = []
        for setting in pragma_settings:
            try:
                cursor.execute(f"PRAGMA {setting}")
                value = cursor.fetchone()
                if value:
                    config_data.append({"Configuración": setting, "Valor": value[0]})
            except:
                pass
        
        if config_data:
            config_df = pd.DataFrame(config_data)
            st.dataframe(config_df, use_container_width=True)
        
        conn.close()
    
    except Exception as e:
        st.error(f"Error obteniendo esquema: {e}")

if __name__ == "__main__":
    main()