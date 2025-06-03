# streamlit_app/pages/03_📊_data_explorer.py
"""
📊 EXPLORADOR DE DATOS
=====================
Página para explorar datos procesados del pipeline
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import sqlite3

# Configurar paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

st.set_page_config(page_title="Explorador de Datos", page_icon="📊", layout="wide")

def main():
    st.title("📊 Explorador de Datos")
    st.markdown("Explora y analiza los datos procesados por el pipeline")
    
    # Sidebar con controles
    with st.sidebar:
        st.markdown("### 🎛️ Controles")
        
        # Selector de fuente de datos
        data_source = st.selectbox(
            "Fuente de datos",
            ["Base de Datos", "Archivos Bronze", "Archivos CSV"],
            help="Selecciona de dónde cargar los datos"
        )
        
        # Filtros
        st.markdown("### 🔍 Filtros")
        
        if data_source == "Base de Datos":
            # Filtros para BD
            date_range = st.date_input("Rango de fechas", value=[], help="Filtrar por rango de fechas")
            price_range = st.slider("Rango de precios", 0, 200, (0, 200))
            user_filter = st.text_input("Usuario específico", placeholder="ej: user_1")
            
        elif data_source == "Archivos Bronze":
            # Selector de archivo Bronze
            bronze_files = get_bronze_files()
            selected_file = st.selectbox("Archivo Parquet", bronze_files)
            
        else:
            # Selector de archivo CSV
            csv_files = get_csv_files()
            selected_csv = st.selectbox("Archivo CSV", csv_files)
        
        # Opciones de visualización
        st.markdown("### 📈 Visualización")
        sample_size = st.number_input("Tamaño de muestra", 100, 10000, 1000)
        show_raw_data = st.checkbox("Mostrar datos raw", value=True)
        
        # Botón de actualizar
        if st.button("🔄 Actualizar datos", use_container_width=True):
            st.rerun()
    
    # Área principal
    try:
        # Cargar datos según la fuente seleccionada
        df = load_data(data_source, locals())
        
        if df is not None and not df.empty:
            display_data_overview(df)
            
            # Tabs para diferentes vistas
            tab1, tab2, tab3, tab4 = st.tabs(["📋 Datos", "📊 Estadísticas", "📈 Gráficos", "🔍 Análisis"])
            
            with tab1:
                display_data_table(df, show_raw_data, sample_size)
            
            with tab2:
                display_statistics(df)
            
            with tab3:
                display_charts(df)
            
            with tab4:
                display_analysis(df)
        
        else:
            st.warning("No hay datos disponibles. Ejecuta el pipeline primero.")
            
            # Botón para ir a control del pipeline
            if st.button("🚀 Ir a Control del Pipeline"):
                st.switch_page("pages/02_🚀_pipeline_control.py")
    
    except Exception as e:
        st.error(f"Error cargando datos: {str(e)}")
        st.code(str(e), language="python")

def get_bronze_files():
    """Obtiene lista de archivos Bronze disponibles"""
    bronze_path = PROJECT_ROOT / "data" / "processed" / "bronze"
    if bronze_path.exists():
        files = list(bronze_path.glob("*.parquet"))
        return [f.name for f in files] if files else ["No hay archivos"]
    return ["Directorio no existe"]

def get_csv_files():
    """Obtiene lista de archivos CSV disponibles"""
    raw_path = PROJECT_ROOT / "data" / "raw"
    csv_files = []
    
    # Buscar en raw y subdirectorios
    for path in [raw_path] + list(raw_path.glob("*/")):
        if path.is_dir():
            csv_files.extend([f.name for f in path.glob("*.csv")])
    
    return csv_files if csv_files else ["No hay archivos CSV"]

def load_data(source, filters):
    """Carga datos según la fuente seleccionada"""
    try:
        if source == "Base de Datos":
            return load_from_database(filters)
        elif source == "Archivos Bronze":
            return load_from_bronze(filters.get('selected_file'))
        elif source == "Archivos CSV":
            return load_from_csv(filters.get('selected_csv'))
        return None
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return None

def load_from_database(filters):
    """Carga datos desde la base de datos SQLite"""
    db_path = PROJECT_ROOT / "data" / "pipeline.db"
    
    if not db_path.exists():
        return None
    
    try:
        conn = sqlite3.connect(str(db_path))
        
        # Query base
        query = "SELECT * FROM transactions"
        conditions = []
        
        # Aplicar filtros
        if filters.get('price_range'):
            min_price, max_price = filters['price_range']
            conditions.append(f"price BETWEEN {min_price} AND {max_price}")
        
        if filters.get('user_filter'):
            user = filters['user_filter'].strip()
            if user:
                conditions.append(f"user_id = '{user}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY timestamp LIMIT 5000"
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convertir timestamp si es necesario
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        return df
        
    except Exception as e:
        st.error(f"Error conectando a BD: {e}")
        return None

def load_from_bronze(selected_file):
    """Carga datos desde archivos Parquet Bronze"""
    if not selected_file or selected_file == "No hay archivos":
        return None
    
    bronze_path = PROJECT_ROOT / "data" / "processed" / "bronze" / selected_file
    
    try:
        df = pd.read_parquet(bronze_path)
        return df.head(5000)  # Limitar para performance
    except Exception as e:
        st.error(f"Error leyendo Parquet: {e}")
        return None

def load_from_csv(selected_csv):
    """Carga datos desde archivos CSV"""
    if not selected_csv or selected_csv == "No hay archivos CSV":
        return None
    
    # Buscar archivo en raw y subdirectorios
    raw_path = PROJECT_ROOT / "data" / "raw"
    csv_path = None
    
    for path in [raw_path] + list(raw_path.glob("*/")):
        if path.is_dir():
            potential_path = path / selected_csv
            if potential_path.exists():
                csv_path = potential_path
                break
    
    if not csv_path:
        return None
    
    try:
        df = pd.read_csv(csv_path)
        return df.head(5000)  # Limitar para performance
    except Exception as e:
        st.error(f"Error leyendo CSV: {e}")
        return None

def display_data_overview(df):
    """Muestra overview general de los datos"""
    st.markdown("### 📋 Resumen de Datos")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Filas", f"{len(df):,}")
    
    with col2:
        st.metric("📊 Total Columnas", len(df.columns))
    
    with col3:
        if 'price' in df.columns:
            avg_price = df['price'].mean()
            st.metric("💰 Precio Promedio", f"${avg_price:.2f}")
        else:
            st.metric("💰 Precio Promedio", "N/A")
    
    with col4:
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
        st.metric("💾 Memoria", f"{memory_usage:.1f} MB")

def display_data_table(df, show_raw, sample_size):
    """Muestra tabla de datos"""
    st.markdown("### 📋 Datos")
    
    if show_raw:
        # Muestra datos raw
        display_df = df.head(sample_size)
        st.dataframe(
            display_df,
            use_container_width=True,
            height=400
        )
        
        # Info adicional
        st.markdown("#### 📊 Información de Columnas")
        info_df = pd.DataFrame({
            "Columna": df.columns,
            "Tipo": df.dtypes,
            "No Nulos": df.count(),
            "% Completo": (df.count() / len(df) * 100).round(2)
        })
        st.dataframe(info_df, use_container_width=True)
    
    else:
        st.info("Datos raw ocultados. Activar en el sidebar para ver.")

def display_statistics(df):
    """Muestra estadísticas descriptivas"""
    st.markdown("### 📊 Estadísticas Descriptivas")
    
    # Estadísticas numéricas
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) > 0:
        st.markdown("#### 🔢 Columnas Numéricas")
        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
        
        # Estadísticas adicionales
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📈 Distribución de Valores")
            for col in numeric_cols[:3]:  # Solo primeras 3 columnas
                if col in df.columns:
                    st.write(f"**{col}:**")
                    hist_data = df[col].dropna()
                    if len(hist_data) > 0:
                        fig = px.histogram(x=hist_data, title=f"Distribución de {col}")
                        st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 📊 Box Plots")
            for col in numeric_cols[:3]:
                if col in df.columns:
                    st.write(f"**{col}:**")
                    box_data = df[col].dropna()
                    if len(box_data) > 0:
                        fig = px.box(y=box_data, title=f"Box Plot de {col}")
                        st.plotly_chart(fig, use_container_width=True)
    
    # Estadísticas categóricas
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    
    if len(categorical_cols) > 0:
        st.markdown("#### 🏷️ Columnas Categóricas")
        for col in categorical_cols:
            if col in df.columns:
                st.write(f"**{col}:**")
                value_counts = df[col].value_counts().head(10)
                st.bar_chart(value_counts)

def display_charts(df):
    """Muestra gráficos interactivos"""
    st.markdown("### 📈 Visualizaciones")
    
    if 'price' in df.columns:
        # Serie temporal de precios
        if 'timestamp' in df.columns:
            st.markdown("#### 📅 Precios en el Tiempo")
            
            # Preparar datos para la serie temporal
            df_time = df.copy()
            if df_time['timestamp'].dtype == 'object':
                df_time['timestamp'] = pd.to_datetime(df_time['timestamp'], errors='coerce')
            
            df_time = df_time.dropna(subset=['timestamp', 'price'])
            df_time = df_time.sort_values('timestamp')
            
            if len(df_time) > 0:
                fig = px.line(
                    df_time, 
                    x='timestamp', 
                    y='price',
                    title="Evolución de Precios",
                    labels={'price': 'Precio ($)', 'timestamp': 'Fecha'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Distribución de precios por usuario
        if 'user_id' in df.columns:
            st.markdown("#### 👥 Precios por Usuario")
            
            user_stats = df.groupby('user_id')['price'].agg(['mean', 'count']).reset_index()
            user_stats = user_stats.sort_values('mean', ascending=False).head(10)
            
            fig = px.bar(
                user_stats,
                x='user_id',
                y='mean',
                title="Precio Promedio por Usuario (Top 10)",
                labels={'mean': 'Precio Promedio ($)', 'user_id': 'Usuario'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Heatmap de correlaciones
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 1:
            st.markdown("#### 🔥 Mapa de Correlaciones")
            
            corr_matrix = df[numeric_cols].corr()
            
            fig = px.imshow(
                corr_matrix,
                title="Matriz de Correlaciones",
                color_continuous_scale="RdBu_r",
                aspect="auto"
            )
            st.plotly_chart(fig, use_container_width=True)

def display_analysis(df):
    """Muestra análisis avanzado"""
    st.markdown("### 🔍 Análisis Avanzado")
    
    if 'price' in df.columns:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📊 Análisis de Precios")
            
            price_data = df['price'].dropna()
            
            if len(price_data) > 0:
                # Estadísticas básicas
                st.write("**Estadísticas:**")
                st.write(f"- Media: ${price_data.mean():.2f}")
                st.write(f"- Mediana: ${price_data.median():.2f}")
                st.write(f"- Desviación estándar: ${price_data.std():.2f}")
                st.write(f"- Rango: ${price_data.min():.2f} - ${price_data.max():.2f}")
                
                # Detectar outliers
                Q1 = price_data.quantile(0.25)
                Q3 = price_data.quantile(0.75)
                IQR = Q3 - Q1
                outliers = price_data[(price_data < Q1 - 1.5*IQR) | (price_data > Q3 + 1.5*IQR)]
                
                st.write(f"- Outliers detectados: {len(outliers)} ({len(outliers)/len(price_data)*100:.1f}%)")
                
                if len(outliers) > 0:
                    st.write("**Outliers extremos:**")
                    extreme_outliers = outliers.nlargest(5)
                    for val in extreme_outliers:
                        st.write(f"  - ${val:.2f}")
        
        with col2:
            st.markdown("#### 👥 Análisis de Usuarios")
            
            if 'user_id' in df.columns:
                user_analysis = df.groupby('user_id').agg({
                    'price': ['count', 'mean', 'min', 'max', 'std']
                }).round(2)
                
                user_analysis.columns = ['Transacciones', 'Precio_Medio', 'Precio_Min', 'Precio_Max', 'Precio_Std']
                user_analysis = user_analysis.reset_index()
                
                st.write("**Top usuarios por transacciones:**")
                top_users = user_analysis.nlargest(5, 'Transacciones')
                st.dataframe(top_users, use_container_width=True)
                
                # Análisis de comportamiento
                st.write("**Insights:**")
                avg_transactions = user_analysis['Transacciones'].mean()
                high_volume_users = len(user_analysis[user_analysis['Transacciones'] > avg_transactions])
                
                st.write(f"- Promedio transacciones por usuario: {avg_transactions:.1f}")
                st.write(f"- Usuarios de alto volumen: {high_volume_users}")
                st.write(f"- Usuario más activo: {user_analysis.loc[user_analysis['Transacciones'].idxmax(), 'user_id']}")
    
    # Análisis temporal si hay timestamp
    if 'timestamp' in df.columns:
        st.markdown("#### 📅 Análisis Temporal")
        
        df_temp = df.copy()
        if df_temp['timestamp'].dtype == 'object':
            df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'], errors='coerce')
        
        df_temp = df_temp.dropna(subset=['timestamp'])
        
        if len(df_temp) > 0:
            # Extraer componentes de fecha
            df_temp['hour'] = df_temp['timestamp'].dt.hour
            df_temp['day'] = df_temp['timestamp'].dt.day
            df_temp['month'] = df_temp['timestamp'].dt.month
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Actividad por hora
                hourly_activity = df_temp.groupby('hour').size()
                fig = px.bar(
                    x=hourly_activity.index,
                    y=hourly_activity.values,
                    title="Actividad por Hora del Día",
                    labels={'x': 'Hora', 'y': 'Número de Transacciones'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Actividad por día del mes
                daily_activity = df_temp.groupby('day').size()
                fig = px.line(
                    x=daily_activity.index,
                    y=daily_activity.values,
                    title="Actividad por Día del Mes",
                    labels={'x': 'Día', 'y': 'Número de Transacciones'}
                )
                st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()