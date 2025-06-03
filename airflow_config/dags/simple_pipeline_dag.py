# airflow_config/dags/simple_pipeline_dag.py
"""
🚀 DAG SIMPLIFICADO - EJECUTA SCRIPTS DIRECTAMENTE
=================================================
En lugar de reimplementar la lógica, simplemente ejecuta los scripts que ya funcionan.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# ==========================================
# CONFIGURACIÓN DEL DAG
# ==========================================

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_pipeline_execution',
    default_args=default_args,
    description='Pipeline simplificado - Ejecuta scripts directamente',
    schedule_interval=None,  # Solo manual
    catchup=False,
    max_active_runs=1,
    tags=['simple', 'scripts', 'production'],
)

# ==========================================
# CONFIGURACIÓN DE PATHS
# ==========================================

# Detectar ruta del proyecto automáticamente
PROJECT_ROOT = Path(__file__).parent.parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

# ==========================================
# TAREAS COMO BASH OPERATORS - ¡SÚPER SIMPLE!
# ==========================================

start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# 1. Descargar datos (si es necesario)
download_task = BashOperator(
    task_id='download_data',
    bash_command=f"""
    cd {PROJECT_ROOT}
    source .venv/bin/activate
    export PYTHONPATH="{PROJECT_ROOT}/src:$PYTHONPATH"
    python3 src/data_flow/download_data.py
    """,
    dag=dag,
)

# 2. Convertir CSV a Bronze (Parquet)
bronze_task = BashOperator(
    task_id='convert_to_bronze',
    bash_command=f"""
    cd {PROJECT_ROOT}
    source .venv/bin/activate
    export PYTHONPATH="{PROJECT_ROOT}/src:$PYTHONPATH"
    python3 src/data_flow/bronze_converter.py
    """,
    dag=dag,
)

# 3. ¡EJECUTAR EL PIPELINE PRINCIPAL DIRECTAMENTE!
pipeline_task = BashOperator(
    task_id='run_main_pipeline',
    bash_command=f"""
    cd {PROJECT_ROOT}
    source .venv/bin/activate
    export PYTHONPATH="{PROJECT_ROOT}/src:$PYTHONPATH"
    python3 src/pipeline/data_ingestion.py
    """,
    dag=dag,
)

# 4. Generar reporte simple
report_task = BashOperator(
    task_id='generate_report',
    bash_command=f"""
    cd {PROJECT_ROOT}
    echo "📊 REPORTE DE PIPELINE - $(date)" > logs/pipeline_report_$(date +%Y%m%d).txt
    echo "=================================" >> logs/pipeline_report_$(date +%Y%m%d).txt
    echo "✅ Pipeline ejecutado exitosamente" >> logs/pipeline_report_$(date +%Y%m%d).txt
    echo "📁 Archivos Bronze:" >> logs/pipeline_report_$(date +%Y%m%d).txt
    ls -la data/processed/bronze/ >> logs/pipeline_report_$(date +%Y%m%d).txt
    echo "📊 Base de datos:" >> logs/pipeline_report_$(date +%Y%m%d).txt
    ls -la data/pipeline.db >> logs/pipeline_report_$(date +%Y%m%d).txt
    echo "📄 Reporte guardado en logs/pipeline_report_$(date +%Y%m%d).txt"
    """,
    dag=dag,
)

end_task = DummyOperator(
    task_id='pipeline_completed',
    dag=dag,
)

# ==========================================
# DEPENDENCIAS - FLUJO LINEAL SIMPLE
# ==========================================

start_task >> download_task >> bronze_task >> pipeline_task >> report_task >> end_task

# ==========================================
# OPCIÓN ALTERNATIVA: DAG AUN MÁS SIMPLE
# ==========================================

# Si quieres hacer TODO en una sola tarea:
simple_dag = DAG(
    'ultra_simple_pipeline',
    default_args=default_args,
    description='Pipeline ultra simplificado - Todo en una tarea',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['ultra-simple', 'one-task'],
)

# Una sola tarea que ejecuta todo el pipeline
all_in_one_task = BashOperator(
    task_id='run_complete_pipeline',
    bash_command=f"""
    echo "🚀 Iniciando pipeline completo..."
    cd {PROJECT_ROOT}
    source .venv/bin/activate
    export PYTHONPATH="{PROJECT_ROOT}/src:$PYTHONPATH"
    
    echo "📥 Descargando datos..."
    python3 src/data_flow/download_data.py || echo "⚠️ Descarga falló o datos ya existen"
    
    echo "🥉 Convirtiendo a Bronze..."
    python3 src/data_flow/bronze_converter.py || echo "⚠️ Conversión falló o archivos ya existen"
    
    echo "🚀 Ejecutando pipeline principal..."
    python3 src/pipeline/data_ingestion.py
    
    echo "📊 Generando reporte..."
    mkdir -p logs
    echo "Pipeline completado exitosamente en $(date)" > logs/simple_report_$(date +%Y%m%d).txt
    
    echo "✅ ¡Pipeline completado!"
    """,
    dag=simple_dag,
)

# ==========================================
# DAG MODULAR CON VALIDACIÓN
# ==========================================

modular_dag = DAG(
    'modular_pipeline',
    default_args=default_args,
    description='Pipeline modular con validaciones',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['modular', 'validated'],
)

# Verificar prerequisites
check_task = BashOperator(
    task_id='check_prerequisites',
    bash_command=f"""
    cd {PROJECT_ROOT}
    echo "🔍 Verificando prerequisites..."
    
    # Verificar virtual environment
    if [ ! -f ".venv/bin/activate" ]; then
        echo "❌ Virtual environment no encontrado"
        exit 1
    fi
    
    # Verificar directorios
    mkdir -p data/raw data/processed/bronze data/processed/silver data/processed/gold logs
    
    # Verificar archivos críticos
    source .venv/bin/activate
    export PYTHONPATH="{PROJECT_ROOT}/src:$PYTHONPATH"
    
    echo "📦 Verificando módulos Python..."
    python3 -c "
import sys
sys.path.insert(0, 'src')
try:
    from pipeline.data_ingestion import DataIngestionPipeline
    print('✅ DataIngestionPipeline importado')
except Exception as e:
    print(f'❌ Error importando: {{e}}')
    exit(1)
"
    
    echo "✅ Prerequisites verificados"
    """,
    dag=modular_dag,
)

# Ejecutar descarga con validación
download_validated_task = BashOperator(
    task_id='download_with_validation',
    bash_command=f"""
    cd {PROJECT_ROOT}
    source .venv/bin/activate
    export PYTHONPATH="{PROJECT_ROOT}/src:$PYTHONPATH"
    
    echo "📥 Ejecutando descarga..."
    python3 -c "
from src.data_flow.download_data import DataDownloader
downloader = DataDownloader()
success, _ = downloader.download_challenge_data()
if success:
    verification = downloader.verify_downloaded_data()
    if verification:
        print('✅ Descarga y verificación exitosas')
    else:
        print('❌ Error en verificación')
        exit(1)
else:
    print('⚠️ Descarga falló, verificando archivos existentes...')
    # Verificar si ya existen
    import os
    csv_count = len([f for f in os.listdir('data/raw') if f.endswith('.csv')] if os.path.exists('data/raw') else [])
    if csv_count >= 6:
        print(f'✅ Encontrados {{csv_count}} archivos CSV existentes')
    else:
        print(f'❌ Solo se encontraron {{csv_count}} archivos CSV')
        exit(1)
"
    """,
    dag=modular_dag,
)

# Ejecutar Bronze con validación
bronze_validated_task = BashOperator(
    task_id='bronze_with_validation',
    bash_command=f"""
    cd {PROJECT_ROOT}
    source .venv/bin/activate
    export PYTHONPATH="{PROJECT_ROOT}/src:$PYTHONPATH"
    
    echo "🥉 Ejecutando conversión Bronze..."
    python3 -c "
from src.data_flow.bronze_converter import BronzeConverter
converter = BronzeConverter()
result = converter.convert_all_csv_to_bronze()
if result.get('success', False):
    print(f'✅ Bronze exitoso: {{result.get(\"converted_files\", 0)}} archivos')
    
    # Verificar archivos generados
    import os
    parquet_count = len([f for f in os.listdir('data/processed/bronze') if f.endswith('.parquet')] if os.path.exists('data/processed/bronze') else [])
    print(f'📊 Archivos Parquet generados: {{parquet_count}}')
    
    if parquet_count >= 6:
        print('✅ Conversión Bronze verificada')
    else:
        print('❌ Archivos Parquet insuficientes')
        exit(1)
else:
    print('❌ Error en conversión Bronze')
    exit(1)
"
    """,
    dag=modular_dag,
)

# Pipeline principal - ¡EL QUE YA TIENES!
main_pipeline_task = BashOperator(
    task_id='execute_main_pipeline',
    bash_command=f"""
    cd {PROJECT_ROOT}
    source .venv/bin/activate
    export PYTHONPATH="{PROJECT_ROOT}/src:$PYTHONPATH"
    
    echo "🚀 Ejecutando pipeline principal..."
    echo "📁 Directorio actual: $(pwd)"
    echo "🐍 Python: $(which python3)"
    echo "📊 Archivos Bronze disponibles:"
    ls -la data/processed/bronze/
    
    # Ejecutar el pipeline que YA FUNCIONA
    python3 src/pipeline/data_ingestion.py
    
    echo "📊 Verificando resultados..."
    echo "Base de datos generada:"
    ls -la data/pipeline.db
    
    echo "✅ Pipeline principal completado"
    """,
    dag=modular_dag,
)

# Reporte final detallado
final_report_task = BashOperator(
    task_id='generate_final_report',
    bash_command=f"""
    cd {PROJECT_ROOT}
    
    REPORT_FILE="logs/detailed_report_$(date +%Y%m%d_%H%M%S).txt"
    
    echo "📊 REPORTE DETALLADO DEL PIPELINE" > $REPORT_FILE
    echo "=================================" >> $REPORT_FILE
    echo "📅 Fecha: $(date)" >> $REPORT_FILE
    echo "📁 Proyecto: {PROJECT_ROOT}" >> $REPORT_FILE
    echo "" >> $REPORT_FILE
    
    echo "📋 ARCHIVOS GENERADOS:" >> $REPORT_FILE
    echo "Bronze layer:" >> $REPORT_FILE
    ls -la data/processed/bronze/ >> $REPORT_FILE
    echo "" >> $REPORT_FILE
    
    echo "Base de datos:" >> $REPORT_FILE
    ls -la data/pipeline.db >> $REPORT_FILE
    echo "" >> $REPORT_FILE
    
    echo "Logs del pipeline:" >> $REPORT_FILE
    ls -la logs/ | tail -5 >> $REPORT_FILE
    echo "" >> $REPORT_FILE
    
    echo "🎯 ESTADO: PIPELINE COMPLETADO EXITOSAMENTE" >> $REPORT_FILE
    
    echo "📄 Reporte guardado en: $REPORT_FILE"
    cat $REPORT_FILE
    """,
    dag=modular_dag,
)

# Dependencias para DAG modular
start_modular = DummyOperator(task_id='start_modular', dag=modular_dag)
end_modular = DummyOperator(task_id='end_modular', dag=modular_dag)

start_modular >> check_task >> download_validated_task >> bronze_validated_task >> main_pipeline_task >> final_report_task >> end_modular