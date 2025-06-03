#!/bin/bash
# Script para activar el environment de Airflow con UV
export AIRFLOW_HOME="/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config"
export PYTHONPATH="/Users/andresariasmedina/Documents/Pruebas/pragma_test/src:$PYTHONPATH"
export PROJECT_ROOT="/Users/andresariasmedina/Documents/Pruebas/pragma_test"
source "/Users/andresariasmedina/Documents/Pruebas/pragma_test/.venv/bin/activate"

echo "✅ Environment de Airflow activado (UV)"
echo "🐍 Python: $(which python)"
echo "🌪️ Airflow: $(which airflow)"
echo "⚡ UV: $(which uv)"
echo "📱 tmux: $(which tmux)"
echo "📁 AIRFLOW_HOME: $AIRFLOW_HOME"
echo ""
echo "💡 Comandos útiles:"
echo "  airflow webserver --port 8080"
echo "  airflow scheduler"
echo "  airflow dags list"
echo "  uv pip install <package>  # Para instalar paquetes adicionales"
