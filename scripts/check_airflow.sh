#!/bin/bash
# scripts/check_airflow.sh

PROJECT_ROOT=$(dirname $(dirname $(realpath $0)))
AIRFLOW_HOME="$PROJECT_ROOT/airflow_config"
VENV_PATH="$PROJECT_ROOT/.venv"

echo "🔍 ESTADO DE AIRFLOW"
echo "==================="
echo "📁 Proyecto: $PROJECT_ROOT"
echo "🌪️ Airflow Home: $AIRFLOW_HOME"
echo ""

# Verificar directorios
echo "📁 Directorios:"
[ -d "$AIRFLOW_HOME" ] && echo "  ✅ airflow_config/" || echo "  ❌ airflow_config/"
[ -d "$AIRFLOW_HOME/dags" ] && echo "  ✅ dags/" || echo "  ❌ dags/"
[ -d "$VENV_PATH" ] && echo "  ✅ .venv/" || echo "  ❌ .venv/"
echo ""

# Verificar procesos
echo "📋 Procesos:"
webserver_pid=$(pgrep -f "airflow webserver" 2>/dev/null || true)
if [ ! -z "$webserver_pid" ]; then
    echo "  ✅ Webserver corriendo (PID: $webserver_pid)"
else
    echo "  ❌ Webserver NO corriendo"
fi

scheduler_pid=$(pgrep -f "airflow scheduler" 2>/dev/null || true)
if [ ! -z "$scheduler_pid" ]; then
    echo "  ✅ Scheduler corriendo (PID: $scheduler_pid)"
else
    echo "  ❌ Scheduler NO corriendo"
fi

# Verificar sesiones tmux
if command -v tmux &> /dev/null; then
    echo ""
    echo "📱 Sesiones tmux:"
    if tmux has-session -t airflow-webserver 2>/dev/null; then
        echo "  ✅ airflow-webserver"
    else
        echo "  ❌ airflow-webserver"
    fi
    
    if tmux has-session -t airflow-scheduler 2>/dev/null; then
        echo "  ✅ airflow-scheduler"
    else
        echo "  ❌ airflow-scheduler"
    fi
fi

echo ""
echo "🌐 URLs:"
echo "  Web UI: http://localhost:8080"
echo "  Usuario: admin"
echo "  Password: admin123"
echo ""

# Verificar conexión a Airflow si está corriendo
if [ ! -z "$webserver_pid" ] && [ ! -z "$scheduler_pid" ]; then
    export AIRFLOW_HOME="$AIRFLOW_HOME"
    cd "$PROJECT_ROOT"
    
    if [ -f "$VENV_PATH/bin/activate" ]; then
        source "$VENV_PATH/bin/activate"
        echo "📄 DAGs disponibles:"
        timeout 5 airflow dags list 2>/dev/null | head -5 | while read line; do
            echo "  $line"
        done || echo "  ⚠️ Timeout conectando a Airflow"
    else
        echo "❌ Virtual environment no encontrado"
    fi
else
    echo "⚠️ Airflow no está completamente iniciado"
    echo "💡 Para iniciar: ./scripts/start_airflow.sh"
fi