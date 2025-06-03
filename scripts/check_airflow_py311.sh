#!/bin/bash
export AIRFLOW_HOME="/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config"
cd "/Users/andresariasmedina/Documents/Pruebas/pragma_test"

echo "🔍 DIAGNÓSTICO AIRFLOW CON PYTHON 3.11 + UV"
echo "==========================================="
echo "📁 Proyecto: /Users/andresariasmedina/Documents/Pruebas/pragma_test"
echo "🌪️ Airflow Home: /Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config"
echo ""

# Verificar herramientas
echo "🛠️ Herramientas:"
command -v uv &> /dev/null && echo "  ✅ UV: $(uv --version)" || echo "  ❌ UV no disponible"
command -v tmux &> /dev/null && echo "  ✅ tmux: $(tmux -V)" || echo "  ❌ tmux no disponible"

# Verificar Python y ambiente
echo ""
echo "🐍 Environment:"
if [ -f "/Users/andresariasmedina/Documents/Pruebas/pragma_test/.venv/bin/activate" ]; then
    source "/Users/andresariasmedina/Documents/Pruebas/pragma_test/.venv/bin/activate"
    echo "  ✅ Virtual env activo (UV)"
    echo "  🐍 Python: $(python --version)"
    echo "  📍 Ubicación: $(which python)"
    
    if command -v airflow &> /dev/null; then
        echo "  ✅ Airflow disponible"
        echo "  📋 Versión: $(airflow version)"
        echo "  📍 Ubicación: $(which airflow)"
    else
        echo "  ❌ Airflow NO disponible"
    fi
else
    echo "  ❌ Virtual env no encontrado"
fi

echo ""
echo "📁 Archivos críticos:"
[ -f "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/airflow.db" ] && echo "  ✅ airflow.db" || echo "  ❌ airflow.db"
[ -f "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/airflow.cfg" ] && echo "  ✅ airflow.cfg" || echo "  ❌ airflow.cfg"
[ -d "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/dags" ] && echo "  ✅ dags/" || echo "  ❌ dags/"

if [ -d "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/dags" ]; then
    dag_count=$(ls -1 "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/dags"/*.py 2>/dev/null | wc -l)
    echo "  📋 DAGs encontrados: $dag_count"
    
    if [ $dag_count -gt 0 ]; then
        echo "  📝 Archivos DAG:"
        ls -la "/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config/dags"/*.py 2>/dev/null | awk '{print "    " $9}' || true
    fi
fi

echo ""
echo "📋 Procesos:"
webserver_pid=$(pgrep -f "airflow webserver" 2>/dev/null || true)
scheduler_pid=$(pgrep -f "airflow scheduler" 2>/dev/null || true)

if [ ! -z "$webserver_pid" ]; then
    echo "  ✅ Webserver (PID: $webserver_pid)"
else
    echo "  ❌ Webserver NO corriendo"
fi

if [ ! -z "$scheduler_pid" ]; then
    echo "  ✅ Scheduler (PID: $scheduler_pid)"
else
    echo "  ❌ Scheduler NO corriendo"
fi

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

echo ""
echo "🌐 Conectividad:"
if [ ! -z "$webserver_pid" ]; then
    if curl -s http://localhost:8080 > /dev/null 2>&1; then
        echo "  ✅ Web UI accesible en http://localhost:8080"
    else
        echo "  ⚠️ Web UI no responde (puede estar iniciando)"
    fi
else
    echo "  ❌ Web UI no disponible"
fi

echo ""
echo "🔑 Credenciales:"
echo "  👤 Usuario: admin"
echo "  🔐 Password: admin123"

# Test de DAGs si Airflow está corriendo
if [ ! -z "$webserver_pid" ] && [ ! -z "$scheduler_pid" ]; then
    echo ""
    echo "📋 Estado de DAGs:"
    source "/Users/andresariasmedina/Documents/Pruebas/pragma_test/.venv/bin/activate"
    export AIRFLOW_HOME="/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config"
    timeout 10 airflow dags list 2>/dev/null | head -10 || echo "  ⚠️ Timeout o error conectando a la base de datos"
fi

echo ""
echo "💡 Comandos útiles:"
echo "  🚀 Iniciar: ./scripts/start_airflow_py311.sh"
echo "  🛑 Parar: ./scripts/stop_airflow.sh"
echo "  🔄 Reiniciar: ./scripts/restart_airflow.sh"
echo "  📱 Ver webserver: tmux attach -t airflow-webserver"
echo "  📅 Ver scheduler: tmux attach -t airflow-scheduler"
