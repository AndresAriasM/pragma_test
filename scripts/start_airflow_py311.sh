#!/bin/bash
export AIRFLOW_HOME="/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config"
cd "/Users/andresariasmedina/Documents/Pruebas/pragma_test"
source "/Users/andresariasmedina/Documents/Pruebas/pragma_test/.venv/bin/activate"

echo "🌪️ Iniciando Airflow with Python 3.11 + UV + tmux..."
echo "📁 Proyecto: /Users/andresariasmedina/Documents/Pruebas/pragma_test"
echo "🐍 Python: $(python --version)"
echo "⚡ UV: $(uv --version)"
echo "🌪️ Airflow: $(airflow version)"
echo "📱 tmux: $(tmux -V)"
echo "🌐 Web UI: http://localhost:8080"
echo "👤 Login: admin / admin123"
echo ""

# Terminar sesiones existentes
echo "🛑 Terminando sesiones tmux anteriores..."
tmux kill-session -t airflow-webserver 2>/dev/null || true
tmux kill-session -t airflow-scheduler 2>/dev/null || true

# Esperar un momento para limpieza
sleep 2

echo "🚀 Iniciando servicios con tmux..."

# Crear sesión para webserver
tmux new-session -d -s airflow-webserver -c "/Users/andresariasmedina/Documents/Pruebas/pragma_test" \
    "source '/Users/andresariasmedina/Documents/Pruebas/pragma_test/.venv/bin/activate' && \
     export AIRFLOW_HOME='/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config' && \
     export PYTHONPATH='/Users/andresariasmedina/Documents/Pruebas/pragma_test/src:$PYTHONPATH' && \
     echo '🌐 Iniciando Airflow Webserver...' && \
     airflow webserver --port 8080"

# Crear sesión para scheduler
tmux new-session -d -s airflow-scheduler -c "/Users/andresariasmedina/Documents/Pruebas/pragma_test" \
    "source '/Users/andresariasmedina/Documents/Pruebas/pragma_test/.venv/bin/activate' && \
     export AIRFLOW_HOME='/Users/andresariasmedina/Documents/Pruebas/pragma_test/airflow_config' && \
     export PYTHONPATH='/Users/andresariasmedina/Documents/Pruebas/pragma_test/src:$PYTHONPATH' && \
     echo '📅 Iniciando Airflow Scheduler...' && \
     sleep 5 && \
     airflow scheduler"

# Esperar que los servicios inicien
echo "⏳ Esperando que los servicios inicien..."
sleep 10

echo ""
echo "✅ Airflow iniciado correctamente!"
echo "📱 Sesiones tmux activas:"
echo "  🌐 Webserver: tmux attach -t airflow-webserver"
echo "  📅 Scheduler: tmux attach -t airflow-scheduler"
echo ""
echo "🌐 Acceso Web: http://localhost:8080"
echo "👤 Usuario: admin"
echo "🔐 Password: admin123"
echo ""
echo "💡 Comandos útiles:"
echo "  🔍 Ver estado: ./scripts/check_airflow_py311.sh"
echo "  🛑 Parar servicios: ./scripts/stop_airflow.sh"
echo "  📱 Listar sesiones: tmux list-sessions"
echo "  🔄 Reiniciar: ./scripts/restart_airflow.sh"
